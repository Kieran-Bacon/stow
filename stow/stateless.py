""" Functions for the stow package to provided a stateless method of interacting with various manager artefacts.
"""

import os
import re
import contextlib
import typing
from functools import wraps
from urllib.parse import urlparse

from .artefacts import Artefact
from .manager import Manager
from .utils import connect
from .managers import FS

from . import exceptions

def _getManager(artefact) -> typing.Tuple[Manager, str]:
    if isinstance(artefact, Artefact):
        manager = artefact.manager
        relpath = artefact.path

    else:
        parsedURL = urlparse(artefact)

        if not parsedURL.scheme:
            manager = connect(manager="FS", path="/")
            relpath = os.path.abspath(os.path.expanduser(parsedURL.path))

        elif parsedURL.scheme == "s3":
            manager = connect(manager="S3", bucket=parsedURL.netloc)
            relpath = parsedURL.path

        else:
            # Unsupported / invalid path
            raise exceptions.InvalidPath("Couldn't find manager to handle path: {}".format(artefact))

        relpath = relpath if relpath else "/"

    return manager, relpath

def artefact(artefact: str) -> Artefact:
    """ Fetch an artefact object for the given path

    Params:
        artefact (str): Manager relative path
    """
    manager, relpath = _getManager(artefact)
    return manager[relpath]

def exists(path: str) -> bool:
    """ Check if the path points at a valid artefact

    Params:
        path (str): the path to check if it exists

    Returns:
        bool: True if an artefact is found at the location
    """
    manger, relpath = _getManager(path)
    return relpath in manger


@wraps(Manager.isabs)
def isabs(path: str):
    manager, _ = _getManager(path)

    if isinstance(manager, FS):
        return manager.isabs(path)

    else:
        # For the path to identify a different/valid manager then it must be an absolute path
        return True

@wraps(Manager.abspath)
def abspath(path: str) -> str:
    manager, relpath = _getManager(path)
    return manager.abspath(relpath)

@wraps(Manager.abspath)
def relpath(path: str) -> str:
    return Manager.relpath(path)

@wraps(Manager.commonprefix)
def commonprefix(artefacts: typing.Iterable[typing.Union[Artefact, str]]):

    paths = []
    for art in artefacts:
        if isinstance(art, Artefact):
            paths.append(art.path)

        else:
            paths.append(art)

    return os.path.commonprefix(paths)


@wraps(Manager.commonpath)
def commonpath(artefacts: typing.Iterable[typing.Union[Artefact, str]]):

    paths = []
    for art in artefacts:
        if isinstance(art, Artefact):
            paths.append(art.path)

        else:
            paths.append(art)

    return os.path.commonpath(paths)

@wraps(Manager.dirname)
def dirname(artefact: typing.Union[Artefact, str]):
    manager, relpath = _getManager(artefact)
    return manager.dirname(relpath)

@wraps(Manager.basename)
def basename(artefact: typing.Union[Artefact, str]):
    manager, relpath = _getManager(artefact)
    return manager.basename(relpath)

@wraps(Manager.join)
def join(*artefacts: typing.Iterable[typing.Union[Artefact, str]]):
    base = artefacts[0]
    parsedURL = urlparse(base)
    manager, _ = _getManager(base)

    path = manager.join(*artefacts)
    if parsedURL.scheme:
        path = parsedURL.scheme + ":/" + path

    return path

@wraps(Manager.touch)
def touch(artefact: str):
    manager, relpath = _getManager(artefact)
    return manager.touch(relpath)

@wraps(Manager.mkdir)
def mkdir(artefact: str):
    manager, relpath = _getManager(artefact)
    return manager.mkdir(relpath)

@wraps(Manager.localise)
@contextlib.contextmanager
def localise(artefact):
    manager, relpath = _getManager(artefact)
    with manager.localise(relpath) as abspath:
        yield abspath

@wraps(Manager.open)
@contextlib.contextmanager
def open(artefact, mode, **kwargs):
    manager, relpath = _getManager(artefact)
    with manager.open(relpath, mode, **kwargs) as handle:
        yield handle

@wraps(Manager.ls)
def ls(artefact = ".", **kwargs):
    manager, relpath = _getManager(artefact)
    return manager.ls(relpath, **kwargs)

@wraps(Manager.get)
def get(src_remote, dest_local):
    manager, relpath = _getManager(src_remote)
    manager.get(relpath, dest_local)

@wraps(Manager.put)
def put(src, dest, *, overwrite=False, merge=False):
    src_manager, src_relpath = _getManager(src)
    dest_manager, dest_relpath = _getManager(dest)
    return dest_manager.put(src_manager[src_relpath], dest_relpath, overwrite=overwrite, merge=merge)

@wraps(Manager.cp)
def cp(src, dest):
    srcM, srcP = _getManager(src)
    destM, destP = _getManager(dest)
    assert srcM is destM
    srcM.cp(srcP, destP)

@wraps(Manager.mv)
def mv(src, dest):
    srcM, srcP = _getManager(src)
    destM, destP = _getManager(dest)
    assert srcM is destM
    srcM.mv(srcP, destP)

@wraps(Manager.rm)
def rm(artefact, *args, **kwargs):
    manger, relpath = _getManager(artefact)
    manger.rm(relpath, *args, **kwargs)
