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
def put(src, dest):
    dest_manager, dest_relpath = _getManager(dest)

    if isinstance(src, str):
        src_manager, src_relpath = _getManager(src)
        dest_manager.put(src_manager[src_relpath], dest_relpath)
    else:
        dest_manager.put(src, dest_relpath)

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
