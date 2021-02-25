""" Functions for the stow package to provided a stateless method of interacting with various manager artefacts.
"""

import os
import typing
import urllib.parse
import contextlib
from functools import wraps

from .artefacts import Artefact
from .manager import Manager, LocalManager
from . import utils

def _getManager(artefact) -> typing.Tuple[Manager, str]:

    if isinstance(artefact, Artefact):
        manager = artefact.manager
        relpath = artefact.path

    elif isinstance(artefact, str):
        return utils.parseURL(artefact)

    else:
        raise TypeError("Artefact reference must be either `stow.Artefact` or string not type {}".format(type(artefact)))

    return manager, relpath

@wraps(utils.find)
def find(*args, **kwargs) -> typing.Type[Manager]:
    return utils.find(*args, **kwargs)

@wraps(utils.connect)
def connect(*args, **kwargs) -> Manager:
    return utils.connect(*args, **kwargs)

@wraps(utils.parseURL)
def parseURL(*args, **kwargs) -> utils.ParsedURL:
    return utils.parseURL(*args, **kwargs)

def artefact(stowPath: str) -> Artefact:
    """ Fetch an artefact object for the given path

    Params:
        stowPath: Manager relative path to artefact

    Returns:
        Arefact: The artefact object

    Raises:
        ArtefactNotFound: In the event that no artefact exists at the location given
    """
    manager, relpath = _getManager(stowPath)
    return manager[relpath]

@wraps(Manager.abspath)
def abspath(*args, **kwargs) -> str:
    return Manager.abspath(*args, **kwargs)

@wraps(Manager.basename)
def basename(*args, **kwargs) -> str:
    return Manager.basename(*args, **kwargs)

@wraps(Manager.commonpath)
def commonpath(*args, **kwargs) -> str:
    return Manager.commonpath(*args, **kwargs)

@wraps(Manager.commonprefix)
def commonprefix(*args, **kwargs) -> str:
    return Manager.commonprefix(*args, **kwargs)

@wraps(Manager.dirname)
def dirname(*args, **kwargs) -> str:
    return Manager.dirname(*args, **kwargs)

@wraps(Manager.expanduser)
def expanduser(*args, **kwargs) -> str:
    return Manager.expanduser(*args, **kwargs)

@wraps(Manager.expandvars)
def expandvars(*args, **kwargs) -> str:
    return Manager.expandvars(*args, **kwargs)

@wraps(Manager.isabs)
def isabs(*args, **kwargs) -> str:
    return Manager.isabs(*args, **kwargs)

@wraps(Manager.join)
def join(*args, **kwargs) -> str:
    return Manager.join(*args, **kwargs)

@wraps(Manager.normcase)
def normcase(*args, **kwargs) -> str:
    return Manager.normcase(*args, **kwargs)

@wraps(Manager.normpath)
def normpath(*args, **kwargs) -> str:
    return Manager.normpath(*args, **kwargs)

@wraps(Manager.realpath)
def realpath(*args, **kwargs) -> str:
    return Manager.realpath(*args, **kwargs)

@wraps(Manager.relpath)
def relpath(*args, **kwargs) -> str:
    return Manager.relpath(*args, **kwargs)

@wraps(Manager.samefile)
def samefile(*args, **kwargs) -> str:
    return Manager.samefile(*args, **kwargs)

@wraps(Manager.sameopenfile)
def sameopenfile(*args, **kwargs) -> str:
    return Manager.sameopenfile(*args, **kwargs)

@wraps(Manager.samestat)
def samestat(*args, **kwargs) -> str:
    return Manager.samestat(*args, **kwargs)

@wraps(Manager.split)
def split(*args, **kwargs) -> str:
    return Manager.split(*args, **kwargs)

@wraps(Manager.splitdrive)
def splitdrive(*args, **kwargs) -> str:
    return Manager.splitdrive(*args, **kwargs)

@wraps(Manager.splitext)
def splitext(*args, **kwargs) -> str:
    return Manager.splitext(*args, **kwargs)

@wraps(Manager.md5)
def md5(*args, **kwargs) -> str:
    return Manager.md5(*args, **kwargs)


# Instance wrapped

@wraps(Manager.isfile)
def isfile(artefact: typing.Union[Artefact, str]) -> bool:
    manager, relpath = _getManager(artefact)
    return manager.isfile(relpath)

@wraps(Manager.isdir)
def isdir(artefact: typing.Union[Artefact, str]) -> bool:
    manager, relpath = _getManager(artefact)
    return manager.isdir(relpath)

@wraps(Manager.islink)
def islink(artefact: typing.Union[Artefact, str]) -> bool:
    manager, relpath = _getManager(artefact)
    return manager.islink(relpath)

@wraps(Manager.ismount)
def ismount(artefact: typing.Union[Artefact, str]) -> bool:
    manager, relpath = _getManager(artefact)
    return manager.ismount(relpath)

@wraps(Manager.getctime)
def getctime(artefact: typing.Union[Artefact, str]) -> bool:
    manager, relpath = _getManager(artefact)
    return manager.getctime(relpath)

@wraps(Manager.getmtime)
def getmtime(artefact: typing.Union[Artefact, str]) -> bool:
    manager, relpath = _getManager(artefact)
    return manager.getmtime(relpath)

@wraps(Manager.getatime)
def getatime(artefact: typing.Union[Artefact, str]) -> bool:
    manager, relpath = _getManager(artefact)
    return manager.getatime(relpath)

@wraps(Manager.exists)
def exists(artefact: typing.Union[Artefact, str]) -> bool:
    manager, relpath = _getManager(artefact)
    return manager.exists(relpath)

@wraps(Manager.lexists)
def lexists(artefact: typing.Union[Artefact, str]) -> bool:
    manager, relpath = _getManager(artefact)
    return manager.lexists(relpath)

@wraps(Manager.touch)
def touch(artefact, *args, **kwargs):
    manager, relpath = _getManager(artefact)
    return manager.touch(relpath)

@wraps(Manager.mkdir)
def mkdir(artefact, *args, **kwargs):
    manager, relpath = _getManager(artefact)
    return manager.mkdir(relpath,*args, **kwargs)

@wraps(Manager.localise)
@contextlib.contextmanager
def localise(artefact, *args, **kwargs):
    manager, relpath = _getManager(artefact)
    with manager.localise(relpath, *args, **kwargs) as abspath:
        yield abspath

@wraps(Manager.open)
@contextlib.contextmanager
def open(artefact, mode = "r", *args, **kwargs):
    manager, relpath = _getManager(artefact)
    with manager.open(relpath, mode, *args, **kwargs) as handle:
        yield handle

@wraps(Manager.ls)
def ls(artefact = os.path.curdir, **kwargs):
    manager, relpath = _getManager(artefact)
    return manager.ls(manager.abspath(relpath), **kwargs)

@wraps(Manager.get)
def get(src_remote, dest_local, *args, **kwargs):
    manager, relpath = _getManager(src_remote)
    manager.get(relpath, dest_local, *args, **kwargs)

@wraps(Manager.put)
def put(src, dest, *args, **kwargs):

    # Get the destination manager for the artifact
    dest_manager, dest_relpath = _getManager(dest)

    # Put the source path into the manager
    return dest_manager.put(src, dest_relpath, *args, **kwargs)

@wraps(Manager.cp)
def cp(src, dest, *args, **kwargs):
    srcM, srcP = _getManager(src)
    destM, destP = _getManager(dest)
    destM.cp(srcP, destP, *args, **kwargs)

@wraps(Manager.mv)
def mv(src, dest, *args, **kwargs):
    srcM, srcP = _getManager(src)
    destM, destP = _getManager(dest)
    assert srcM is destM
    srcM.mv(srcP, destP, *args, **kwargs)

@wraps(Manager.sync)
def sync(src, dest, *args, **kwargs):
    srcM, srcP = _getManager(src)
    destM, destP = _getManager(dest)

    # Call sync on the destination manager
    destM.sync(srcM[srcP], destM[destP], *args, **kwargs)

@wraps(Manager.rm)
def rm(artefact, *args, **kwargs):
    manger, relpath = _getManager(artefact)
    manger.rm(relpath, *args, **kwargs)

supports_unicode_filenames = os.path.supports_unicode_filenames