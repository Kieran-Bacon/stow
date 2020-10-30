""" Functions for the stow package to provided a stateless method of interacting with various manager artefacts.
"""

import os
import re
import contextlib
import typing
from functools import wraps
from urllib.parse import urlparse

from .artefacts import Artefact
from .manager import Manager, LocalManager, RemoteManager
from .utils import connect
from .managers import FS, managers

from . import exceptions

def _getManager(artefact) -> typing.Tuple[Manager, str]:
    if isinstance(artefact, Artefact):
        manager = artefact.manager
        relpath = artefact.path

    elif isinstance(artefact, str):
        parsedURL = urlparse(artefact)

        protocol = parsedURL.scheme
        netloc = parsedURL.netloc

        if not protocol:


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

    else:
        raise TypeError("Artefact reference must be either `stow.Artefact` or string not type {}".format(type(artefact)))

    return manager, relpath

def manager(artefact: str) -> Manager:
    """ Fetch the manager object for the given URL """
    pass

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

def isabs(path: str):
    """ Check if the provided path is an absolute path for the manager

    Args:
        path (str): Path to check

    Returns:
        bool: True if the path given is absolute
    """

    manager, _ = _getManager(path)

    if isinstance(manager, FS):
        return manager.isabs(path)

    else:
        # For the path to identify a different/valid manager then it must be an absolute path
        return True

def abspath(artefact: typing.Union[Artefact, str]) -> str:
    manager, relpath = _getManager(artefact)

    if isinstance(managers, LocalManager):
        return os.path.abspath(relpath)

    else:
        NotImplementedError("Cannot find given absolute of remote artefact")

def basename(self, artefact: typing.Union[Artefact, str]) -> str:
    """ Return the basename of the provided artefact/relative path. The base name of a filepath is the name of the
    file/folder at the end of the hierarchy.

    Args:
        artefact (Artefact/str): the artefact to have it's name extracted

    Returns:
        str: the base name of the artefact
    """
    return os.path.basename(self.relpath(relpath))

def basename(artefact: typing.Union[Artefact, str]):
    manager, relpath = _getManager(artefact)
    return manager.basename(relpath)

def commonprefix(self, paths: typing.Iterable[str]) -> typing.Iterable[str]:
    return os.path.commonprefix(paths)

def commonpath(self, paths: typing.Iterable[str]) -> typing.Iterable[str]:
    return os.path.commonpath(paths)

@abstractmethod
def abspath(self, relpath: str) -> str:

    pass



    def getctime(artefact: typing.Union[Artefact, str]) -> float:
        """Return the time of last access of path. The return value is a floating point number giving the number of
        seconds since the epoch

        Args:
            artefact: An artefact object or string to get access time from

        Return:
            float: the timestamp of the file created

        Raises:
            ArtefactNotFound: in the event that the path doesn't lead anywhere
        """
        pass



    def getmtime(artefact: typing.Union[Artefact, str]) -> float:

    def getatime(artefact: typing.Union[Artefact, str]) -> float:

    os.path.getsize(path)

os.path.isfile(path)
Return True if path is an existing regular file. This follows symbolic links, so both islink() and isfile() can be true for the same path.

Changed in version 3.6: Accepts a path-like object.

os.path.isdir(path)
Return True if path is an existing directory. This follows symbolic links, so both islink() and isdir() can be true for the same path.

Changed in version 3.6: Accepts a path-like object.

os.path.islink(path)
Return True if path refers to an existing directory entry that is a symbolic link. Always False if symbolic links are not supported by the Python runtime.

Changed in version 3.6: Accepts a path-like object.

os.path.ismount(path)
Return True if pathname path is a mount point: a point in a file system where a different file system has been mounted. On POSIX, the function checks whether path’s parent, path/.., is on a different device than path, or whether path/.. and path point to the same i-node on the same device — this should detect mount points for all Unix and POSIX variants. It is not able to reliably detect bind mounts on the same filesystem. On Windows, a drive letter root and a share UNC are always mount points, and for any other path GetVolumePathName is called to see if it is different from the input path.

New in version 3.4: Support for detecting non-root mount points on Windows.

Changed in version 3.6: Accepts a path-like object.




@wraps(Manager.relpath)
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

@wraps(Manager.join)
def join(*artefacts: typing.Iterable[typing.Union[Artefact, str]]):
    base = artefacts[0]
    parsedURL = urlparse(base)
    manager, _ = _getManager(base)
    return manager.join(*artefacts)

@wraps(Manager.touch)
def touch(artefact: str):
    manager, relpath = _getManager(artefact)
    return manager.touch(relpath)

@wraps(Manager.mkdir)
def mkdir(artefact: str, ignoreExists: bool = True, overwrite: bool = False):
    manager, relpath = _getManager(artefact)
    return manager.mkdir(relpath, ignoreExists, overwrite)

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

    # Get the destination manager for the artifact
    dest_manager, dest_relpath = _getManager(dest)

    if isinstance(src, bytes):
        return dest_manager.put(src, dest_relpath, overwrite=overwrite, merge=merge)

    else:
        src_manager, src_relpath = _getManager(src)
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

@wraps(Manager.sync)
def sync(src, dest):
    srcM, srcP = _getManager(src)
    destM, destP = _getManager(dest)

    # Call sync on the destination manager
    destM.sync(srcM[srcP], destM[destP])

@wraps(Manager.rm)
def rm(artefact, *args, **kwargs):
    manger, relpath = _getManager(artefact)
    manger.rm(relpath, *args, **kwargs)
