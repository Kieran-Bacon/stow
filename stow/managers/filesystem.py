import os
import abc
import time
import typing
import sys
import errno
import stat
import datetime
import urllib
from typing import Optional, Union

import binascii
import hashlib

posix = None
if os.name == 'nt':
    import ctypes

else:
    import posix

from .. import _utils as utils
from ..worker_config import WorkerPoolConfig
from ..artefacts import Artefact, File, Directory, PartialArtefact
from ..types import HashingAlgorithm
from ..manager.base_managers import LocalManager
from ..callbacks import AbstractCallback, DefaultCallback

if hasattr(os, 'listxattr'):
    def _copyExtendedAttribues(src, dst, *, follow_symlinks=True):
        """Copy extended filesystem attributes from `src` to `dst`.

        Overwrite existing attributes.

        If `follow_symlinks` is false, symlinks won't be followed.

        """

        try:
            names = os.listxattr(src, follow_symlinks=follow_symlinks)
        except OSError as e:
            if e.errno not in (errno.ENOTSUP, errno.ENODATA, errno.EINVAL):
                raise
            return
        for name in names:
            try:
                value = os.getxattr(src, name, follow_symlinks=follow_symlinks)
                os.setxattr(dst, name, value, follow_symlinks=follow_symlinks)
            except OSError as e:
                if e.errno not in (errno.EPERM, errno.ENOTSUP, errno.ENODATA,
                                   errno.EINVAL):
                    raise
else:
    def _copyExtendedAttribues(*args, **kwargs):
        pass


class FS(LocalManager):
    """ Wrap a local filesystem (a networked drive or local directory)

    Args:
        path (str): The local relative path to where the manager is to be initialised
    """

    def __new__(cls, path: Optional[str] = None, drive: Optional[str] = None):
        # Note - Though the arguments must match the instances called from super, they do not have to have the same
        # defaults. This allows use to handle the default behaviour differently for the managers

        if path is None:
            return super().__new__(RootFS) # type: ignore

        else:
            return super().__new__(SubdirectoryFS) # type: ignore

    if os.name == 'nt':

        def __init__(self, path: str = os.path.sep, drive: str = 'c'):
            super().__init__()

            self._drive = drive
            expected_drive, self._path = os.path.splitdrive(path)

            if expected_drive and drive.lower() != expected_drive[:-1].lower():
                raise ValueError(f'Drive letter passed does not match drive letter of path: {drive} != ({expected_drive}):{self._path}')

            self._root = self._drive + ':' + self._path

        COPY_BUFFER_SIZE = 1024 * 1024

    else:
        def __init__(self, path: str = os.path.sep, drive: str = ''):
            super().__init__()
            self._drive = drive
            _, self._root = os.path.splitdrive(path)
            self._path = self._root

        COPY_BUFFER_SIZE = 64 * 1024

    def _abspath(self, path: str) -> str:
        ...

    def _relative(self, path: str) -> str:
        ...

    def _mklink(self, source: str, destination: str, soft: bool):
        if soft:
            os.symlink(self._abspath(source), self._abspath(destination))
        else:
            os.link(self._abspath(source), self._abspath(destination))

        return PartialArtefact(self, destination)

    def __repr__(self):
        return '<Manager(FS)>'

    def _exists(self, managerPath: str):
        return os.path.exists(self._abspath(managerPath))

    def _metadata(self, _: str):
        return {}

    def _isLink(self, file: str):
        return os.path.islink(self._abspath(file))

    def _isMount(self, directory: str):
        return os.path.ismount(self._abspath(directory))

    def _identifyPath(self, entry: Union[str, os.DirEntry]):

        try:

            if isinstance(entry, str):
                abspath = self._abspath(entry)
                artefactStat = os.stat(abspath)
            else:
                abspath = entry.path
                artefactStat = entry.stat()

            # Export artefact created time information
            createdTime = datetime.datetime.fromtimestamp(artefactStat.st_ctime, tz=datetime.timezone.utc)
            modifiedTime = datetime.datetime.fromtimestamp(artefactStat.st_mtime, tz=datetime.timezone.utc)
            accessedTime = datetime.datetime.fromtimestamp(artefactStat.st_atime, tz=datetime.timezone.utc)

            if stat.S_ISDIR(artefactStat.st_mode):
                return Directory(
                    self,
                    self._relative(abspath),
                    createdTime=createdTime,
                    modifiedTime=modifiedTime,
                    accessedTime=accessedTime,
                )

            else:
                return File(
                    self,
                    self._relative(abspath),
                    artefactStat.st_size,
                    modifiedTime,
                    createdTime=createdTime,
                    accessedTime=accessedTime,
                )

        except:
            return None

    def _digest(self, file: File, algorithm: HashingAlgorithm):

        with file.open('rb') as handle:
            if algorithm is HashingAlgorithm.MD5:
                return hashlib.md5(handle.read()).hexdigest()
            elif algorithm is HashingAlgorithm.CRC32:
                return hex(binascii.crc32(handle.read()) & 0xFFFFFFFF)
            elif algorithm is HashingAlgorithm.SHA1:
                return hashlib.sha1(handle.read()).hexdigest()
            elif algorithm is HashingAlgorithm.SHA256:
                return hashlib.sha256(handle.read()).hexdigest()
            else:
                raise NotImplementedError(f'{algorithm} hashing is not implemented')

    def _defaultcopyfile(self, source: str, destination: str, sourceStat: os.stat_result, callback):
        """ Generic why to copy file bytes to new location """

        with open(source, 'rb') as source_handle:
            with open(destination, 'wb') as destination_handle:

                transfer = callback.get_bytes_transfer(destination, sourceStat.st_size)
                while True:
                    buffer = source_handle.read(self.COPY_BUFFER_SIZE)
                    if not buffer:
                        break

                    transfer(destination_handle.write(buffer))

        callback.added(source)

    if posix is not None and hasattr(posix, '_fcopyfile'):
        # The implementation is MAC os - there is

        def _copyfile(self, source: str, destination: str, callback):
            """ Copy a regular file content or metadata by using high-performance
            fcopyfile(3) syscall (macOS).
            """
            with open(source, 'rb') as source_handle:
                with open(destination, 'rb') as destination_handle:

                    posix._fcopyfile(
                        source_handle.fileno(),
                        destination_handle.fileno(),
                        posix._COPYFILE_DATA
                    )

            callback.added(source)

    elif hasattr(os, "sendfile"):
        # Linux with sendfile protocol

        def _copyfile(self, source: str, destination: str, sourceStat: os.stat_result, callback):

            with open(source, 'rb') as source_handle:
                with open(destination, 'rb') as destination_handle:

                    infd = source_handle.fileno()
                    outfd = destination_handle.fileno()

                    try:
                        blocksize = max(os.fstat(infd).st_size, 2 ** 23)  # min 8MiB
                    except OSError:
                        blocksize = 2 ** 27  # 128MiB

                    # On 32-bit architectures truncate to 1GiB to avoid OverflowError,
                    # see bpo-38319.
                    if sys.maxsize < 2 ** 32:
                        blocksize = min(blocksize, 2 ** 30)

                    transfer = callback.get_bytes_transfer(destination, sourceStat.st_size)

                    offset = 0
                    while True:
                        try:
                            sent = os.sendfile(outfd, infd, offset, blocksize)

                        except OSError as err:
                            # ...in oder to have a more informative exception.
                            err.filename = source_handle.name
                            err.filename2 = destination_handle.name

                            if err.errno == errno.ENOTSOCK:
                                # sendfile() on this platform (probably Linux < 2.6.33)
                                # does not support copies between regular files (only
                                # sockets).
                                self._copyfile = self._defaultcopyfile
                                return self._defaultcopyfile(source, destination, callback=callback)

                            if err.errno == errno.ENOSPC:  # filesystem is full
                                raise err from None

                            raise err
                        else:
                            if sent == 0:
                                break  # EOF
                            offset += sent
                            transfer(sent)

            callback.added(source)

    elif os.name == 'nt':

        def _copyfile(self, source: str, destination: str, sourceStat: os.stat_result, callback):
            """ readinto()/memoryview() based variant of copyfile
            """

            # Open the source and destination location
            with open(source, 'rb') as source_handle:
                with open(destination, 'wb') as destination_handle:

                    # Ensure that there is a need to transfer any data
                    source_length = sourceStat.st_size
                    if not source_length:
                        return

                    # Setup the read buffer for readinto and callback tracker
                    transfer = callback.get_bytes_transfer(destination, source_length)
                    read_size = min(source_length, self.COPY_BUFFER_SIZE)

                    # Create a bytes array and a view onto it -
                    with memoryview(bytearray(read_size)) as mv:
                        while True:
                            read = source_handle.readinto(mv)
                            if not read:
                                # Nothing more to read
                                break

                            elif read < read_size:
                                # Ensure that only part written into is written to destination
                                with mv[:read] as smv:
                                    transfer(destination_handle.write(smv))

                            else:
                                # Entire buffer replaced with read - write entire buffer
                                transfer(destination_handle.write(mv))

            callback.added(source)

    else:

        _copyfile = _defaultcopyfile


    def _copystats(
        self,
        source: str,
        destination: str,
        sourceStat: os.stat_result,
        modified_time: Optional[float] = None,
        accessed_time: Optional[float] = None
        ):

        # Set the times of the destination
        os.utime(
            destination,
            ns=(
                (sourceStat.st_atime_ns if accessed_time is None else int(accessed_time*1e-3)),
                (sourceStat.st_mtime_ns if modified_time is None else int(modified_time*1e-3)),
            )
        )

        # Copy any other system attributes
        _copyExtendedAttribues(source, destination)

        # Set the permissions of the destination object
        os.chmod(destination, stat.S_IMODE(sourceStat.st_mode))

        return sourceStat

    if os.name == 'posix':

        def _copystatsWrapper(function):
            def wrapped(source, destination, *args, **kwargs):
                stat = function(source, destination, *args, **kwargs)
                os.chflag(destination, stat.st_flags)
            return wrapped

        _copystats = _copystatsWrapper(_copystats)

    def _copytree(
        self,
        source: str,
        destination: str,
        sourceStat: os.stat_result,
        callback: AbstractCallback,
        modified_time: Optional[float] = None,
        accessed_time: Optional[float] = None
        ):

        # Ensure the desintation
        os.makedirs(destination, exist_ok=True)

        # Add that you are copying self
        callback.addTaskCount(1, isAdding=True)

        # Scan the directory - Copy all the entries to the new location
        with os.scandir(source) as scandir_it:
            for entry in scandir_it:

                subdestination = os.path.join(destination, entry.name)
                entryStat = entry.stat()

                if entry.is_dir():
                    self._copytree(
                        entry.path,
                        subdestination,
                        entryStat,
                        callback,
                        modified_time,
                        accessed_time
                    )
                else:
                    callback.addTaskCount(1, isAdding=True)

                    sourceStat = entry.stat()
                    self._copyfile(
                        entry.path,
                        subdestination,
                        entryStat,
                        callback,
                    )
                    self._copystats(
                        entry.path,
                        subdestination,
                        entryStat,
                        modified_time,
                        accessed_time
                    )

        # Copy the directory stats to the new location
        self._copystats(
            source,
            destination,
            sourceStat,
            modified_time,
            accessed_time
        )
        callback.added(source)


    def _get(
        self,
        source: Artefact,
        destination: str,
        *,
        callback: AbstractCallback,
        modified_time: Optional[float] = None,
        accessed_time: Optional[float] = None,
        **kwargs
        # worker_config: Optional[WorkerPoolConfig] = None,
        ):

        # Convert source path
        sourceAbspath = source.abspath
        sourceStat = os.stat(sourceAbspath)

        if isinstance(source, File):
            self._copyfile(
                sourceAbspath,
                destination,
                sourceStat,
                callback=callback,
            )
            self._copystats(
                sourceAbspath,
                destination,
                sourceStat,
                modified_time,
                accessed_time
            )
            callback.added(destination)

        else:

            self._copytree(
                sourceAbspath,
                destination,
                sourceStat,
                callback=callback,
                modified_time=modified_time,
                accessed_time=accessed_time
            )

    def _getBytes(self, source: Artefact, *, callback = None) -> bytes:
        with open(self._abspath(source.path), "rb") as handle:
            return handle.read()

    def _put(
        self,
        source: Artefact,
        destination: str,
        /,
        callback: AbstractCallback,
        modified_time: Optional[float] = None,
        accessed_time: Optional[float] = None,
        worker_config: Optional[WorkerPoolConfig] = None,
        **kwargs
        ):
        """ For remote sources - we want to 'get' the artefact and place it directly at the destination location. This
        is exactly what remote managers are expecting when get is called. As such, by relying on the `artefact.save`
        which in turn calls get, we can handle local and remote managers by ensuring that our local get method copies
        files to the destination, and remote managers download the data to target location.

        """

        # Convert destination path
        destinationAbspath = self._abspath(destination)

        # Save the source to the destination
        # NOTE save calls the get method of the source manager - which is the most efficient method of dowloading
        # the artefact to the local fs which is is trying todo.
        source.save(
            destinationAbspath,
            callback=callback,
            modified_time=modified_time,
            accessed_time=accessed_time,
            worker_config=worker_config,
        )

        # Create a partial artefact for the newly downloaded file
        return PartialArtefact(self, destination)

    def _putBytes(
        self,
        fileBytes: bytes,
        destination: str,
        *,
        callback,
        modified_time: Optional[datetime.datetime] = None,
        accessed_time: Optional[datetime.datetime] = None,
        **kwargs
        ):

        # Convert destination path
        destinationAbspath = self._abspath(destination)

        # Makesure the destination exists
        os.makedirs(os.path.dirname(destinationAbspath), exist_ok=True)

        # Write the byte file
        transfer = callback.get_bytes_transfer(destination, len(fileBytes))
        with open(destinationAbspath, "wb") as handle:
            transfer(handle.write(fileBytes))

        artefact = PartialArtefact(self, destination)

        # Update the new artefact time if provided
        if any((modified_time, accessed_time)):
            utils.utime(destinationAbspath, modified_time=modified_time, accessed_time=accessed_time)

        return artefact

    def _cp(
        self,
        source: Artefact,
        destination: str,
        *,
        callback: AbstractCallback,
        modified_time: Optional[float] = None,
        accessed_time: Optional[float] = None,
        **kwargs
        ):
        return self._get(
            source,
            self._abspath(destination),
            callback=callback,
            modified_time=modified_time,
            accessed_time=accessed_time,
        )

    def _mv(self, source: str, destination: str, *args, **kwargs):

        sourceAbspath = self._abspath(source)
        destinationAbspath = self._abspath(destination)

        # Ensure the destination location
        os.makedirs(os.path.dirname(destinationAbspath), exist_ok=True)

        # Move the source artefact
        os.rename(sourceAbspath, destinationAbspath)

        return PartialArtefact(self, self._relative(destinationAbspath))

    def _ls(self, directory: str, recursive: bool = False):

        # Get a path to the folder
        abspath = self._abspath(directory)

        # Iterate over the folder and identify every object - add the created
        with os.scandir(abspath) as scandir_it:
            for entry in scandir_it:
                art = self._identifyPath(entry)
                yield art
                if recursive and isinstance(art, Directory):
                    yield from self._ls(art.path, recursive=recursive)

    def _rmtree(self, path: str, callback = None):

        # TODO check if it is faster to separate them out into two lists and then iterate over them
        # or is it faster to iterate one straight away (given the call to callback would have to be run more)

        # Scan the directory
        directory_entries, file_entries = [], []
        with os.scandir(path) as scandir_it:
            for entry in scandir_it:
                if entry.is_dir(follow_symlinks=False):
                    directory_entries.append(entry)
                else:
                    file_entries.append(entry)

        # Record the number of items to delete - All files in this directory plus self (child directires will add themselves)
        callback.addTaskCount(len(file_entries) + 1, isAdding=False)

        # For each artefact in path - delete or recursively delete
        for directory_entry in directory_entries:
            self._rmtree(directory_entry.path, callback=callback)

        for file_entry in file_entries:
                os.remove(file_entry.path)
                callback.removed(file_entry.path)

        os.rmdir(path)
        callback.removed(path)

    def _rm(self, artefact: Artefact, *, callback):

        # Convert the artefact
        path = self._abspath(artefact.path)

        if isinstance(artefact, Directory):
            self._rmtree(path, callback=callback)

        else:
            os.remove(path)
            callback.removed(path)

    if os.name == 'nt':
        @classmethod
        def _signatureFromURL(cls, url: urllib.parse.ParseResult):
            return {'drive': url.scheme or 'c'}, os.path.splitdrive(os.path.abspath(os.path.expanduser(url.path)))[1]

        def toConfig(self):
            return {'manager': 'FS', 'path': self._root, 'drive': self._drive}
    else:
        @classmethod
        def _signatureFromURL(cls, url: urllib.parse.ParseResult):
            return {}, os.path.abspath(os.path.expanduser(url.path))

        def toConfig(self):
            return {'manager': 'FS', 'path': self._root}
    @property
    def root(self):
        return self._root


    class CommandLineConfig:
        def __init__(self, manager):
            self._manager = manager

        @staticmethod
        def arguments() -> typing.List[typing.Tuple]:
            return [
                (('-r', '--root'), {'help': 'The root/cwd location of the manager'})
            ]

        def initialise(self, kwargs):
            # return self._manager(kwargs.get('root',))
            return self._manager()


class RootFS(FS):

    if os.name == 'nt':
        def _abspath(self, managerPath: str) -> str:
            return self._drive + ':' + os.path.splitdrive(os.path.abspath(managerPath))[1]

    else:
        def _abspath(self, managerPath: str) -> str:
            return os.path.abspath(managerPath)

    def _relative(self, abspath: str) -> str:
        return abspath

class SubdirectoryFS(FS):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._rootLength = len(self._root)

    def _cwd(self):
        return self.SEPARATOR

    def _abspath(self, managerPath: str) -> str:
        return os.path.abspath(self.join(self._root, managerPath, joinAbsolutes=True))

    def _relative(self, abspath: str) -> str:
        return abspath[self._rootLength:] or os.sep
