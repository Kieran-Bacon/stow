import os
import stat
import datetime
import shutil
import urllib
from typing import Optional

import binascii
import hashlib

if os.name == 'nt':
    import ctypes

from ..artefacts import Artefact, File, Directory, PartialArtefact, HashingAlgorithm
from ..manager.base_managers import LocalManager

class FS(LocalManager):
    """ Wrap a local filesystem (a networked drive or local directory)

    Args:
        path (str): The local relative path to where the manager is to be initialised
    """

    def __new__(cls, path: str = None, drive: str = None):
        # Note - Though the arguments must match the instances called from super, they do not have to have the same
        # defaults. This allows use to handle the default behaviour differently for the managers

        if path is None:
            return super().__new__(RootFS)

        else:
            return super().__new__(SubdirectoryFS)

    if os.name == 'nt':

        def __init__(self, path: str = os.path.sep, drive: str = 'c'):
            super().__init__()

            self._drive = drive
            expected_drive, self._path = os.path.splitdrive(path)

            if expected_drive and drive.lower() != expected_drive[:-1].lower():
                raise ValueError(f'Drive letter passed does not match drive letter of path: {drive} != ({expected_drive}):{self._path}')

            self._root = self._drive + ':' + self._path

    else:
        def __init__(self, path: str = os.path.sep, drive: str = ''):
            super().__init__()
            self._drive = drive
            _, self._root = os.path.splitdrive(path)
            self._path = self._root

    def _mklink(self, source: str, destination: str):
        os.symlink(self._abspath(source), self._abspath(destination))
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

    def _identifyPath(self, managerPath: str):

        try:

            abspath = self._abspath(managerPath)

            artefactStat = os.stat(abspath)

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
                raise NotImplementedError(f'Amazon does not provide {algorithm} hashing')

    def _get(self, source: Artefact, destination: str, *, callback = None):

        # Convert source path
        sourceAbspath = self._abspath(source.path)

        # Identify download method
        method = shutil.copytree if os.path.isdir(sourceAbspath) else shutil.copy

        # Download
        method(sourceAbspath, destination)

    def _getBytes(self, source: Artefact, *, callback = None) -> bytes:

        with open(self._abspath(source.path), "rb") as handle:
            return handle.read()

    def _put(
        self,
        source: Artefact,
        destination: str,
        *,
        metadata = None,
        callback = None,
        modified_time: Optional[datetime.datetime] = None,
        accessed_time: Optional[datetime.datetime] = None,
        **kwargs
        ):

        # Convert destination path
        destinationAbspath = self._abspath(destination)

        # Ensure the destination
        os.makedirs(os.path.dirname(destinationAbspath), exist_ok=True)

        fileUpdators = []
        if modified_time is not None or accessed_time is not None:
            def updateFileTimes(artefact: Artefact):
                self._setArtefactTimes(
                    modified_time=(modified_time or artefact.modifiedTime.timestamp()),
                    accessed_time=(accessed_time or artefact.accessedTime.timestamp())
                )
            fileUpdators.append(updateFileTimes)

        # Select the put method
        with source.localise() as sourceAbspath:

            if isinstance(source, Directory):
                shutil.copytree(sourceAbspath, destinationAbspath)

                if fileUpdators:
                    for artefact in self._ls(destinationAbspath):
                        for updator in fileUpdators:
                            updator(artefact)

            else:
                shutil.copy(sourceAbspath, destinationAbspath)

                if fileUpdators:
                    for updator in fileUpdators:
                        updator(destinationAbspath)

        return PartialArtefact(self, destination)

    def _putBytes(
        self,
        fileBytes: bytes,
        destination: str,
        *,
        metadata = None,
        callback = None,
        modified_time: Optional[datetime.datetime] = None,
        accessed_time: Optional[datetime.datetime] = None,
        **kwargs
        ):

        # Convert destination path
        destinationAbspath = self._abspath(destination)

        # Makesure the destination exists
        os.makedirs(os.path.dirname(destinationAbspath), exist_ok=True)

        # Write the byte file
        with open(destinationAbspath, "wb") as handle:
            handle.write(fileBytes)

        return PartialArtefact(self, destination)

    def _cp(self, source: Artefact, destination: str):
        return self._put(source, destination)

    def _mv(self, source: str, destination: str):

        sourceAbspath = self._abspath(source)
        destinationAbspath = self._abspath(destination)

        # Ensure the destination location
        os.makedirs(os.path.dirname(destinationAbspath), exist_ok=True)

        # Move the source artefact
        os.rename(sourceAbspath, destinationAbspath)

        return PartialArtefact(self, self._relative(destinationAbspath))

    def _ls(self, directory: str):

        # Get a path to the folder
        abspath = self._abspath(directory)

        # Iterate over the folder and identify every object - add the created
        for art in os.listdir(abspath):
            artefact = self._identifyPath(
                self.join(directory, art)
            )

            if artefact is not None:
                yield artefact

    def _rm(self, artefact: Artefact):

        # Convert the artefact
        path = self._abspath(artefact.path)

        # Select method for deleting
        method = shutil.rmtree if isinstance(artefact, Directory) else os.remove

        # Remove the artefact
        method(path)

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
