import os
import datetime
import shutil
import urllib
import typing

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

    def __new__(cls, path: str = os.path.sep):

        manager = super().__new__(cls)

        if path == os.path.sep:
            return super().__new__(RootFS)

        else:
            return super().__new__(SubdirectoryFS)

        os.symlink()

    if os.name == 'nt':
        def __init__(self, path: str = os.path.sep):
            super().__init__()
            self._drive, self._path = os.path.splitdrive(path)
            self._drive = self._drive or 'C:'
            self._root = self._drive + self._path

        # def _mklink(self, source: str, destination: str):

        #     source, destination = self._abspath(source), self._abspath(destination)

        #     kdll = ctypes.windll.LoadLibrary("kernel32.dll")
        #     kdll.CreateSymbolicLinkA(source, destination, 0)
        #     return PartialArtefact(self, destination)

        #     # win32file.CreateSymbolicLink(source.abspath, destination, 1)

    else:
        def __init__(self, path: str = os.path.sep):
            super().__init__()
            self._drive, self._root = os.path.splitdrive(path)
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

            stats = os.stat(abspath)

            # Export artefact created time information
            createdTime = datetime.datetime.fromtimestamp(stats.st_ctime, tz=datetime.timezone.utc)
            modifiedTime = datetime.datetime.fromtimestamp(stats.st_mtime, tz=datetime.timezone.utc)
            accessedTime = datetime.datetime.fromtimestamp(stats.st_atime, tz=datetime.timezone.utc)

            if os.path.isfile(abspath):
                return File(
                    self,
                    self._relative(abspath),
                    stats.st_size,
                    modifiedTime,
                    createdTime=createdTime,
                    accessedTime=accessedTime,
                )

            elif os.path.isdir(abspath):
                return Directory(
                    self,
                    self._relative(abspath),
                    createdTime=createdTime,
                    modifiedTime=modifiedTime,
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

    def _put(self, source: str, destination: str, *, metadata = None, callback = None):

        # Convert destination path
        destinationAbspath = self._abspath(destination)

        # Ensure the destination
        os.makedirs(os.path.dirname(destinationAbspath), exist_ok=True)

        # Select the put method
        with source.localise() as sourceAbspath:
            method = shutil.copytree if os.path.isdir(sourceAbspath) else shutil.copy

            # Perform the putting
            method(sourceAbspath, destinationAbspath)

        return PartialArtefact(self, destination)

    def _putBytes(self, fileBytes: bytes, destination: str, *, metadata = None, callback = None):

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
                self.join(directory, art, separator='/')
            )

            if artefact is not None:
                yield artefact

    def _rm(self, artefact: Artefact):

        # Convert the artefact
        artefact = self._abspath(artefact.path)

        # Select method for deleting
        method = shutil.rmtree if os.path.isdir(artefact) else os.remove

        # Remove the artefact
        method(artefact)

    @classmethod
    def _signatureFromURL(cls, url: urllib.parse.ParseResult):
        # TODO figure this out
        # print("what", url.path)
        # print("the", (os.path.expanduser(url.path)))
        # print("fuck", os.path.abspath(os.path.expanduser(url.path)))
        # from nt import _getfullpathname
        # print("mate", _getfullpathname(url.path))

        return {}, os.path.abspath(os.path.expanduser(url.path))

    @property
    def root(self):
        return self._root

    def toConfig(self):
        return {'manager': 'FS', 'path': self._root}

class RootFS(FS):

    def _abspath(self, managerPath: str) -> str:
        return os.path.abspath(managerPath)

    def _relative(self, abspath: str) -> str:
        return abspath

class SubdirectoryFS(FS):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._rootLength = len(self._root)

    def _cwd(self):
        return self._root

    def _abspath(self, managerPath: str) -> str:
        return os.path.abspath(self.join(self._root, managerPath, joinAbsolutes=True))

    def _relative(self, abspath: str) -> str:
        return abspath[self._rootLength:] or os.sep
