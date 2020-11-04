import os
import datetime
import shutil
import pytz
import urllib

from ..artefacts import Artefact, File, Directory
from ..manager import LocalManager

class FS(LocalManager):
    """ Wrap a local filesystem (a networked drive or local directory)

    Args:
        path (str): The local relative path to where the manager is to be initialised
    """

    def __init__(self, path: str):
        # Record the local path to the original directory
        self._path = os.path.abspath(path)
        super().__init__()

    def __repr__(self): return '<Manager(FS): {}>'.format(self._path)

    def _abspath(self, managerPath):
        return os.path.abspath(os.path.join(self._path, managerPath[1:]))

    def _makeFile(self, path: str):
        abspath = self._abspath(path)

        if not os.path.exists(abspath):
            with open(abspath, "w"):
                pass

        stats = os.stat(abspath)

        # Created time
        createdTime = datetime.datetime.utcfromtimestamp(stats.st_mtime)
        createdTime = pytz.UTC.localize(createdTime)

        # Modified time
        modifiedTime = datetime.datetime.utcfromtimestamp(stats.st_mtime)
        modifiedTime = pytz.UTC.localize(modifiedTime)

        # Access time
        accessedTime = datetime.datetime.utcfromtimestamp(stats.st_mtime)
        accessedTime = pytz.UTC.localize(accessedTime)

        return File(
            self,
            path,
            stats.st_size,
            modifiedTime,
            createdTime,
            accessedTime,
        )


    def _makeDirectory(self, path: str):
        abspath = self._abspath(path)

        if not os.path.exists(abspath):
            os.path.makedirs(abspath)

        stats = os.stat(abspath)

        # Created time
        createdTime = datetime.datetime.utcfromtimestamp(stats.st_mtime)
        createdTime = pytz.UTC.localize(createdTime)

        # Modified time
        modifiedTime = datetime.datetime.utcfromtimestamp(stats.st_mtime)
        modifiedTime = pytz.UTC.localize(modifiedTime)

        # Access time
        accessedTime = datetime.datetime.utcfromtimestamp(stats.st_mtime)
        accessedTime = pytz.UTC.localize(accessedTime)

        return Directory(
            self,
            path,
            createdTime=createdTime,
            modifiedTime=modifiedTime,
            accessedTime=accessedTime,
        )

    def _identifyPath(self, path: str):

        abspath = self._abspath(path)

        if os.path.exists(abspath):
            if os.path.isfile(abspath):
                return self._makeFile(path)

            elif os.path.isdir(abspath):
                return self._makeDirectory(path)

        return None

    def _get(self, src_remote: Artefact, dest_local: str):

        # Get the absolute path to the object
        src_remote = self.abspath(src_remote.path)

        # Identify download method
        method = shutil.copytree if os.path.isdir(src_remote) else shutil.copy

        # Download
        method(src_remote, dest_local)

    def _getBytes(self, source: File) -> bytes:

        with open(self._abspath(source), "rb") as handle:
            fileBytes = handle.read()

        return fileBytes

    def _put(self, src_local, dest_remote):

        if os.path.isdir(src_local):
            # Copy the directory into place
            shutil.copytree(src_local, dest_remote)

        else:
            # Putting a file
            os.makedirs(os.path.dirname(dest_remote), exist_ok=True)
            shutil.copy(src_local, dest_remote)

    def _putBytes(self, source, destinationAbsPath):

        # Makesure the destination exists
        os.makedirs(os.path.dirname(destinationAbsPath), exist_ok=True)

        # Write the byte file
        with open(destinationAbsPath, "wb") as handle:
            handle.write(source)

    def _cp(self, srcObj: Artefact, destPath: str):
        self._put(self.abspath(srcObj.path), self.abspath(destPath))

    def _mv(self, srcObj: Artefact, destPath: str):

        absDestination = self._abspath(destPath)
        os.makedirs(os.path.dirname(absDestination), exist_ok=True)
        os.rename(self._abspath(srcObj.path), absDestination)

    def _ls(self, directory: Directory):

        # Get a path to the folder
        abspath = self._abspath(directory.path)

        # Iterate over the folder and identify every object - add the created
        for art in os.listdir(abspath):
            self._addArtefact(
                self._identifyPath(
                    self.join(directory.path, art)
                )
            )

    def _rm(self, artefact: Artefact):

        abspath = self.abspath(artefact.path)
        if not os.path.exists(abspath): return # NOTE the file has already been deleted - copy directory has this affect

        if isinstance(artefact, Directory):
            shutil.rmtree(abspath)
        else:
            os.remove(abspath)

    @classmethod
    def _loadFromProtocol(cls, url: urllib.parse.ParseResult):
        return cls(url.path)

    def toConfig(self):
        return {'manager': 'FS', 'path': self._path}