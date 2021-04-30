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

    def _abspath(self, managerPath: str) -> str:
        path = self.join(self._path, managerPath, joinAbsolutes=True)

        if os.name == 'nt':
            path = path.replace('/', '\\')

        return path

    def _identifyPath(self, managerPath: str):

        abspath = self._abspath(managerPath)

        if os.path.exists(abspath):

            stats = os.stat(abspath)

            # Created time
            createdTime = datetime.datetime.utcfromtimestamp(stats.st_ctime)
            createdTime = pytz.UTC.localize(createdTime)

            # Modified time
            modifiedTime = datetime.datetime.utcfromtimestamp(stats.st_mtime)
            modifiedTime = pytz.UTC.localize(modifiedTime)

            # Access time
            accessedTime = datetime.datetime.utcfromtimestamp(stats.st_atime)
            accessedTime = pytz.UTC.localize(accessedTime)

            if os.path.isfile(abspath):
                return File(
                    self,
                    managerPath,
                    stats.st_size,
                    modifiedTime,
                    createdTime,
                    accessedTime,
                )

            elif os.path.isdir(abspath):
                return Directory(
                    self,
                    managerPath,
                    createdTime=createdTime,
                    modifiedTime=modifiedTime,
                    accessedTime=accessedTime,
                )

        return None

    def _get(self, source: Artefact, destination: str):

        # Convert source path
        sourceAbspath = self._abspath(source.path)

        # Identify download method
        method = shutil.copytree if os.path.isdir(sourceAbspath) else shutil.copy

        # Download
        method(sourceAbspath, destination)

    def _getBytes(self, source: Artefact) -> bytes:

        with open(self._abspath(source.path), "rb") as handle:
            return handle.read()

    def _put(self, source: str, destination: str):

        # Convert destination path
        destinationAbspath = self._abspath(destination)

        # Ensure the destination
        os.makedirs(os.path.dirname(destinationAbspath), exist_ok=True)

        # Select the put method
        method = shutil.copytree if os.path.isdir(source) else shutil.copy

        # Perform the putting
        method(source, destinationAbspath)

    def _putBytes(self, fileBytes: bytes, destination: str):

        # Convert destination path
        destinationAbspath = self._abspath(destination)

        # Makesure the destination exists
        os.makedirs(os.path.dirname(destinationAbspath), exist_ok=True)

        # Write the byte file
        with open(destinationAbspath, "wb") as handle:
            handle.write(fileBytes)

    def _cp(self, source: Artefact, destination: str):
        self._put(self._abspath(source.path), destination)

    def _mv(self, source: Artefact, destination: str):

        # Convert the source and destination
        source, destination = self._abspath(source.path), self._abspath(destination)

        # Ensure the destination location
        os.makedirs(os.path.dirname(destination), exist_ok=True)

        # Move the source artefact
        os.rename(source, destination)

    def _ls(self, directory: str):

        # Get a path to the folder
        abspath = self._abspath(directory)

        # Iterate over the folder and identify every object - add the created
        for art in os.listdir(abspath):
            self._addArtefact(
                self._identifyPath(
                    self.join(directory, art, separator='/')
                )
            )

    def _rm(self, artefact: Artefact):

        # Convert the artefact
        artefact = self._abspath(artefact.path)

        # Select method for deleting
        method = shutil.rmtree if os.path.isdir(artefact) else os.remove

        # Remove the artefact
        method(artefact)

    @classmethod
    def _signatureFromURL(cls, url: urllib.parse.ParseResult):
        return {"path": "/"}, os.path.abspath(os.path.expanduser(url.path))

    def toConfig(self):
        return {'manager': 'FS', 'path': self._path}