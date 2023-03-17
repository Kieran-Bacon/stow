import os
import datetime
import shutil
import urllib
import typing

from ..artefacts import Artefact, File, Directory, PartialArtefact
from ..manager.base_managers import LocalManager

class FS(LocalManager):
    """ Wrap a local filesystem (a networked drive or local directory)

    Args:
        path (str): The local relative path to where the manager is to be initialised
    """

    def __new__(cls, path: str = os.path.sep):

        manager = super().__new__(cls)

        if path != os.path.sep:
            # The class is not the default system wide FS manager - overload the functions to
            # perform a translation of provided paths

            # Update the current working directory information
            cwd = path if os.path.isdir(path) else os.path.dirname(path)
            manager._cwd = lambda: cwd

            # Update the absolute path method to become relative to the root
            def relativeAbspath(managerPath: str) -> str:
                return os.path.abspath(cls.join(path, managerPath, joinAbsolutes=True))
            manager._abspath = relativeAbspath

            # Absolute to relative
            rootLength = len(path)
            def relative(abspath: str) -> str:
                return abspath[rootLength:]
            manager._relative = relative

        return manager

    def __init__(self, path: str = os.path.sep):
        super().__init__()
        self._root = path

    def __repr__(self):
        return '<Manager(FS)>'

    def _abspath(self, managerPath: str) -> str:
        return os.path.abspath(managerPath)

    def _relative(self, abspath: str) -> str:
        return abspath

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

    def _get(self, source: Artefact, destination: str, *, Callback = None):

        # Convert source path
        sourceAbspath = self._abspath(source.path)

        # Identify download method
        method = shutil.copytree if os.path.isdir(sourceAbspath) else shutil.copy

        # Download
        method(sourceAbspath, destination)

    def _getBytes(self, source: Artefact, *, Callback = None) -> bytes:

        with open(self._abspath(source.path), "rb") as handle:
            return handle.read()

    def _put(self, source: str, destination: str, *, metadata = None, Callback = None):

        # Convert destination path
        destinationAbspath = self._abspath(destination)

        # Ensure the destination
        os.makedirs(os.path.dirname(destinationAbspath), exist_ok=True)

        # Select the put method
        method = shutil.copytree if os.path.isdir(source) else shutil.copy

        # Perform the putting
        method(source, destinationAbspath)

        return PartialArtefact(self, destination)

    def _putBytes(self, fileBytes: bytes, destination: str, *, metadata = None, Callback = None):

        # Convert destination path
        destinationAbspath = self._abspath(destination)

        # Makesure the destination exists
        os.makedirs(os.path.dirname(destinationAbspath), exist_ok=True)

        # Write the byte file
        with open(destinationAbspath, "wb") as handle:
            handle.write(fileBytes)

        return PartialArtefact(self, destination)

    def _cp(self, source: Artefact, destination: str):
        return self._put(self._abspath(source.path), destination)

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