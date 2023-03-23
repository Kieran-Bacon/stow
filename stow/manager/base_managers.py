
import os
import abc
import typing
import tempfile
import contextlib
import datetime

from ..artefacts import Artefact
from .manager import Manager

class LocalManager(Manager, abc.ABC):
    """ Abstract Base Class for managers that will be working with local artefacts.
    """

    def _setArtefactTimes(self, path: str, modified_time: float, accessed_time: float) -> None:
        return os.utime(path, (accessed_time, modified_time))

    def _setmtime(
        self,
        artefact: Artefact,
        _datetime: typing.Union[float, datetime.datetime]
        ) -> datetime.datetime:

        if isinstance(_datetime, float):
            timestamp = _datetime
            _datetime = datetime.datetime.fromtimestamp(_datetime)
        else:
            timestamp = _datetime.timestamp()

        self._setArtefactTimes(
            artefact.abspath,
            timestamp,
            artefact.accessedTime.timestamp()
        )

        return _datetime

    def _setatime(
        self,
        artefact: Artefact,
        _datetime: typing.Union[float, datetime.datetime]
        ):

        if isinstance(_datetime, float):
            timestamp = _datetime
            _datetime = datetime.datetime.fromtimestamp(_datetime)
        else:
            timestamp = _datetime.timestamp()

        self._setArtefactTimes(
            artefact.abspath,
            artefact.modifiedTime.timestamp(),
            timestamp
        )

        return _datetime

    @contextlib.contextmanager
    def localise(self, artefact: typing.Union[Artefact, str]):

        _, _, path = self._splitManagerArtefactForm(artefact, load=False)

        abspath = self._abspath(path)
        os.makedirs(os.path.dirname(abspath), exist_ok=True)

        try:
            yield abspath
        except Exception as e:
            raise

class RemoteManager(Manager, abc.ABC):
    """ Abstract Base Class for managers that will be working with remote artefacts so efficiency with fetching and
    pushing files is important for time and bandwidth
    """

    @staticmethod
    def _compare(dict1, dict2, key):
        # Extract the two sets of keys
        keys1, keys2 = set(dict1[key].keys()), set(dict2[key].keys())
        return keys1.difference(keys2), keys1.intersection(keys2), keys2.difference(keys1)

    @classmethod
    def _parseHierarchy(cls, path, _toplevel=None):

        # Store separately the directories and files of the path
        directories = {}
        files = {}

        # For each item process their checksums
        for item in os.listdir(path):

            # Identify their absolute path and relative manager path from the temporary local files
            abspath = os.path.join(path, item)

            if os.path.isdir(abspath):
                directories[abspath] = cls._parseHierarchy(abspath, _toplevel=path)

            else:
                files[abspath] = cls.md5(abspath)

        return {"directories": directories, "files": files}

    @classmethod
    def _compareHierarhy(cls, original, new):

        # Data containers for files and directory comparison
        toPush, toDelete = set(), set()

        # Compare the directories
        removed, editted, added = cls._compare(original, new, "directories")
        for directory in editted:
            put, delete = cls._compareHierarhy(original['directories'][directory], new['directories'][directory])

            # Union the result of the comparison on the sub directory level
            added |= put
            removed |= delete

        toPush |= added
        toDelete |= removed

        # Compare the files
        removed, editted, added = cls._compare(original, new, "files")
        for file in editted:
            if original['files'][file] != new['files'][file]:
                # The checksum of the files are not the same, therefore, the file has been editted and needs to be pushed
                added.add(file)

        toPush |= added
        toDelete |= removed

        return toPush, toDelete

    @contextlib.contextmanager
    def localise(self, artefact):

        # Load the artefacts from the remote
        _, obj, path = self._splitManagerArtefactForm(artefact, require=False)

        # Setup a location locally to be able to work with the files
        exception = None
        with tempfile.TemporaryDirectory() as directory:

            # Generate a temporay path for the file to be downloaded into
            local_path = os.path.join(directory, self.basename(path))

            # Get the contents and put it into the temporay directory
            if obj is not None:
                self.get(path, local_path)

                if os.path.isdir(local_path):
                    # To collected item is a directory - walk the directory and record its state
                    checksum = self._parseHierarchy(local_path)

                else:
                    # Generate a checksum for the file
                    checksum = self.md5(local_path)

            else:
                # No checksum for no object
                checksum = None

            # Return the local path to the object
            try:
                yield local_path
            except Exception as e:
                exception = e

            # The user has stopped interacting with the artefact - resolve any differences with manager
            if os.path.exists(local_path):
                if checksum:
                    if os.path.isdir(local_path):
                        # Compare the new hiearchy - update only affected files/directories
                        put, delete = self._compareHierarhy(checksum, self._parseHierarchy(local_path))

                        # Define the method for converting the abspath back to the manager relative path
                        contexualise = lambda x: self.join(path, x[len(local_path)+1:], separator='/')

                        # Put/delete the affected artefacts
                        for abspath in put: self.put(abspath, contexualise(abspath))
                        for abspath in delete: self.rm(contexualise(abspath), recursive=True)

                    elif self.md5(local_path) != checksum:
                        # The file has been changed - upload the file's contents
                        self.put(self._localLoad(local_path), path)

                else:
                    # New item - put the artefact into the manager
                    self.put(self._localLoad(local_path), path)

        if exception is not None:
            raise exception