
import os
import abc
import typing
import tempfile
import contextlib
import datetime

from ..artefacts import Artefact
from .manager import Manager, Localiser

class LocalLocaliser(Localiser):

    def __init__(self, abspath: str):

        self._abspath = abspath
        os.makedirs(os.path.dirname(abspath), exist_ok=True)

    def start(self):
        return self._abspath

    def close(self):
        pass


class RemoteLocaliser(Localiser):

    def __init__(self, manager: Manager, artefact: typing.Optional[Artefact], path: str):

        self._manager = manager
        self._artefact = artefact
        self._path = path

        self._local_path = None
        self._checksum = None

    @staticmethod
    def _compare(dict1, dict2, key):
        # Extract the two sets of keys
        keys1, keys2 = set(dict1[key].keys()), set(dict2[key].keys())
        return keys1.difference(keys2), keys1.intersection(keys2), keys2.difference(keys1)

    def _parseHierarchy(self, path, _toplevel=None):

        # Store separately the directories and files of the path
        directories = {}
        files = {}

        # For each item process their checksums
        for item in os.listdir(path):

            # Identify their absolute path and relative manager path from the temporary local files
            abspath = os.path.join(path, item)

            if os.path.isdir(abspath):
                directories[abspath] = self._parseHierarchy(abspath, _toplevel=path)

            else:
                files[abspath] = self._manager.md5(abspath)

        return {"directories": directories, "files": files}

    def _compareHierarhy(self, original, new):

        # Data containers for files and directory comparison
        toPush, toDelete = set(), set()

        # Compare the directories
        removed, editted, added = self._compare(original, new, "directories")
        for directory in editted:
            put, delete = self._compareHierarhy(original['directories'][directory], new['directories'][directory])

            # Union the result of the comparison on the sub directory level
            added |= put
            removed |= delete

        toPush |= added
        toDelete |= removed

        # Compare the files
        removed, editted, added = self._compare(original, new, "files")
        for file in editted:
            if original['files'][file] != new['files'][file]:
                # The checksum of the files are not the same, therefore, the file has been editted and needs to be pushed
                added.add(file)

        toPush |= added
        toDelete |= removed

        return toPush, toDelete

    def start(self):

        # Setup a location locally to be able to work with the files
        directory = tempfile.mkdtemp()

        # Generate a temporay path for the file to be downloaded into
        self._local_path = local_path = os.path.join(directory, self._manager.basename(self._path))

        # Get the contents and put it into the temporay directory
        if self._artefact is not None:
            self._manager.get(self._path, local_path)

            if os.path.isdir(local_path):
                # To collected item is a directory - walk the directory and record its state
                self._checksum = self._parseHierarchy(local_path)

            else:
                # Generate a checksum for the file
                self._checksum = self._manager.md5(local_path)

        else:
            # No checksum for no object
            self._checksum = None

        return local_path

    def close(self):

        # The user has stopped interacting with the artefact - resolve any differences with manager
        if os.path.exists(self._local_path):
            if self._checksum:
                if os.path.isdir(self._local_path):
                    # Compare the new hiearchy - update only affected files/directories
                    put, delete = self._compareHierarhy(self._checksum, self._parseHierarchy(self._local_path))

                    # Define the method for converting the abspath back to the manager relative path
                    contexualise = lambda x: self._manager.join(self._path, x[len(self._local_path)+1:], separator=self._manager.SEPARATOR)

                    # Put/delete the affected artefacts
                    for abspath in put: self._manager.put(abspath, contexualise(abspath))
                    for abspath in delete: self._manager.rm(contexualise(abspath), recursive=True)

                elif self._manager.md5(self._local_path) != self._checksum:
                    # The file has been changed - upload the file's contents
                    self._manager.put(self._local_path, self._path)

            else:
                # New item - put the artefact into the manager
                self._manager.put(self._local_path, self._path)

class LocalManager(Manager, abc.ABC):
    """ Abstract Base Class for managers that will be working with local artefacts.
    """

    def _cwd(self) -> str:
        """ Return the default working directory for the manager - used to default the artefact path if no path provided

        Returns:
            str: The default path of the manager, the current working directory
        """
        return os.getcwd()

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

    def localise(self, artefact: typing.Union[Artefact, str]) -> Localiser:
        _, _, path = self._splitManagerArtefactForm(artefact, load=False)
        return LocalLocaliser(self._abspath(path))

class RemoteManager(Manager, abc.ABC):
    """ Abstract Base Class for managers that will be working with remote artefacts so efficiency with fetching and
    pushing files is important for time and bandwidth
    """

    def _cwd(self) -> str:
        """ Return the default working directory for the manager - used to default the artefact path if no path provided

        Returns:
            str: The default path of the manager, the current working directory
        """
        return '/'

    def localise(self, artefact: typing.Union[Artefact, str]) -> Localiser:
        _, obj, path = self._splitManagerArtefactForm(artefact, require=False)
        return RemoteLocaliser(self, obj, path)
