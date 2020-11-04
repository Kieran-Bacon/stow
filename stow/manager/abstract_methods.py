from abc import ABC, abstractmethod, abstractclassmethod
import urllib.parse
import typing
import contextlib

from ..artefacts import Artefact, File, Directory

class AbstractManager(ABC):

    @abstractmethod
    def __repr__(self):
        pass

    @abstractmethod
    def _abspath(self, managerPath):
        """ Return the most accurate path to an object in the managers vernacular. Opposite of relpath

        examples:
            local managers shall convert a relative path to its full absolute os compatible filepath
            s3 shall convert the relative path to a s3 valid key

        Args:
            artefact (str): The artefact object or it's relative path which is to be converted
        """
        pass

    @abstractmethod
    def _makeFile(self, abspath: str) -> File:
        """ Make a file object using the underlying implementation objects from a manager relative path

        Args:
            abspath: Manager absolute path

        Returns:
            File: The File object representing the on disk data object
        """
        pass

    @abstractmethod
    def _makeDirectory(self, abspath: str) -> Directory:
        """ Make a directory object using the underlying implementation objects from a manager relative path

        Args:
            abspath: Manager absolute path

        Returns:
            File: The File object representing the on disk data object
        """
        pass

    @abstractmethod
    def _identifyPath(self, abspath: str) -> typing.Union[Artefact, None]:
        """ Look at the underying implementation and get an artefact that represents the object at th path given if it
        exists. If no object could be found, return None

        Args:
            abspath: The path for artefact on disk

        Returns:
            typing.Union[Artefact, None]: The artefact object that represents the item on disk or None if nothing exists
        """
        pass

    @abstractmethod
    def _get(self, source: Artefact, destination: str):
        """ Fetch the artefact and downloads its data to the local destination path provided

        The existence of the file to collect has already been checked so this function can be written to assume its
        existence

        Args:
            source: The source object and context that is to be downloaded
            destination: The local path to where the source is to be written
        """
        pass

    @abstractmethod
    def _getBytes(self, source: File) -> bytes:
        """ Fetch the file artefact contents directly. This is to avoid having to write the contents of files to discs
        for some of the other operations.

        The existence of the file to collect has already been checked so this function can be written to assume its
        existence

        Args:
            source: The source object and context that is to be downloaded

        Returns:
            bytes: The bytes content of the disk
        """
        pass

    @abstractmethod
    def _put(self, source: str, destination: str):
        """ Put the local filesystem object onto the underlying manager implementation using the absolute paths given.

        To avoid user error - an artefact cannot be placed onto a Directory unless an overwrite toggle has been passed
        which is False by default. This should protect them from accidentally deleting a directory.

        In the event that they want to do so - the deletion of the directory will be handled before operating this
        function. Therefore their is no need to check/protect against it. (famous last words)

        Args:
            source: A local path to an artefact (File or Directory)
            destination: A manager abspath path for the artefact
        """
        pass

    @abstractmethod
    def _putBytes(self, fileBytes: bytes, destination: str):
        """ Put the bytes of a file object onto the underlying manager implementation using the absolute path given.

        This function allows processes to avoid writing files to disc for speedier transfers.

        If its not possible to transmit bytes - I'd recommend writing the bytes to a tempfile and then operating the
        put method.

        Args:
            fileBytes (bytes): files bytes
            destinationAbsPath (str): Remote absolute path
        """
        pass

    @abstractmethod
    def _cp(self, source: Artefact, destination: str):
        """ Method for copying an artefact local to the manager to an another location on the manager. Implementation
        would avoid having to download data from a manager to re-upload that data.

        If there isn't a method of duplicating the data on the manager, you can call
            self._put(self._abspath(source.path), destination)

        Which will mean the behaviour defaults to the put action.

        Args:
            source: the manager local source file
            destination: a manager abspath path for destination
        """
        pass

    @abstractmethod
    def _mv(self, source: Artefact, destination: str):
        """ Method for moving an artefact local to the manager to an another location on the manager. Implementation
        would avoid having to download data from a manager to re-upload that data.

        If there isn't a method of duplicating the data on the manager, you can call
            self._put(self._abspath(source.path), destination)
            self._rm(self._abspath(source.path))

        Which will mean the behaviour defaults to the put action and then a delete of the original file. Achieving the
        same goal.

        Args:
            source: the manager local source file
            destination: a manager abspath path for destination
        """
        pass

    @abstractmethod
    def _ls(self, directory: Directory):
        """ List all artefacts that are present at the directory objects location and add them into the manager.

        The existence of the directory has already been confirmed.

        TODO _loadArtefact
        This method can be used in conjunction with self._makeFile and self._makeDirectory to great affect:

        1. You can list the items in the directory and call makeFile and makeDirectory on them and collect
        created objects to be returned or

        1. Have ls add all files and directories when called (good when you can download multiple metadata at once for
        no cost) and then have makeFile call ls on its parent directory before hand so that it can return the created
        file object by ls.

        Food for thought.

        Args:
            managerPath: the manager abspath to the directory whose content is to be indexed
        """
        pass

    @abstractmethod
    def _rm(self, artefact: Artefact):
        """ Delete the underlying artefact data on the manager.

        To avoid possible user error in deleting directories, the user must have already indicated that they want to
        delete everything

        Args:
            artefact: The artefact on the manager to be deleted
        """
        pass

    @abstractclassmethod
    def _loadFromProtocol(cls, url: urllib.parse.ParseResult):
        """ Create a new instance of the manager using the information passed via the url ParseResult object that will
        have been created via the stateless interface

        Args:
            url: The result of passing the stateless path through urllib.parse.urlparse

        Returns:
            Manager: A manager of this type loaded with information from the url

        Raises:
            Error: Errors due to missing information and so on
        """
        pass

    @contextlib.contextmanager
    @abstractmethod
    def localise(self, artefact: typing.Union[Artefact, str]):
        """ TODO
        """
        pass

    @abstractmethod
    def toConfig(self) -> dict:
        """ TODO """
        pass