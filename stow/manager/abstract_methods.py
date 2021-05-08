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
    def _abspath(self, managerPath: str) -> str:
        """ Return the absolute path on the backend provider from the standardised manager path.

        Args:
            managerPath: The manager relative path which is to be converted to an absolute path

        Returns:
            str: The manager absolute path

        Examples:
            For the filesystem, this will be the full absolute path to the object. For s3 this is the key of the object.

            >>> stow.connect(manager='FS', path='/home/ubuntu')._abspath('/hello/there')
            '/home/ubuntu/hello/there'
            >>> stow.connect(manager='s3', bucket='bucket-example')._abspath('/hello/there')
            'hello/there'

        """
        pass


    @abstractmethod
    def _identifyPath(self, managerPath: str) -> typing.Union[Artefact, None]:
        """ For the path given, create an `Artefact` for the object at the location on the manager but do not add it
        into the manager. If no object exists - return None

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
    def _getBytes(self, source: Artefact) -> bytes:
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
        function. Therefore there is no need to check/protect against it. (famous last words)

        Args:
            source: A local absolute path to an artefact (File or Directory)
            destination: A manager abspath path for the artefact
        """
        pass

    @abstractmethod
    def _putBytes(self, fileBytes: bytes, destination: str):
        """ Put the bytes of a file object onto the underlying manager implementation using the absolute path given.

        This function allows processes to avoid writing files to disc for speedier transfers.

        If it's not possible to transmit bytes - I'd recommend writing the bytes to a tempfile and then operating the
        put method.

        Args:
            fileBytes (bytes): files bytes
            destinationAbsPath (str): Remote absolute path
        """
        pass

    @abstractmethod
    def _cp(self, source: Artefact, destination: str):
        """ Method for copying an artefact local to the manager to another location on the manager. Implementation
        would avoid having to download data from a manager to re-upload that data.

        If there isn't a method of duplicating the data on the manager, you can call
            self._put(self._abspath(source.path), destination)

        Which will mean the behaviour defaults to the put action.

        Args:
            source: the manager local source artefact
            destination: a manager abspath path for destination
        """
        pass

    @abstractmethod
    def _mv(self, source: Artefact, destination: str):
        """ Method for moving an artefact local to the manager to another location on the manager. Implementation
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
    def _ls(self, directory: str):
        """ List all artefacts that are present at the directory objects location and add them into the manager.

        Args:
            managerPath: the manager path to the directory whose content is to be indexed
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
    def _signatureFromURL(cls, url: urllib.parse.ParseResult):
        """ Create the signature that can be passed to the init of the manager to create a new instance using the
        information passed via the url ParseResult object that will have been created via the stateless interface

        Args:
            url: The result of passing the stateless path through urllib.parse.urlparse

        Returns:
            Manager: A manager of this type loaded with information from the url
            Relpath: The manager relative path for the artefact that may have been referenced

        Raises:
            Error: Errors due to missing information and so on
        """
        pass

    @contextlib.contextmanager
    @abstractmethod
    def localise(self, artefact: typing.Union[Artefact, str]) -> str:
        """ Localise an artefact, ensure that there is an absolute path that reaches this artefact. For local artefacts
        this will be the direct abspath. Remote managers will get the artefact, and pass back the path to this local
        version.

        A path is still returned even if the artefact doesn't exist. It will be the responsibility of the calling method
        to handle what is localised.

        Args:
            artefact: The artefact path or artefact object

        Yields:
            str: The abspath for the artefact
        """
        pass

    @abstractmethod
    def toConfig(self) -> dict:
        """ Generate a config which can be unpacked into the `connect` interface to initialise this manager. To be
        used to seralise and de-seralise a manager object.

        NOTE Defaulted values or environment variables are not guaranteed to be saved

        Returns:
            dict: A dictionary of the kwargs of the init of the manager
        """
        pass