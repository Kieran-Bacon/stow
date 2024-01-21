from abc import ABC, abstractmethod
import urllib.parse
from typing import (
    Union, Optional, Dict, Generator, Tuple, Any
)
from typing_extensions import Self
import contextlib

from ..worker_config import WorkerPoolConfig
from ..types import TimestampLike, HashingAlgorithm
from ..storage_classes import StorageClass
from ..artefacts import Artefact, File, Directory, ArtefactType, ArtefactOrPathLike, Metadata
from ..callbacks import AbstractCallback

class AbstractManager:
    """ The abstract manager interface - outlines and details the methods that should be implemented
    by developers that want to extend the stow manager library
    """

    # @abstractmethod
    # def __repr__(self):
    #     pass

    @abstractmethod
    def _cwd(self) -> str:
        """ Return the default working directory for the manager - used to default the artefact path if no path provided

        Returns:
            str: The default path of the manager, the current working directory
        """
        pass

    @abstractmethod
    def _abspath(self, managerPath: str) -> str:
        """ Return the absolute path for a manager path, including the managers protocol, hostname, and parameters.

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
    def _exists(self, managerPath: str) -> bool:
        """ Return whether a artefact exists at the path given

        Args:
            managerPath (str): The path to the artefact

        Returns:
            bool: True if it exists else False
        """
        pass

    @abstractmethod
    def _identifyPath(self, managerPath: str) -> Optional[ArtefactType]:
        """ For the path given, create an `Artefact` for the object at the location on the manager but do not add it
        into the manager. If no object exists - return None

        Args:
            abspath: The path for artefact on disk

        Returns:
            typing.Union[Artefact, None]: The artefact object that represents the item on disk or None if nothing exists
        """
        pass

    @abstractmethod
    def _isLink(self, file: str) -> bool:
        """ Check if the file object given is a link/shortcut to another file """
        pass

    @abstractmethod
    def _isMount(self, directory: str) -> bool:
        """ Check if the file object given is a mount point """
        pass

    @abstractmethod
    def _get(
        self,
        source: Artefact,
        destination: str,
        /,
        callback: AbstractCallback,
        worker_config: WorkerPoolConfig,
        modified_time: Optional[float],
        accessed_time: Optional[float],
        ):
        """ Fetch the artefact and downloads its data to the local destination path provided

        The existence of the file to collect has already been checked so this function can be written to assume its
        existence

        Args:
            source: The source object and context that is to be downloaded
            destination: The local path to where the source is to be written
            *,
            Callback (AbstractCallback) = None: A callback class to be initialised by the
                downloaded method, and passed bytes transfered counts.
        """
        pass

    @abstractmethod
    def _getBytes(self, source: Artefact, /, callback: AbstractCallback) -> bytes:
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
    def _put(
        self,
        source: Artefact,
        destination: str,
        /,
        callback: AbstractCallback,
        metadata: Optional[Metadata],
        modified_time: Optional[TimestampLike],
        accessed_time: Optional[TimestampLike],
        content_type: Optional[str],
        storage_class: Optional[StorageClass],
        worker_config: WorkerPoolConfig,
        ) -> ArtefactType:
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
    def _putBytes(
        self,
        fileBytes: bytes,
        destination: str,
        *,
        callback: AbstractCallback,
        metadata: Optional[Metadata],
        modified_time: Optional[float],
        accessed_time: Optional[float],
        content_type: Optional[str],
        storage_class: Optional[StorageClass],
        ) -> File:
        """ Put the bytes of a file object onto the underlying manager implementation using the absolute path given.

        This function allows processes to avoid writing files to disc for speedier transfers.

        If it's not possible to transmit bytes - I'd recommend writing the bytes to a tempfile and then operating the
        put method.

        Args:
            fileBytes (bytes): files bytes
            destinationAbsPath (str): Remote absolute path
            TODO
        """
        pass

    @abstractmethod
    def _cp(
        self,
        source: Artefact,
        destination: str,
        /,
        callback: AbstractCallback,
        worker_config: WorkerPoolConfig,
        metadata: Optional[Metadata],
        modified_time: Optional[float],
        accessed_time: Optional[float],
        content_type: Optional[str],
        storage_class: Optional[StorageClass],
        ) -> ArtefactType:
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
    def _mv(
        self,
        source: Artefact,
        destination: str,
        /,
        callback: AbstractCallback,
        worker_config: WorkerPoolConfig,
        metadata: Optional[Metadata],
        modified_time: Optional[float],
        accessed_time: Optional[float],
        storage_class: Optional[StorageClass],
        content_type: Optional[str],
        ) -> ArtefactType:
        """ Move an artefact from its location to another location managed by the same manager class. This method should
        attempt exploit manager implementation to have transfer done remotely, and avoid having data downloaded to be
        pushed.

        An example of this could be the s3 mv feature - though an artefact and its destination maybe in different
        buckets, it is possible to transfer artefacts inside s3.

        As such, it is important that the method can handle destinations that would be managed by other manager
        instances. If this is not possible, the static final variable cls.ISOLATED should be set TRUE. Which will mean
        the behaviour defaults to the put action and then a delete of the original file. Achieving the same goal.

        Args:
            source (str): An absolute path to the source object (from _abspath) to be moved
            destination (str): An absolute path to the destination (from _abspath of the destination manager)
        """
        pass

    @abstractmethod
    def _ls(
        self,
        directory: str,
        recursive: bool = False,
        *,
        include_metadata: bool = False,
        worker_config: Optional[WorkerPoolConfig] = None,
        ) -> Generator[ArtefactType, None, None]:
        """ List all artefacts that are present at the directory objects location and add them into the manager.

        Args:
            managerPath: the manager path to the directory whose content is to be indexed
        """
        pass

    @abstractmethod
    def _overwrite(self, manager: Self, artefact: Optional[ArtefactType], overwrite: bool, callback: AbstractCallback):
        """ Signals that the provided argument is about to be overwritten - take any actions necessary to permit that to
        happen or raise error if not acceptable.

        Default manager behaviour is governed by environment variables
        - SAFE_FILE_OVERWRITE
        - SAFE_DIRECTORY_OVERWRITE
        If not safe then they will be deleted via `rm` before writing

        Args:
            artefact: The artefact to be overwritten
            overwrite (bool): The flag to indicate whether the user is agrees to overwrite
            callback (AbstractCallback): A callback that could track the consequences
        """
        pass

    @abstractmethod
    def _rm(self, *artefact: str, callback: AbstractCallback, worker_config: WorkerPoolConfig):
        """ Delete the underlying artefact data on the manager.

        To avoid possible user error in deleting directories, the user must have already indicated that they want to
        delete everything

        Args:
            artefact: The artefact on the manager to be deleted
        """
        pass

    @abstractmethod
    def _digest(self, file: File, algorithm: HashingAlgorithm):
        pass

    @classmethod
    @abstractmethod
    def _signatureFromURL(cls, url: urllib.parse.ParseResult) -> Tuple[Dict[str, Any], str]:
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

    @property
    @abstractmethod
    def protocol(self) -> str:
        """ The protocol for the manager e.g. fs/s3/k8s """
        pass

    @property
    @abstractmethod
    def root(self) -> str:
        """ Returns the root information for the manager (name or path) """
        pass

    @abstractmethod
    def localise(self, artefact: ArtefactOrPathLike) -> str:
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

    @property
    @abstractmethod
    def config(self) -> Dict[str, str]:
        """ Generate a config which can be unpacked into the `connect` interface to initialise this
        manager. To be used to seralise and de-seralise a manager object.

        NOTE Defaulted values or environment variables are not guaranteed to be saved

        Returns:
            dict: A dictionary of the kwargs of the init of the manager
        """
        pass
