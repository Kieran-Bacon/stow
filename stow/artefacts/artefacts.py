import os
import abc
import datetime
import typing
from typing import Union, Optional, Dict
from typing_extensions import Self

from .interfaces import ManagerInterface, Localiser
from ..storage_classes import StorageClassInterface
from ..worker_config import WorkerPoolConfig
from ..types import HashingAlgorithm, TimestampLike, StrOrPathLike
from .. import utils
from ..callbacks import AbstractCallback, DefaultCallback
from .. import exceptions

class ArtefactReloader:
    def __new__(cls, protocol, config, path):
        from ..manager.manager import Manager
        return Manager.connect(manager=protocol, **config)[path]

class Artefact:
    """ Artefacts are the items that are being stored - it is possible that through another mechanism that these items
    are deleted and they are no longer able to work

    Args:
        manager: The manager this file belongs to
        path: The file's relative path
    """

    def __init__(
        self,
        manager: ManagerInterface,
        path: str,
        *,
        createdTime: Optional[datetime.datetime] = None,
        modifiedTime: Optional[datetime.datetime] = None,
        accessedTime: Optional[datetime.datetime] = None,
        metadata: Optional[Dict[str, str]] = None,
        ):

        self._manager = manager  # Link back to the owning manager
        self._path = path  # Relative path on manager

        self._createdTime = createdTime
        self._modifiedTime = modifiedTime
        self._accessedTime = accessedTime
        self._metadata = metadata

    def __reduce__(self):
        return (ArtefactReloader, (self._manager.protocol, self._manager.config, self._path))

    def __hash__(self):
        return hash(self.abspath)

    def __eq__(self, other: "Artefact"):
        return (
            isinstance(other, self.__class__) and
            self._manager is other._manager and
            type(self) == type(other) and
            self.path == other.path
        )

    def __fspath__(self):
        return self._manager._abspath(self._path)

    @property
    def abspath(self) -> str:
        """ Get the absolute path to the object for the manager """
        return self._manager._abspath(self._path)

    @property
    def directory(self) -> 'Directory':
        """ Directory object this artefact exists within """
        return self._manager.artefact(self._manager.dirname(self._path), type=Directory)

    @property
    def metadata(self):
        """ Get accessible file metadata as hosted by the manager """
        if self._metadata is None:
            self._metadata = self._manager._metadata(self._path)
        return self._metadata

    @property
    def createdTime(self) -> datetime.datetime:
        """ UTC localised datetime of time file last modified by a write/append method """
        return self._createdTime or self.modifiedTime

    @property
    def modifiedTime(self) -> datetime.datetime:
        """ UTC localised datetime of time file last modified by a write/append method """
        if self._modifiedTime is None:
            raise exceptions.ArtefactMetadataUndefined(f"{self} does not have modified time defined")
        return self._modifiedTime

    @modifiedTime.setter
    def modifiedTime(self, _datetime: typing.Union[float, datetime.datetime]):
        self._modifiedTime = self._manager.setmtime(self, _datetime)

    @property
    def accessedTime(self) -> datetime.datetime:
        """ UTC localised datetime of time file last modified by a write/append method """
        return (self._accessedTime or self.modifiedTime)
    @accessedTime.setter
    def accessedTime(self, _datetime: typing.Union[float, datetime.datetime]):
        self._accessedTime = self._manager.setatime(self, _datetime)


    @property
    def path(self) -> str:
        """ Return the manager relative path to this Artefact """
        return self._path
    @path.setter
    def path(self, path: str):
        """ Move the file on the target (perform the rename) - if it fails do not change the local file name """
        self._manager.mv(self, path)
        self._path = path

    @property
    def basename(self):
        """ Basename of the artefact - holding directory path removed leaving filename and extension """
        self._manager.basename(self.abspath)
        return self._manager.basename(self)
    @basename.setter
    def basename(self, basename: str):
        path: str = self._manager.join(self._manager.dirname(self._path), self._manager.basename(basename))
        self._manager.mv(self, path)
        self._path = path

    @property
    def name(self):
        """ Name of artefact - for `File` this is without extension """
        return self.basename
    @name.setter
    def name(self, name: str):
        self.basename = name

    def localise(self) -> Localiser:
        """ Localise this artefact

        Returns:
            Localiser: The context manager object
        """
        return self._manager.localise(self)

    def save(
        self,
        path: str,
        force: bool = False,
        *,
        callback: AbstractCallback = DefaultCallback(),
        modified_time: Optional[TimestampLike] = None,
        accessed_time: Optional[TimestampLike] = None,
        worker_config: Optional[WorkerPoolConfig] = None,
        ) -> Self:

        return self._manager.get(
            self,
            path,
            overwrite=force,
            callback=callback,
            modified_time=modified_time,
            accessed_time=accessed_time,
            worker_config=worker_config
        )

    def delete(self, force: bool = False):
        """ Delete this artefact from the disk

        Args:
            force: An "are you sure" for directories

        Raises:
            OperationNotPermitted: If directory and deletion has not been deleted
        """
        self._manager.rm(self, recursive=force)

class File(Artefact):
    """ A filesystem file object - a container of bytes representing some data

    Args:
        manager: The submanager this file belongs to
        path: The file's relative path
        modifiedTime: The time the file was last modified via a write/append operation
        size: The size in bytes of the file content
    """

    def __init__(
        self,
        manager,
        path: str,
        size: int,
        modifiedTime: datetime.datetime,
        *,
        content_type: Optional[str] = None,
        metadata: Optional[typing.Dict[str, str]] = None,
        createdTime: Optional[datetime.datetime] = None,
        accessedTime: Optional[datetime.datetime] = None,
        digest: Optional[typing.Dict[HashingAlgorithm, str]] = None,
        isLink: Optional[bool] = None,
        storage_class: Optional[StorageClassInterface] = None
        ):
        super().__init__(
            manager,
            path,
            modifiedTime=modifiedTime,
            createdTime=createdTime,
            accessedTime=accessedTime,
            metadata=metadata
        )

        self._size = size  # The size in bytes of the object
        self._content_type = content_type
        self._digest = digest or {}  # A signature for the file content
        self._isLink = isLink
        self._storage_class = storage_class

    def __len__(self): return self.size
    def __repr__(self):
        return '<stow.File: {} modified({}) size({} bytes)>'.format(self._path, self._modifiedTime, self._size)

    @property
    def storage_class(self) -> Optional[StorageClassInterface]:
        return self._storage_class
    @storage_class.setter
    def storage_class(self, sclass: StorageClassInterface):
        # Either we have a new method on the managers that allows us access to the storage class for that manager or
        # we simply call put and fetch the update from the response object1
        artefact = self._manager.put(self, self, content_type=self.content_type, metadata=self.metadata, storage_class=sclass)
        self._storage_class = artefact.storage_class
        self._createdTime = artefact._createdTime
        self._modifiedTime = artefact._modifiedTime
        self._accessedTime = artefact._accessedTime

    @property
    def content_type(self):
        if self._content_type is None:
            self._content_type = self._manager._get_content_type(self._path)

        return self._content_type

    @content_type.setter
    def content_type(self, value: str):
        self._content_type = value
        self._manager._set_content_type(self._path, value)

    @property
    def name(self):
        if "." not in self.basename:
            return self.basename
        return self.basename[:self.basename.rindex(".")]

    @name.setter
    def name(self, name: str):
        ext = self.extension
        if ext:
            self.basename = "{}.{}".format(name, ext)

        else:
            self.basename = name

    @property
    def extension(self):
        """ File extension string - extention indicates file purpose and associated applications """
        if "." not in self.path:
            return ""
        return self.path[self.path.rindex(".")+1:]
    @extension.setter
    def extension(self, ext: str):
        self.basename = ".".join([self.name, ext])

    def content(self, content: Optional[bytes] = None) -> bytes:
        """ file content as bytes """

        if content is not None:
            if not isinstance(content, bytes):
                raise ValueError("Cannot set the content of the file to non bytes type - {} given".format(type(content)))

            with self.open("wb") as handle:
                handle.write(content)

            return content

        else:
            with self.open("rb") as handle:
                return handle.read()

    def set_artefact_time(
        self,
        modified_time: Optional[TimestampLike] = None,
        accessed_time: Optional[TimestampLike] = None
        ) -> tuple[float, float]:
        """ TODO """
        times = modified_time, accessed_time = self._manager.set_artefact_time(self, modified_time, accessed_time)
        self._modifiedTime = datetime.datetime.fromtimestamp(modified_time, tz=datetime.timezone.utc)
        self._accessedTime = datetime.datetime.fromtimestamp(accessed_time, tz=datetime.timezone.utc)
        return times

    def digest(self, algorithm: HashingAlgorithm = HashingAlgorithm.MD5) -> str:
        """ Get the file digest to verify validaty - if a manager does not have it's own method of creatin file digests
        the md5 checksum will be used for the file contents.
        """
        if algorithm not in self._digest:
            self._digest[algorithm] = self._manager.digest(self, algorithm)

        return self._digest[algorithm]

    @property
    def size(self) -> int:
        """ Size of file content in bytes """
        return self._size

    def isLink(self):
        if self._isLink is None:
            self._isLink = self._manager._isLink(self.path)
        return self._isLink

    @typing.overload
    def open(self, mode: typing.Literal['r', 'w'] = 'r', **kwargs) -> typing.TextIO:
        pass
    @typing.overload
    def open(self, mode: typing.Literal['rb', 'wb'], **kwargs) -> typing.BinaryIO:
        pass
    def open(self, mode: str = 'r', **kwargs) -> typing.IO[typing.Any]:
        """ Context manager to allow the pulling down and opening of a file """
        return self._manager.open(self, mode, **kwargs)

class Directory(Artefact):
    """ A directory represents an local filesystems directory or folder. Directories hold references to other
    directories or files

    Args:
        manager (stow.Manager): The manager this directory object belongs to
        path (str): the manager relative path for the object
        contents (set): collection of artefacts which reside within this directory
        *,
        collected (bool): Toggle as to whether the directory contents has been collected (false when JIT Loading)
    """

    def __init__(
        self,
        manager,
        path: str,
        *,
        createdTime: Optional[datetime.datetime] = None,
        modifiedTime: Optional[datetime.datetime] = None,
        accessedTime: Optional[datetime.datetime] = None,
        metadata: Optional[Dict[str, str]] = None,
        isMount: Optional[bool] = None
        ):
        super().__init__(
            manager,
            path,
            createdTime=createdTime,
            modifiedTime=modifiedTime,
            accessedTime=accessedTime,
            metadata=metadata
        )

        self._isMount = isMount

    def __len__(self): return len(self.ls())
    def __iter__(self): return self._manager.iterls(self, recursive=False, ignore_missing=False)
    def __repr__(self): return '<stow.Directory: {}>'.format(self._path)
    def __contains__(self, artefact: typing.Union[Artefact, str]) -> bool:

        if isinstance(artefact, str):
            return self._manager.exists(
                self._manager.join(self, artefact, separator='/', joinAbsolutes=True)
            )

        elif isinstance(artefact, Artefact):
            return (
                self._manager == artefact._manager and
                artefact._path.startswith(self._path)
            )

        else:
            raise TypeError(f"Directory ({self}) contains does not support type {type(artefact)} ({artefact})")

    @property
    def modifiedTime(self) -> Optional[datetime.datetime]:
        return self._modifiedTime

    def isMount(self):
        if self._isMount is None:
            self._isMount = self._manager._isMount(self.path)
        return self._isMount

    def mkdir(self, path: str) -> "Directory":
        """ Create a directory nested inside this `Directory` with the relative path given

        Args:
            path: Relative path to directory, path to new directory location

        Returns:
            Directory: The newly created directory object
        """
        return self._manager.mkdir(self._manager.join(self._path, path, separator='/', joinAbsolutes=True)) # type: ignore

    def touch(self, path: str) -> File:
        """ Touch a file at given location relative to this Directory

        Args:
            path: The relative path to directory to touch new file

        Returns:
            File: The newly created file object
        """
        return self._manager.touch(self._manager.join(self._path, path, separator='/', joinAbsolutes=True)) # type: ignore

    def relpath(self, artefact: typing.Union[Artefact, str], separator: str = os.sep) -> str:
        """ Assuming the artefact is a member of this directory, return a filepath which is relative to this directory

        Args:
            artefact: the artefact who's path will be made relative

        Returns:
            str: the relative path to the artefact from this directory

        Raises:
            ArtefactNotMember: raised when artefact is not a member of the directory
        """

        # Get the path
        if isinstance(artefact, Artefact):
            path = artefact.path
        else:
            path = artefact

        # # Raise error if the artefact is not a member of the directory
        # if not path.startswith(self.path):
        #     raise exceptions.ArtefactNotMember(
        #         "Cannot create relative path for Artefact {} as its not a member of {}".format(artefact, self)
        #     )

        # Return the path
        return self._manager.relpath(path, self.path, separator=separator)

    def open(self, path: str, mode: str = "r", **kwargs) -> typing.IO[typing.AnyStr]:
        """ Open a file and create a stream to that file. Expose interface of `open`

        Args:
            path: Path to directory object
            mode: The open method
            kwargs: kwargs to be passed to the interface of open

        Yields:
            io.IOBase: An IO object depending on the mode for interacting with the file
        """
        return self._manager.open(
            self._manager.join(self._path, path, separator='/', joinAbsolutes=True),
            mode,
            **kwargs
        )

    def rm(
            self,
            *paths: str,
            recursive: bool = False,
            callback: AbstractCallback = DefaultCallback(),
            ignore_missing: bool = False,
            worker_config: Optional[WorkerPoolConfig] = None,
        ):
        """ Remove an artefact at the given location

        Args:
            artefact: Path that is to be deleted
            recursive: If the target is a directory, whether to delete recursively the directories contents

        Raises:
            OperationNotPermitted: In the even the target is a directory and recursive has not been toggled
        """


        return self._manager.rm(
            *([self._manager.join(self, path, joinAbsolutes=True) for path in paths] if paths else (self,)),
            recursive=recursive,
            callback=callback,
            ignore_missing=ignore_missing,
            worker_config=worker_config,
        )

    def iterls(
        self,
        path: Optional[StrOrPathLike] = None,
        recursive: bool = False,
        *,
        ignore_missing: bool = False
        ) -> typing.Generator[Union[File, "Directory"], None, None]:
        """ Create a generator over the contents of this object (or sub-directory)

        Args:
            path (str) = None: A prefix to objects in this object to be listed
            recursive (bool) = False: List contents of directories if True
            *,
            ignore_missing (bool) = False: Do not raise ArtefactNotFound if target doesn't exist

        Returns:
            Generator[Artefact]: A generator of artefact objects, generating according to the
                underlying storage manager

        Raises:
            ArtefactNoLongerExists: If the directory can no longer be found in the manager, and
                ignore_missing is False
        """

        try:
            yield from self._manager.iterls(
                self if path is None else self._manager.join(self.path, path, separator='/'),
                recursive=recursive,
                ignore_missing=ignore_missing
            ) # type: ignore
        except exceptions.ArtefactNotFound as e:
            raise exceptions.ArtefactNoLongerExists(f"Directory {self} no longer exists") from e

    def ls(
        self,
        path: Optional[str] = None,
        recursive: bool = False,
        *,
        ignore_missing: bool = False
        ) -> typing.Set[Artefact]:
        """ List the contents of this directory, or directory's directories.

        Args:
            path: The path to sub directory whose contents is to be returned
            recursive: Whether to recursively fetch all child contents for child directories

        Returns:
            typing.Set[Artefact]: The collection of objects within the targeted directory

        Raises:
            ArtefactNoLongerExists: If the directory can no longer be found in the manager, and
                ignore_missing is False
        """
        return set(self.iterls(self, recursive=recursive, ignore_missing=ignore_missing))

    def isEmpty(self) -> bool:
        """ Check whether the directory has contents

        Returns:
            bool: True when there is at least one item in the directory False when the directory is empty
        """
        for _ in self._manager.iterls(self._path, recursive=False, ignore_missing=False):
            return False
        else:
            return True

    def empty(self):
        """ Empty the directory of contents """
        for artefact in self.ls():
            self._manager.rm(artefact, recursive=True)

class PartialArtefact(File, Directory):

    def __init__(self, manager, path: str):
        self._manager = manager
        self._path = path

    def __str__(self):
        # Trigger the update of this partial object and return the value of it's string before it overwrites this!
        return self.__getattribute__('__str__')()

    def __fspath__(self):
        return self._manager._abspath(self._path)

    def __getattribute__(self, attr: str):

        # For debugger only (only way this function can be called twice)
        if object.__getattribute__(self, "__class__").__name__ != "PartialArtefact":                  # pragma: no cover
            return object.__getattribute__(self, attr)                                                # pragma: no cover

        # Get the artefact information
        manager = object.__getattribute__(self, "_manager")
        path = object.__getattribute__(self, "_path")

        try:
            artefact = manager[path]
        except exceptions.ArtefactNotFound as e:
            # Though we have created a partial artefact through an action we have taken that should result in an
            # artefact being created, the artefact was not found meaning that the artefact has since been deleted.
            raise exceptions.ArtefactNoLongerExists("Artefact has been removed") from e

        object.__setattr__(self, '__class__', type(artefact.__class__.__name__, (artefact.__class__,),{}))
        object.__setattr__(self, '__dict__', artefact.__dict__)

        if attr == "__class__":
            return artefact.__class__
        else:
            return object.__getattribute__(self, attr)

    def __setattr__(self, __name: str, __value: typing.Any) -> None:
        if __name not in ['_manager', '_path']:

            getattr(self, '_path')
        object.__setattr__(self, __name, __value)
