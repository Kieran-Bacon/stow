import os
import abc
import io
import datetime
import contextlib
import typing
import enum

from . import _utils as utils
from . import exceptions

class HashingAlgorithm(enum.Enum):
    MD5 = enum.auto()
    CRC32 = enum.auto()
    CRC32C = enum.auto()
    SHA1 = enum.auto()
    SHA256 = enum.auto()

class ArtefactReloader:
    def __new__(self, config, path):
        return utils.connect(**config)[path]

class Artefact:
    """ Artefacts are the items that are being stored - it is possible that through another mechanism that these items
    are deleted and they are no longer able to work

    Args:
        manager: The manager this file belongs to
        path: The file's relative path
    """

    def __init__(
        self,
        manager,
        path: str
        ):

        self._manager = manager  # Link back to the owning manager
        self._path = path  # Relative path on manager

    def __reduce__(self):
        return (ArtefactReloader, (self._manager.toConfig(), self._path))

    def __hash__(self):
        return hash(self.abspath)

    def __eq__(self, other: "Artefact"):
        return (
            self.manager is other.manager and
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
    def manager(self):
        """ Return the manager object this Artefact belongs to """
        return self._manager

    @property
    def directory(self):
        """ Directory object this artefact exists within """
        return self._manager[self._manager.dirname(self._path)]

    @property
    @abc.abstractmethod
    def modifiedTime(self):
        """ UTC localised datetime of time file last modified by a write/append method """
        pass

    @property
    @abc.abstractmethod
    def createdTime(self):
        """ UTC localised datetime of time file last modified by a write/append method """
        pass

    @property
    @abc.abstractmethod
    def accessedTime(self):
        """ UTC localised datetime of time file last modified by a write/append method """
        pass

    @property
    def path(self):
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
        return self._manager.basename(self)
    @basename.setter
    def basename(self, basename: str):
        path = self._manager.join(self._manager.dirname(self._path), self._manager.basename(basename))
        self.manager.mv(self, path)
        self._path = path

    @property
    def name(self):
        """ Name of artefact - for `File` this is without extension """
        return self.basename
    @name.setter
    def name(self, name: str):
        self.basename = name

    @abc.abstractmethod
    @contextlib.contextmanager
    def localise(self):
        pass

    def save(self, path: str, force: bool = False):
        """ Save the artefact to a local location

        Args:
            path: A local path where the Artefact is to be saved
            force: Ignore artefacts at the destination location

        Raises:
            OperationNotPermitted: If the location given is a Directory and the get is not enforced
        """
        self._manager.get(self, path)

    def delete(self, force: bool = False):
        """ Delete this artefact from the disk

        Args:
            force: An "are you sure" for directories

        Raises:
            OperationNotPermitted: If directory and deletion has not been deleted
        """
        self.manager.rm(self, recursive=force)

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
        size: float,
        modifiedTime: datetime.datetime,
        *,
        content_type: str = None,
        metadata: typing.Dict[str, str] = None,
        createdTime: datetime.datetime = None,
        accessedTime: datetime.datetime = None,
        digest: typing.Dict[HashingAlgorithm, str] = None,
        isLink: bool = None
        ):
        super().__init__(manager, path)

        self._size = size  # The size in bytes of the object
        self._content_type = content_type
        self._metadata = metadata
        self._createdTime = createdTime  # Time the artefact was physically created
        self._modifiedTime = modifiedTime  # Time the artefact was last modified via the os
        self._accessedTime = accessedTime  # Time the artefact was last accessed
        self._digest = digest or {}  # A signature for the file content
        self._isLink = isLink

    def __len__(self): return self.size
    def __repr__(self):
        return '<stow.File: {} modified({}) size({} bytes)>'.format(self._path, self._modifiedTime, self._size)

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
    def metadata(self):
        """ Get accessible file metadata as hosted by the manager """
        if self._metadata is None:
            self._metadata = self._manager._metadata(self._path)
        return self._metadata

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

    @property
    def content(self) -> bytes:
        """ file content as bytes """
        with self.open("rb") as handle:
            return handle.read()

    @content.setter
    def content(self, cont: bytes):
        if not isinstance(cont, bytes):
            raise ValueError("Cannot set the content of the file to non bytes type - {} given".format(type(cont)))

        with self.open("wb") as handle:
            handle.write(cont)

    @property
    def createdTime(self):
        """ UTC localised datetime of time file last modified by a write/append method """
        if self._createdTime is None:
            return self._modifiedTime
        return self._createdTime

    @property
    def modifiedTime(self):
        """ UTC localised datetime of time file last modified by a write/append method """
        return self._modifiedTime

    @modifiedTime.setter
    def modifiedTime(self, _datetime: typing.Union[float, datetime.datetime]):
        self._modifiedTime = self._manager.setmtime(self, _datetime)

    @property
    def accessedTime(self):
        """ UTC localised datetime of time file last modified by a write/append method """
        if self._accessedTime is None:
            return self._modifiedTime
        return self._accessedTime

    @accessedTime.setter
    def accessedTime(self, _datetime: typing.Union[float, datetime.datetime]):
        self._accessedTime = self._manager.setatime(self, _datetime)

    def digest(self, algorithm: HashingAlgorithm = HashingAlgorithm.MD5):
        """ Get the file digest to verify validaty - if a manager does not have it's own method of creatin file digests
        the md5 checksum will be used for the file contents.
        """
        if algorithm not in self._digest:
            self._digest[algorithm] = self._manager.digest(self, algorithm)

        return self._digest[algorithm]

    @property
    def size(self):
        """ Size of file content in bytes """
        return self._size

    def isLink(self):
        if self._isLink is None:
            self._isLink = self._manager._isLink(self)
        return self._isLink

    @contextlib.contextmanager
    def localise(self) -> str:
        """ Localise this File artefact

        Returns:
            str: the absolute local path to the manager path
        """
        with self.manager.localise(self) as abspath:
            yield abspath

    @contextlib.contextmanager
    def open(self, mode: str = 'r', **kwargs) -> io.IOBase:
        """ Context manager to allow the pulling down and opening of a file """
        with self._manager.open(self, mode, **kwargs) as handle:
            yield handle

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
        createdTime: datetime.datetime = None,
        modifiedTime: datetime.datetime = None,
        accessedTime: datetime.datetime = None,
        isMount: bool = None
        ):
        super().__init__(manager, path)

        self._createdTime = createdTime
        self._modifiedTime = modifiedTime
        self._accessedTime = accessedTime
        self._isMount = isMount

    def __len__(self): return len(self.ls())
    def __iter__(self): return iter(self._contents)
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
    def createdTime(self):
        """ UTC localised datetime of time file last modified by a write/append method """
        return self._createdTime

    @property
    def modifiedTime(self):
        """ UTC localised datetime of time file last modified by a write/append method """
        return self._modifiedTime

    @property
    def accessedTime(self):
        """ UTC localised datetime of time file last modified by a write/append method """
        return self._accessedTime

    def isMount(self):
        if self._isMount is None:
            self._isMount = self._manager._isMount(self)
        return self._isMount

    def mkdir(self, path: str):
        """ Create a directory nested inside this `Directory` with the relative path given

        Args:
            path: Relative path to directory, path to new directory location

        Returns:
            Directory: The newly created directory object
        """
        return self.manager.mkdir(self.manager.join(self._path, path, separator='/', joinAbsolutes=True))

    def touch(self, path: str) -> File:
        """ Touch a file at given location relative to this Directory

        Args:
            path: The relative path to directory to touch new file

        Returns:
            File: The newly created file object
        """
        return self.manager.touch(self.manager.join(self._path, path, separator='/', joinAbsolutes=True))

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
        return self.manager.relpath(path, self.path, separator=separator)

    @contextlib.contextmanager
    def localise(self, path: str = None) -> str:
        """ Localise an artefact this directory or a child artefact with the provided path.

        Args:
            path: Path of localisation

        Returns:
            str: the absolute local path to the manager path
        """
        with self.manager.localise(self if path is None else self.manager.join(self._path, path, separator='/')) as abspath:
            yield abspath

    @contextlib.contextmanager
    def open(self, path: str, mode: str = "r", **kwargs) -> io.IOBase:
        """ Open a file and create a stream to that file. Expose interface of `open`

        Args:
            path: Path to directory object
            mode: The open method
            kwargs: kwargs to be passed to the interface of open

        Yields:
            io.IOBase: An IO object depending on the mode for interacting with the file
        """
        with self.manager.open(
            self.manager.join(self._path, path, separator='/', joinAbsolutes=True),
            mode,
            **kwargs
            ) as handle:
            yield handle

    def rm(self, path: str = None, recursive: bool = False):
        """ Remove an artefact at the given location

        Args:
            artefact: Path that is to be deleted
            recursive: If the target is a directory, whether to delete recursively the directories contents

        Raises:
            OperationNotPermitted: In the even the target is a directory and recursive has not been toggled
        """
        return self.manager.rm(self.manager.join(self.path, path, separator='/', joinAbsolutes=True), recursive)

    def iterls(
        self,
        path: str = None,
        recursive: bool = False,
        *,
        ignore_missing: bool = False
        ) -> typing.Generator[Artefact, None, Artefact]:
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
            return self._manager.iterls(
                self if path is None else self.manager.join(self.path, path, separator='/'),
                recursive=recursive,
                ignore_missing=ignore_missing
            )
        except exceptions.ArtefactNotFound as e:
            raise exceptions.ArtefactNoLongerExists(f"Directory {self} no longer exists") from e

    def ls(
        self,
        path: str = None,
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
        return set(self.iterls(path, recursive=recursive, ignore_missing=ignore_missing))

    def isEmpty(self) -> bool:
        """ Check whether the directory has contents

        Returns:
            bool: True when there is at least one item in the directory False when the directory is empty
        """
        for _ in self._manager.iterls(self._path):
            return False
        else:
            return True

    def empty(self):
        """ Empty the directory of contents """
        for artefact in self.ls():
            self._manager.rm(artefact, recursive=True)

class PartialArtefact:

    def __init__(self, manager, path: str):
        self._manager = manager
        self._path = path

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

ArtefactType = typing.Union[File, Directory]
