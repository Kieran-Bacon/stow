import abc
import io
import datetime
import contextlib
import typing
import weakref

from . import exceptions

class Artefact:
    """ Artefacts are the items that are being stored - it is possible that through another mechanism that these items
    are deleted and they are no longer able to work

    Args:
        manager: The submanager this file belongs to
        path: The file's relative path
    """

    def __init__(
        self,
        manager,
        path: str
        ):

        self._manager = manager  # Link back to the owning manager
        self._path = path  # Relative path on manager
        self._exists = True # As you are created you are assumed to exist

    def __getattribute__(self, attr):

        if object.__getattribute__(self, '_exists'):
            return object.__getattribute__(self, attr)
        else:
            raise exceptions.ArtefactNoLongerExists(
                "Artefact {} {} no longer exists".format(type(self), object.__getattribute__(self, "_path"))
            )

    def __hash__(self): return hash(id(self))
    def __eq__(self, other): return hash(self) == hash(other)

    def __fspath__(self):
        with self.localise() as path:
            return path

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

    @property
    def basename(self):
        """ Basename of the artefact - holding directory path removed leaving filename and extension """
        return self._manager.basename(self.path)
    @basename.setter
    def basename(self, basename: str):
        self.manager.mv(self, self._manager.join(self._manager.dirname(self.path), basename, separator='/'))

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
        createdTime: datetime.datetime = None,
        accessedTime: datetime.datetime = None
        ):
        super().__init__(manager, path)

        self._size = size  # The size in bytes of the object
        self._createdTime = createdTime  # Time the artefact was physically created
        self._modifiedTime = modifiedTime  # Time the artefact was last modified via the os
        self._accessedTime = accessedTime  # Time the artefact was last accessed

    def __len__(self): return self._size
    def __repr__(self):
        return '<stow.File: {} modified({}) size({} bytes)>'.format(self._path, self._modifiedTime, self._size)

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
    def modifiedTime(self):
        """ UTC localised datetime of time file last modified by a write/append method """
        return self._modifiedTime

    @property
    def createdTime(self):
        """ UTC localised datetime of time file last modified by a write/append method """
        if self._createdTime is None:
            return self._modifiedTime
        return self._createdTime

    @property
    def accessedTime(self):
        """ UTC localised datetime of time file last modified by a write/append method """
        if self._accessedTime is None:
            return self._modifiedTime
        return self._accessedTime

    @property
    def size(self):
        """ Size of file content in bytes """
        return self._size

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

    def _update(self, other: Artefact):
        self._createdTime = other._createdTime
        self._modifiedTime = other._modifiedTime
        self._accessedTime = other._accessedTime
        self._size = other._size

class SubFile(File):
    """ A file object of a submanager. Wrapper for a complete Manager File

    Args:
        manager: The submanager this file belongs to
        path: The file's relative path
        file: The concrete file object this subfile wraps
    """

    def __init__(self, manager, path: str, file: File):
        super(File, self).__init__(manager, path)
        self._concrete = file

    def __len__(self): return len(self._concrete)
    def __repr__(self): return '<stow.SubFile for {}>'.format(self._concrete)

    @property
    def _modifiedTime(self): return self._concrete._modifiedTime
    @_modifiedTime.setter
    def _modifiedTime(self, time): self._concrete._modifiedTime = time

    @property
    def _createdTime(self): return self._concrete._createdTime
    @_createdTime.setter
    def _createdTime(self, time): self._concrete._createdTime = time

    @property
    def _accessedTime(self): return self._concrete._accessedTime
    @_accessedTime.setter
    def _accessedTime(self, time): self._concrete._accessedTime = time

    @property
    def content(self) -> bytes: return self._concrete.content
    @content.setter
    def content(self, content: bytes): self._concrete.content = content

    @property
    def _size(self): return self._concrete.size
    @_size.setter
    def _size(self, newSize): self._concrete._size = newSize

    @contextlib.contextmanager
    def open(self, mode: str = 'r', **kwargs) -> io.TextIOWrapper:
        """ Context manager to allow the pulling down and opening of a file """
        with self._concrete.open(mode, **kwargs) as handle:
            yield handle

    # def _update(self, other: Artefact): self._concrete._update(other)

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
        accessedTime: datetime.datetime = None
        ):
        super().__init__(manager, path)
        self._contents = weakref.WeakSet()
        self._collected = False

        self._createdTime = createdTime
        self._modifiedTime = modifiedTime
        self._accessedTime = accessedTime

    def __len__(self): return len(self.ls())
    def __iter__(self): return iter(self._contents)
    def __repr__(self): return '<stow.Directory: {}>'.format(self._path)
    def __contains__(self, artefact: typing.Union[Artefact, str]) -> bool:
        if isinstance(artefact, Artefact):
            return artefact.manager is self.manager and artefact in self._contents
        else:
            return self.manager.join(self._path, artefact, separator='/') in self.manager

    def _add(self, artefact: Artefact) -> None:
        assert isinstance(artefact, (File, Directory)) and not isinstance(artefact, (SubDirectory))
        self._contents.add(artefact)
    def _remove(self, artefact: Artefact) -> None: self._contents.remove(artefact)

    @property
    def createdTime(self):
        """ UTC localised datetime of time file last modified by a write/append method """
        if self._createdTime is None:
            if self._contents:
                return min(x.createdTime for x in self._contents)
            return None
        return self._createdTime

    @property
    def modifiedTime(self):
        """ UTC localised datetime of time file last modified by a write/append method """
        if self._modifiedTime is None:
            if self._contents:
                return max(x.modifiedTime for x in self._contents)
            return None
        return self._modifiedTime

    @property
    def accessedTime(self):
        """ UTC localised datetime of time file last modified by a write/append method """
        if self._accessedTime is None:
            if self._contents:
                return max(x.accessedTime for x in self._contents)
            return None
        return self._accessedTime

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

    def relpath(self, artefact: typing.Union[Artefact, str]) -> str:
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

        # Raise error if the artefact is not a member of the directory
        if not path.startswith(self.path):
            raise exceptions.ArtefactNotMember(
                "Cannot create relative path for Artefact {} as its not a member of {}".format(artefact, self)
            )

        # Return the path
        return self.manager.relpath(path, self.path)

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
        with self.manager.open(self.manager.join(self._path, self.manager._managerPath(path)[1:], separator='/'), mode, **kwargs) as handle:
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

    def _ls(self, recursive: bool = False):
        """ Get the current contents from this directory, do not update or edit state """

        if recursive:
            # Iterate through contents and recursively add lower level artifacts
            contents = set()
            for art in self._contents:
                if isinstance(art, Directory):
                    contents |= art._ls(True)

                contents.add(art)

            # Return all child content
            return contents

        # Make the content a set and return just this level
        return set(self._contents)

    def ls(self, path: str = None, recursive: bool = False) -> typing.Set[Artefact]:
        """ List the contents of this directory, or directory's directories.

        Args:
            path: The path to sub directory whose contents is to be returned
            recursive: Whether to recursively fetch all child contents for child directories

        Returns:
            typing.Set[Artefact]: The collection of objects within the targeted directory
        """
        return self._manager.ls(self.path if not path else self.manager.join(self.path, path, separator='/'), recursive=recursive)

    def isEmpty(self) -> bool:
        """ Check whether the directory has contents

        Returns:
            bool: True when there is at least one item in the directory False when the directory is empty
        """
        return not (bool(self._contents) or bool(len(self)))

    def _update(self, other: 'Directory'):

        self._contents = self._contents.union(other._contents)
        self._createdTime = other._createdTime
        self._modifiedTime = other._modifiedTime
        self._accessedTime = other._accessedTime
        self._collected = False

class SubDirectory(Directory):
    """ A directory object of a submanager. Wrapper for a complete Manager directory """

    def __init__(self, manager, path: str, directory: Directory):
        super(Directory, self).__init__(manager, path)  # Access the parent of Directory
        self._contents = weakref.WeakSet()
        self._collected = False
        self._concrete = directory

    @property
    def _collected(self): return self._concrete._collected
    @_collected.setter
    def _collected(self, value): pass


    def __len__(self): return len(self._concrete)
    def __repr__(self): return '<stow.SubDirectory for {}>'.format(self._concrete)
    def _add(self, artefact: Artefact) -> None:
        assert isinstance(artefact, (SubFile, SubDirectory))
        self._contents.add(artefact)

    @property
    def _modifiedTime(self): return self._concrete._modifiedTime

    @property
    def _createdTime(self): return self._concrete._createdTime

    @property
    def _accessedTime(self): return self._concrete._accessedTime

    def _update(self, other: Artefact):
        self._concrete._update(other._concrete)