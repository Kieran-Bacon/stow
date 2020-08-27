import os
import io
import datetime
import tempfile
import contextlib
import typing
import weakref

from . import exceptions

class Artefact:
    """ Aretefacts are the items that are being stored - it is possible that through another mechanism that these items
    are deleted and they are no longer able to work
    """

    def __init__(self, manager, path: str):
        self._manager = manager
        self._path = path
        self._exists = True # As you are created you are assumed to exist

    def __getattr__(self, attr):
        if self.__getattribute__('_exists'):
            return self.__getattribute__(attr)
        else:
            raise FileNotFoundError(f"{self} no longer exists")

    def __hash__(self): return hash(id(self))
    def __eq__(self, other): return hash(self) == hash(other)

    @property
    def manager(self): return self._manager

    @property
    def path(self): return self._path
    @path.setter
    def path(self, path: str):
        """ Move the file on the target (perform the rename) - if it fails do not change the local file name """
        self._manager.mv(self, path)

    @property
    def basename(self):
        return self._manager.basename(self.path)

    @basename.setter
    def basename(self, basename: str):
        """ Rename the object """
        self.manager.mv(self, self._manager.join(self._manager.dirname(self.path), basename))

    @property
    def name(self):
        """ Get the name of the object """
        return self.basename

    @name.setter
    def name(self, name: str):
        self.basename = name

    def save(self, path: str):
        self._manager.get(self, path)

class File(Artefact):
    """ File stuff """

    def __init__(self, manager, path: str, modifiedTime: datetime.datetime, size: float):
        super().__init__(manager, path)

        self._modifiedTime = modifiedTime
        self._size = size

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
        if "." not in self.path:
            return ""
        return self.path[self.path.rindex(".")+1:]
    @extension.setter
    def extension(self, ext: str):
        self.basename = ".".join([self.name, ext])

    @property
    def content(self) -> bytes:
        with self.open("rb") as handle:
            return handle.read()

    @content.setter
    def content(self, cont: bytes):
        if not isinstance(cont, bytes):
            raise ValueError("Cannot set the content of the file to non bytes type - {} given".format(type(cont)))

        with self.open("wb") as handle:
            handle.write(cont)

    @property
    def modifiedTime(self): return self._modifiedTime
    @modifiedTime.setter
    def modifiedTime(self, time):
        self._modifiedTime = time

    @property
    def size(self): return self._size
    @size.setter
    def size(self, newSize):
        self._size = newSize

    @contextlib.contextmanager
    def open(self, mode: str = 'r', **kwargs) -> io.TextIOWrapper:
        """ Context manager to allow the pulling down and opening of a file """
        with self._manager.open(self, mode, **kwargs) as handle:
            yield handle

    def _update(self, other: Artefact):
        self._modifiedTime = other.modifiedTime
        self._size = other.size

class SubFile(File):

    def __init__(self, manager, path: str, file: File):
        Artefact.__init__(self, manager, path)
        self._concrete = file

    def __len__(self): return len(self._concrete)
    def __repr__(self): return '<stow.SubFile for {}>'.format(self._concrete)

    @property
    def content(self) -> bytes: return self._concrete.content

    @property
    def modifiedTime(self): return self._concrete.modifiedTime
    @modifiedTime.setter
    def modifiedTime(self, time): self._concrete.modifiedTime = time

    @property
    def size(self): return self._concrete.size
    @size.setter
    def size(self, newSize): self._concrete.size = newSize

    @contextlib.contextmanager
    def open(self, mode: str = 'r', **kwargs) -> io.TextIOWrapper:
        """ Context manager to allow the pulling down and opening of a file """
        with self._concrete.open(mode, **kwargs) as handle:
            yield handle

    def _update(self, other: Artefact): self._concrete._update(other)

class Directory(Artefact):
    """ A directory represents an os FS directory

    Params:
        manager (stow.Manager): The manager this directory object belongs to
        path (str): the manager relative path for the object
        contents (set): collection of artefacts which reside within this directoy
        *,
        collected (bool): Toggle as to whether the directory contents has been collected (false when JIT Loading)
    """

    def __init__(self, manager, path: str):
        super().__init__(manager, path)
        self._contents = weakref.WeakSet()
        self._collected = False

    def __len__(self): return len(self.ls())
    def __iter__(self): return iter(self._contents)
    def __repr__(self): return '<stow.Directory: {}>'.format(self._path)
    def __contains__(self, artefact: typing.Union[Artefact, str]) -> bool:
        if isinstance(artefact, Artefact):
            return artefact.manager is self.manager and artefact in self._contents
        else:
            return self.manager.join(self._path, artefact) in self.manager

    def _add(self, artefact: Artefact) -> None:
        assert isinstance(artefact, (File, Directory)) and not isinstance(artefact, (SubDirectory))
        self._contents.add(artefact)
    def _remove(self, artefact: Artefact) -> None: self._contents.remove(artefact)

    def mkdir(self, path: str): self.manager.mkdir(self.manager.join(self._path, path))
    def touch(self, path: str): self.manager.touch(self.manager.join(self._path, path))

    @contextlib.contextmanager
    def localise(self, path: str):
        with self.manager.localise(self.manager.join(self._path, path)) as abspath:
            yield abspath

    @contextlib.contextmanager
    def open(self, path: str, mode: str = "r", **kwargs):
        with self.manager.open(self.manager.join(self._path, path), mode, **kwargs) as handle:
            yield handle

    def rm(self, path, recursive: bool = False): return self.manager.rm(self.manager.join(self.path, path), recursive)
    def ls(self, recursive: bool = False): return self._manager.ls(self, recursive=recursive)
    def isEmpty(self) -> bool:
        """ Check whether the directory is contents or not - doesn't perform a lookup for all files in the event that
        files have already been identified

        Returns:
            bool: True when there is at least one item in the directory False when the directory is empty
        """
        return not (bool(self._contents) or bool(len(self)))

class SubDirectory(Directory):
    """ Create a directory """

    def __init__(self, manager, path: str, directory: Directory):
        super().__init__(manager, path)
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