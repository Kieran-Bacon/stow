import os
import io
import datetime
import tempfile
import contextlib

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

    def save(self, path: str):
        self._manager.get(self, path)

class File(Artefact):
    """ File stuff """

    def __init__(self, manager, path: str, modified_date: datetime.datetime, size: float):
        super().__init__(manager, path)

        self._modified_date = modified_date
        self._size = size

    def __len__(self): return self._size
    def __repr__(self):
        return '<storage.File: name({}) - modified({}) - size({})>'.format(self._path, self._modified_date, self._size)

    @property
    def modifiedTime(self): return self._modified_date
    @modifiedTime.setter
    def modifiedTime(self, time):
        self._modified_date = time

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
        assert self.manager == other.manager and self.path == other.path
        self._modified_date = other._modified_date
        self._size = other._size

class Directory(Artefact):
    """ A directory represents an os FS directory """

    def __init__(self, manager, path: str, contents: set = None):
        super().__init__(manager, path)
        self._contents = set(contents) if contents else set()

    def __len__(self): return len(self._contents)
    def __iter__(self): return iter(self._contents)
    def __repr__(self): return '<storage.Directory({})>'.format(self._path)
    def _add(self, artefact: Artefact) -> None: self._contents.add(artefact)
    def _remove(self, artefact: Artefact) -> None: self._contents.remove(artefact)

    def mkdir(self, path: str): self._container.mkdir(self.manager._join(self._path, path))
    def touch(self, path: str): self._container.touch(self.manager._join(self._path, path))
    def rm(self, path, recursive: bool = False): return self.manager.rm(self.manager._join(self.path, path), recursive)

    def ls(self, recursive: bool = False):

        if recursive:

            # Iterate through contents and recursively add lower level artifacts
            contents = set()
            for art in self._contents:
                if isinstance(art, Directory):
                    contents = contents.union(art.ls(recursive))

                contents.add(art)

            # Return all child content
            return contents

        return self._contents.copy()


