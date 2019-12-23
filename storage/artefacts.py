import os
import io
import datetime
import tempfile
import hashlib
import contextlib

from . import sep
from .interfaces import Artefact, Container, Manager

class File(Artefact):
    """ File stuff """

    @staticmethod
    def md5(path):
        hash_md5 = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)

        return hash_md5.hexdigest()

    def __init__(self, container: Manager, remote_path: str, modified_date: datetime.datetime, size: float):
        super().__init__(container, remote_path)

        self._modified_date = modified_date
        self._size = size

    def __repr__(self):
        return '<storage.File: name({}) - modified({}) - size({})>'.format(self._path, self._modified_date, self._size)

    def __len__(self): return self._size

    def __hash__(self):
        return hash(id(self))

    def __eq__(self, other):
        return self is other

    @property
    def modifiedTime(self): return self._modified_date

    @property
    def size(self): return self._size

    @contextlib.contextmanager
    def _download(self) -> str:
        """ Download the file to a location and return its path. Clean up the file after use """

        try:
            filepath = tempfile.mkstemp()
            self._container.get(self, filepath)
            yield filepath
        except:
            raise RuntimeError("Was unable to retrieve file")
        finally:
            os.remove(filepath)

    @contextlib.contextmanager
    def open(self, encoding: str = 'r') -> io.TextIOWrapper:
        """ Context manager to allow the pulling down and opening of a file """

        with tempfile.TemporaryDirectory() as directory:

            # Generate a temporay path for the file to be downloaded into
            path = os.path.join(directory, 'tempfile')

            # Get the contents and put it into the temporay directory
            self._container.get(self, path)

            # Generate a checksum for the file
            checksum = self.md5(path)

            # Open a connection to this local file and return the handle for the user to interact with it
            with open(path, encoding) as fh:
                yield fh

            # The user has stopped interacting with the file - upload if the file has been editted
            if self.md5(path) != checksum:
                # The file has been changed - upload the file's contents
                self._container.put(path, self)

                stats = os.stat(path)
                self._modified_date = datetime.datetime.now()
                self._size = stats.st_size

    def _update(self, modifiedTime, size):
        self._modified_date = modifiedTime
        self._size = size

class Directory(Artefact, Container):
    """ A directory represents an os FS directory """

    def __init__(self, container: Manager, remote_path: str, contents: set = None):
        super().__init__(container, remote_path)

        self._contents = set(contents) if contents else set()

    def __len__(self): return len(self._contents)

    def add(self, artefact: Artefact) -> None:
        self._contents.add(artefact)

    def remove(self, artefact: Artefact) -> None:
        self._contents.remove(artefact)

    def mkdir(self, path: str):
        self._container.mkdir(os.path.join(self._path, path.strip(sep)))

    def touch(self, path):
        self._container.touch(os.path.join(self._path, path.strip(sep)))

    def ls(self, recursive: bool = False):
        return self._contents.copy()