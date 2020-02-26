import os
from abc import ABC, abstractmethod
import typing
import shutil
import tempfile
import contextlib
import hashlib
import re

from . import SEP
from . import exceptions
from .artefacts import Artefact, File, Directory


class Manager(ABC):
    """ Manager Abstract base class - expressed the interface of a Manager which governs a storage option and allows
    extraction and placement of files in that storage container

    Params:
        name (str): A human readable name for the storage option
    """

    _ROOT_PATH = "/"
    _PLACEHOLDER = "placeholder.ignore"
    _READONLYMODES = ["r", "rb"]

    _RELPATH_REGEX = re.compile(r"^([a-zA-Z0-9]+:)?([/\\\w\.!-_*'()]*)$")

    def __init__(self):
        self._root = Directory(self, self._ROOT_PATH)
        self._paths = {self._ROOT_PATH: self._root}
        self.refresh()

    def __getitem__(self, item): return self._paths[item]
    def __contains__(self, item):
        if isinstance(item, Artefact): return item.manager is self
        return item in self._paths

    @abstractmethod
    def __repr__(self): pass

    def paths(self, classtype = None):
        if classtype is None: return self._paths.copy()
        else: return {path: artefact for path, artefact in self._paths.items() if isinstance(artefact, classtype)}

    @abstractmethod
    def _get(self, source: str, destination: str):
        pass

    def get(self, src_remote: typing.Union[Artefact, str], dest_local: str) -> Artefact:
        """ Get a remote artefact from the storage option and write it to the destination path given.

        Params:
            src_remote (Artefact/str): The remote's file object or its path
            dest_local (str): The local path for the artefact to be written to
        """

        obj, path = self._artefactFormStandardise(src_remote)

        # Identify the path to be loaded
        if obj is None or obj.manager is not self:
            raise exceptions.ArtefactNotMember("Provided artefact is not a member of the manager")

        return self._get(obj, dest_local)

    @abstractmethod
    def _put(self, source, destination):
        pass

    def __putArtefact(self, src_local, dest_remote):

        # Get the owning directory of the item - Ensure that the directories exist for the incoming files
        owner = self._backfillHierarchy(self._dirname(dest_remote))

        # Clean up any files that current exist at the location
        destObj, destPath = self._artefactFormStandardise(dest_remote)
        if destObj is not None: self._rm(destObj, destPath)

        # Put the local file onto the remote using the manager definition
        self._put(src_local, self._abspath(destPath))

        # Extract the artefact depending on the type
        if os.path.isdir(src_local):
            art = self.refresh(dest_remote)

        else:
            art = self._makefile(dest_remote)

            if dest_remote in self._paths:
                # The artefact has overwritten a previous file - update it and return it
                original = self._paths[dest_remote]
                original._update(art)
                return original

        # Save the new artefact
        owner._add(art)
        self._paths[dest_remote] = art
        return art

    def put(self, src_local: str, dest_remote: typing.Union[Artefact, str]) -> (str, str):
        """ Put a local artefact onto the remote at the location given.

        Params:
            src_local (str): The path to the local artefact that is to be put on the remote
            dest_remote (Artefact/str): A file object to overwrite or the relative path to a destination on the
                remote
        """

        srcObj, srcPath = self._artefactFormStandardise(src_local)
        destObj, destPath = self._artefactFormStandardise(dest_remote)

        if destObj is not None and destObj.manager is not self:
                raise exceptions.ArtefactNotMember("Destination artefact is not a member of the manager")

        if isinstance(srcObj, Artefact):
            # Provides was an artefact from a manager that may be remote

            with srcObj.manager.localise(srcObj) as srcAbsPath:
                return self.__putArtefact(srcAbsPath, destPath)

        else:
            # The source is a local filepath
            return self.__putArtefact(srcPath, destPath)

    def ls(self, path: str = '/', recursive: bool = False):

        # Get from the manager store the object for this path - If failed to collect raise membership error
        art = self._paths.get(path)
        if art is None: raise exceptions.ArtefactNotFound("No directory found at location: {}".format(path))

        # Return the contents of the artefact - if not a container artefact raise error
        if isinstance(art, Directory): return art.ls(recursive)
        raise TypeError("None directory artefact found at location")

    @abstractmethod
    def _mv(self, srcObj: Artefact, destPath: str):
        pass

    def mv(self, source: typing.Union[Artefact, str], destination: typing.Union[Artefact, str]):

        # Understand the objects being moved
        srcObj, srcPath = self._artefactFormStandardise(source)
        destObj, destPath = self._artefactFormStandardise(destination)

        # Destination content is being overwritten
        if destObj: self.rm(destObj, recursive=True)

        # Call the lower level, content move function on the manager
        self._mv(srcObj, destPath)

        if isinstance(srcObj, Directory):

            for art in srcObj.ls(recursive=True):

                path = art.path

                # Update the object with it's new path
                art._path = self._join(destPath, art.path[len(srcPath):])

                # Update its membership
                del self._paths[path]
                self._paths[art.path] = art

        # Unconnect object with the directories that it exists in and add it to the destination location
        self[self._dirname(srcPath)]._remove(srcObj)
        self._backfillHierarchy(self._dirname(destPath))._add(srcObj)

        # Move the file info across
        del self._paths[srcPath]
        self._paths[destPath] = srcObj
        srcObj._path = destPath

    @abstractmethod
    def _rm(self, artefact: Artefact, artefactPath: str):
        pass

    def rm(self, artefact: typing.Union[Artefact, str], recursive: bool = False) -> None:

        obj, path = self._artefactFormStandardise(artefact)

        if obj is None or obj.manager is not self:
            raise exceptions.ArtefactNotMember("Artefact ({}) is not a member of the manager".format(artefact))

        if isinstance(obj, Directory) and len(obj) and not recursive:
            raise exceptions.OperationNotPermitted(
                "Cannot delete a container object that isn't empty - set recursive to True to proceed"
            )

        # Trigger the removal of the underlying object
        self._rm(obj, path)

        # Inform all objects that they no longer exist
        if isinstance(obj, Directory):
            for art in obj.ls(recursive=True):
                del self._paths[art.path]
                art._exists = False

        obj._exists = False

        self[self._dirname(obj.path)]._remove(obj)
        del self._paths[obj.path]

    def mkdir(self, path: str):
        with tempfile.TemporaryDirectory() as directory:
            return self.put(directory, path)

    def touch(self, path: str) -> Artefact:

        with tempfile.TemporaryDirectory() as directory:
            emptyFile = os.path.join(directory, 'empty_file')
            open(emptyFile, 'w').close()
            return self.put(emptyFile, path)

    @abstractmethod
    def _abspath(self, artefact:  typing.Union[Artefact, str]) -> str:
        """ Return the most accurate path to an object in the managers vernacular. Opposite of _relpath

        examples:
            local managers shall convert a relative path to its full absolute os compatible filepath
            s3 shall convert the relative path to a s3 valid key

        Params:
            artefact (Artefact/str): The artefact object or it's relative path which is to be converted
        """
        pass

    @classmethod
    def _relpath(cls, abspath: str) -> str:
        """ Converts any path into a manager agnostic path format (/dir/file.txt), opposite of _abspath

        Params:
            abspath (str): The artefact object or it's absolute path which is to be converted
        """
        match = cls._RELPATH_REGEX.match(abspath)
        if match is None: raise exceptions.InvalidPath("Path not in acceptable form: {}".format(abspath))
        abspath = '/' + re.sub(r"[\\/]{2,}|[\\]", "/", match.group(2)).strip('/')
        return abspath

    @abstractmethod
    def _dirname(self, path):
        pass

    @abstractmethod
    def _basename(self, path):
        pass

    def _join(self, *components) -> str:
        """ Join a relative path with another path for sub and return a manager relative path
        """
        return self._relpath("/".join(components))

    @abstractmethod
    def _makefile(self, path):
        """ Make a file object from a manager relative path """
        pass

    @abstractmethod
    def _walkOrigin(self, prefix):
        """ Walk the origin and return the paths to files - empty directories are identified by having an emplty
        placeholder file present
        """
        pass

    def _backfillHierarchy(self, path):
        """ Fetch the owning `container` for the manager relative path given. In the event that no `container` object
        exists for the path, create one and recursively find its owning `container` to add it to. The goal of this
        function is to traverse up the hierarchy and ensure all the directory objects exist, and when they do quickly
        return the container they are in

        Params:
            path (str): The manager relative path for an `Artefact`

        Returns:
            Directory: The owning directory container, which may have just been created
        """

        if path in self._paths:
            # The path points to an already established directory
            directory = self._paths[path]
            if isinstance(directory, File): raise ValueError("Invalid path given. Path points to a file.")
            return directory

        # Create the directory at this location
        art = Directory(self, path)

        # Find the owning directory for this newly created directory, and add it to its contents
        self._backfillHierarchy(self._dirname(path))._add(art)

        # Store the newly created directory in the manager and return
        self._paths[path] = art
        return art

    def _artefactFormStandardise(self, object):
        if isinstance(object, Artefact):
            return object, object.path
        else:
            return self[object] if object in self else None, object

    def refresh(self, prefix=None):

        # Get the objects that appear under the location that has been provided
        if prefix is None:
            unsupportedObjects = self._root.ls(recursive=True)
        elif prefix in self._paths:
            unsupportedObjects = self._paths[prefix].ls(recursive=True)
        else:
            unsupportedObjects = set()

        # Combine the prefix and the intenal path such that it's valid
        for path in self._walkOrigin(prefix):

            found = False
            if path in self._paths:
                # Object already exists inside the manager and as a result has it's hierarchy defined - update it

                storedObject = self._paths[path]
                storedObject._update(self._makefile(path))
                unsupportedObjects.discard(storedObject)

                found = True

            # Find the owning directory for the file and ensure that it is supported
            owning_directory = self._backfillHierarchy(self._dirname(path))
            if owning_directory in unsupportedObjects:
                # The directory wasn't supported before this point - ensure that it was and that all upper directories
                # are supported also
                owner = owning_directory
                while owner in unsupportedObjects:
                    unsupportedObjects.remove(owner)
                    owner = self._backfillHierarchy(self._dirname(owner.path))

            # The owning directory has been created and supported continue if placeholder file or found previously
            if found or self._basename(path) == self._PLACEHOLDER: continue

            # Create the file and to the store
            file = self._makefile(path)
            owning_directory._add(file)
            self._paths[path] = file

        # Remove unsupported objects
        for art in unsupportedObjects:
            self.rm(art, recursive=True)

        return self._root if prefix is None else self._paths[prefix]

    @contextlib.contextmanager
    @abstractmethod
    def localise(self, artefact: typing.Union[Artefact, str]):
        pass

    @staticmethod
    def md5(path):
        hash_md5 = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)

        return hash_md5.hexdigest()

    @contextlib.contextmanager
    def open(self, artefact, mode, **kwargs):

        art, path = self._artefactFormStandardise(artefact)

        if art is None:
            if mode in self._READONLYMODES:
                raise FileNotFoundError('File does not exist in the manager')

            else:
                # Create the file for touching
                self.touch(path)

        with self.localise(artefact) as abspath:
            with open(abspath, mode, **kwargs) as handle:
                yield handle

class LocalManager(Manager, ABC):

    @contextlib.contextmanager
    def localise(self, artefact):
        object, path = self._artefactFormStandardise(artefact)
        yield self._abspath(path)
        object._update(self._makefile(path))

class RemoteManager(Manager, ABC):

    @contextlib.contextmanager
    def localise(self, artefact):
        object, path = self._artefactFormStandardise(artefact)

        with tempfile.TemporaryDirectory() as directory:

            # Generate a temporay path for the file to be downloaded into
            local_path = os.path.join(directory, self._basename(path))

            # Get the contents and put it into the temporay directory
            self.get(path, local_path)

            # Generate a checksum for the file
            checksum = self.md5(local_path)

            # Return the local path to the object
            yield local_path

            # The user has stopped interacting with the file - upload if the file has been editted
            if self.md5(local_path) != checksum:
                # The file has been changed - upload the file's contents
                self.put(local_path, path)

                # update the objects information with the information from that file
                object._update(self._makefile(path))
