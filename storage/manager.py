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

    _RELPATH_REGEX = re.compile(r"^([a-zA-Z0-9]+:)?([/\\\w\.!-_*'() ]*)$")

    def __init__(self):
        self._root = Directory(self, self._ROOT_PATH)
        self._paths = {self._ROOT_PATH: self._root}

    @abstractmethod
    def _isdir(self, relpath: str) -> bool:
        """ Given a relative path, return a bool which is true if the target item is a directory

        Params:
            relpath (str): relative path to the item

        Returns:
            bool: True if relative path leads to a directory, false if item is a file

        Raises:
            ArtefactNotFound: In the event that there is no target of the relative path provided
        """
        pass

    def __getitem__(self, relpath: str):

        # Item found return the item
        if relpath in self._paths: return self._paths[relpath]

        # Attempt to find the artefact - instantiating it when found
        isDir = self._isdir(relpath)
        if isDir:
            art = self._backfillHierarchy(relpath)
        else:
            art = self._makefile(relpath)

        self._add(art)
        return art

    def __contains__(self, item):
        if isinstance(item, Artefact): return item.manager is self
        return item in self._paths

    @abstractmethod
    def __repr__(self): pass

    @abstractmethod
    def abspath(self, artefact:  typing.Union[Artefact, str]) -> str:
        """ Return the most accurate path to an object in the managers vernacular. Opposite of relpath

        examples:
            local managers shall convert a relative path to its full absolute os compatible filepath
            s3 shall convert the relative path to a s3 valid key

        Params:
            artefact (Artefact/str): The artefact object or it's relative path which is to be converted
        """
        pass

    @classmethod
    def relpath(cls, abspath: str) -> str:
        """ Converts any path into a manager agnostic path format (/dir/file.txt), opposite of abspath

        Params:
            abspath (str): The artefact object or it's absolute path which is to be converted
        """
        match = cls._RELPATH_REGEX.match(abspath)
        if match is None: raise exceptions.InvalidPath("Path not in acceptable form: {}".format(abspath))
        abspath = '/' + re.sub(r"[\\/]{2,}|[\\]", "/", match.group(2)).strip('/')
        return abspath

    @abstractmethod
    def dirname(self, path):
        pass

    @abstractmethod
    def basename(self, path):
        pass

    def join(self, *components) -> str:
        """ Join a relative path with another path for sub and return a manager relative path
        """
        return self.relpath("/".join(components))

    def _add(self, art: Artefact, *, owner: Directory = None):
        """ Add an artefact object into the manager data structures

        Parmas:
            art (Artefact): The artefact object to be added
            *,
            owner (Directory) = None: The directory object that the art is to be added to.
                Can be passed to save from looking it up again if the directory is already known
        """
        assert art.manager is self
        if owner is None: owner = self._backfillHierarchy(self.dirname(art.path))
        owner._add(art)
        self._paths[art.path] = art

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

        obj, _ = self._artefactFormStandardise(src_remote)

        # Identify the path to be loaded
        if obj is None or obj.manager is not self:
            raise exceptions.ArtefactNotMember("Provided artefact is not a member of the manager")

        return self._get(obj, dest_local)

    @abstractmethod
    def _put(self, source, destination):
        pass

    def __putArtefact(self, src_local, dest_remote):

        # Clean up any files that current exist at the location
        destObj, destPath = self._artefactFormStandardise(dest_remote)
        if destObj is not None: self._rm(destObj, destPath)

        # Put the local file onto the remote using the manager definition
        self._put(src_local, self.abspath(destPath))

        # Extract the artefact depending on the type of input
        if os.path.isdir(src_local):
            # Source is a directory

            if destObj is not None:
                # An object original existed - identify type of object and handle accordingly

                if isinstance(destObj, Directory):
                    # The original object was a directory - compare downloaded objects with objects to remove no longer
                    # present files and update files to the newly uploaded content
                    self._refresh(destObj)
                    return destObj

                else:
                    # File is being replaced with a directory - delete the file and create a new directory object
                    self._remove(destObj)

            return self._backfillHierarchy(dest_remote)

        else:
            # Source is a file
            art = self._makefile(dest_remote)

            if destObj is not None:
                if isinstance(destObj, File):
                    # The artefact has overwritten a previous file - update it and return it
                    original = self._paths[dest_remote]
                    original._update(art)
                    return original

                else:
                    self._remove(destObj)

            # Get the owning directory of the item - Ensure that the directories exist for the incoming files
            owner = self._backfillHierarchy(self.dirname(dest_remote))
            owner._add(art)
            self._add(art)
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

    @abstractmethod
    def _listdir(self, relpath: str) -> typing.Tuple[typing.Set[str], typing.Set[str]]:
        """ List the underlying objects that are present at the location of the relpath.

        Params:
            relpath (str): Relative path to the directory to be checked

        Returns:
            typing.Set[str]: A set of directory paths found within the relative path
            typing.Set[str]: A set of file paths found within the relative path
        """
        pass

    def _ls(self, artobj: Directory, recursive = True):

        if recursive:

            # Iterate through contents and recursively add lower level artifacts
            contents = set()
            for art in artobj._contents:
                if isinstance(art, Directory): contents |= self._ls(art, recursive)
                contents.add(art)

            # Return all child content
            return contents

        return artobj._contents.copy()

    def ls(self, art: typing.Union[Directory, str] = '/', recursive: bool = False):

        # Convert the incoming artefact reference - requre that the object exist and that it is a directory
        artobj, path = self._artefactFormStandardise(art, require=True)
        if not isinstance(artobj, Directory): raise TypeError("Cannot perform ls action on File artefact: {}".format(artobj))

        # Perform JIT download of directory contents
        if not artobj._collected:

            # Identify what has already been downloaded into the directory object
            ddirs, dfiles = set(), set()
            for art in artobj._contents:
                if isinstance(art, Directory): ddirs.add(art.path)
                else: dfiles.add(art.path)

            # For all the other objects that have not yet been downloaded for the object - download them
            dirs, files = self._listdir(path)
            for directory in dirs.difference(ddirs): self._add(self._backfillHierarchy(directory))
            for file in files.difference(dfiles): self._add(self._makefile(file))

            # Signal that the directory contents has been downloaded NOTE not recursive information
            artobj._collected = True

        if recursive:

            # Iterate through contents and recursively add lower level artifacts
            contents = set()
            for art in artobj._contents:
                if isinstance(art, Directory): contents |= self.ls(art, recursive)
                contents.add(art)

            # Return all child content
            return contents

        return artobj._contents.copy()

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

            for art in self._ls(srcObj, recursive=True):
                # Get the downloaded content for from the directory object

                path = art.path

                # Update the object with it's new path
                art._path = self.join(destPath, art.path[len(srcPath):])

                # Update its membership
                del self._paths[path]
                self._paths[art.path] = art

        # Unconnect object with the directories that it exists in and add it to the destination location
        self[self.dirname(srcPath)]._remove(srcObj)
        self._backfillHierarchy(self.dirname(destPath))._add(srcObj)

        # Move the file info across
        del self._paths[srcPath]
        self._paths[destPath] = srcObj
        srcObj._path = destPath

    @abstractmethod
    def _rm(self, artefact: Artefact, artefactPath: str):
        pass

    def _remove(self, obj: Artefact):
        # Inform all objects that they no longer exist
        if isinstance(obj, Directory):
            for art in self._ls(obj, recursive=True):
                del self._paths[art.path]
                art._exists = False

        obj._exists = False

        self._paths[self.dirname(obj.path)]._remove(obj)
        del self._paths[obj.path]

    def rm(self, artefact: typing.Union[Artefact, str], recursive: bool = False) -> None:
        """ Remove an artefact from the manager using the artefact object or its relative path. If its a directory,
        remove it if it is empty, or all of its contents if recursive has been set to true.

        Params:
            artefact (typing.Union[Artefact, str]): the object which is to be deleted
            recursive (bool) = False: whether to accept the deletion of a directory which has contents
        """

        obj, path = self._artefactFormStandardise(artefact, require=True)

        if obj is None or obj.manager is not self:
            raise exceptions.ArtefactNotMember("Artefact ({}) is not a member of the manager".format(artefact))

        if isinstance(obj, Directory) and len(obj) and not recursive:
            raise exceptions.OperationNotPermitted(
                "Cannot delete a container object that isn't empty - set recursive to True to proceed"
            )

        # Remove the artefact from the manager
        self._rm(obj, path)  # Remove the underlying data objects
        self._remove(obj)  # Remove references in the manager and set the objects._exist = False

    def mkdir(self, path: str):
        with tempfile.TemporaryDirectory() as directory:
            return self.put(directory, path)

    def touch(self, path: str) -> Artefact:

        with tempfile.TemporaryDirectory() as directory:
            emptyFile = os.path.join(directory, 'empty_file')
            open(emptyFile, 'w').close()
            return self.put(emptyFile, path)

    @abstractmethod
    def _makefile(self, path):
        """ Make a file object from a manager relative path """
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

        # Create a directory at this location, add it to the data store and return it
        art = Directory(self, path)
        self._add(art)
        return art

    def _artefactFormStandardise(self, artObj: typing.Union[Artefact, str], require=False) -> (Artefact, str):
        """ Convert the incoming object which could be either an artefact or relative path into a standardised form for
        both such that functions can be easily convert and use what they require

        Params:
            artObj (typing.Union[Artefact, str]): Either the artefact object or it's relative path to be standardised
            require (str): Require that the object exists. when false return None for yet to be created objects but

        Returns:
            Artefact or None: the artefact object or None if it doesn't exists and require is False
            str: The relative path of the object/the passed value
        """
        if isinstance(artObj, Artefact):
            return artObj, artObj.path
        else:
            if require or artObj in self: return self[artObj], artObj
            else: return None, artObj

    def _refresh(self, artobj):

        dirs, files = [], []
        for art in self._ls(artobj, recursive=True):
            try:
                # Test if the artefact still exists
                isDir = self._isdir(art.path)

                if isinstance(art, Directory) and not isDir or isinstance(art, File) and isDir:
                    # Though an artefact exists with the same name, its type has changed and therefore should be deleted
                    self._remove(art)

                elif isDir:
                    # No longer sure that any sub-directory is covered
                    art._collected = False

                else:
                    # Update files that still exist
                    art._update(self._makefile(art.path))

            except exceptions.ArtefactNotFound:
                # Underlying artefact has been deleted - cache it for deletion

                group = dirs if isinstance(art, Directory) else files

                for i, gart in enumerate(group):
                    if len(gart.path) > len(art.path):
                        group.insert(i, art)
                        break

                else:
                    group.append(art)

        # Minimise directories that are to be deleted
        coveredIndex = []
        for i, rdir in reversed(list(enumerate(dirs))):
            for sdir in dirs:
                if sdir is rdir: break

                if sdir.path == rdir.path[:len(sdir.path)]:
                    coveredIndex.append(i)

        for idx in coveredIndex: del dirs[idx]

        coveredIndex = []
        for i, file in enumerate(files):
            for sdir in dirs:
                if sdir.path == file.path[:len(sdir.path)]: coveredIndex.append(i)

        for idx in coveredIndex: del files[idx]

        for art in dirs + files:
            self._remove(art)

        artobj._collected = False

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
        obj, path = self._artefactFormStandardise(artefact)

        abspath = self.abspath(path)
        os.makedirs(os.path.dirname(abspath), exist_ok=True)

        yield abspath

        if os.path.isdir(abspath):
            # The localised object has been made to be a directory - object already in position tf just update the manager
            if obj is not None:
                assert isinstance(obj, Directory)
                self._refresh(obj)

        else:
            # The localised object is
            art = self._makefile(path)
            if obj is not None: obj._update(art)
            else:   self._add(art)

class RemoteManager(Manager, ABC):

    @staticmethod
    def _compare(dict1, dict2, key):
        # Extract the two sets of keys
        keys1, keys2 = set(dict1[key].keys()), set(dict2[key].keys())
        return keys1.difference(keys2), keys1.intersection(keys2), keys2.difference(keys1)

    @classmethod
    def _parseHierarchy(cls, path, _toplevel=None):

        # Store separately the directories and files of the path
        directories = {}
        files = {}

        # For each item process their checksums
        for item in os.listdir(path):

            # Identify their absolute path and relative manager path from the temporary local files
            abspath = os.path.join(path, item)

            if os.path.isdir(abspath):
                directories[abspath] = cls._parseHierarchy(abspath, _toplevel=path)

            else:
                files[abspath] = cls.md5(abspath)

        return {"directories": directories, "files": files}

    @classmethod
    def _compareHierarhy(cls, original, new):

        # Data containers for files and directory comparison
        toPush, toDelete = set(), set()

        # Compare the directories
        removed, editted, added = cls._compare(original, new, "directories")
        for directory in editted:
            put, delete = cls._compareHierarhy(original['directories'][directory], new['directories'][directory])

            # Union the result of the comparison on the sub directory level
            added |= put
            removed |= delete

        toPush |= added
        toDelete |= removed

        # Compare the files
        removed, editted, added = cls._compare(original, new, "files")
        for file in editted:
            if original['files'][file] != new['files'][file]:
                # The checksum of the files are not the same, therefore, the file has been editted and needs to be pushed
                added.add(file)

        toPush |= added
        toDelete |= removed

        return toPush, toDelete

    @contextlib.contextmanager
    def localise(self, artefact):
        obj, path = self._artefactFormStandardise(artefact)

        with tempfile.TemporaryDirectory() as directory:

            # Generate a temporay path for the file to be downloaded into
            local_path = os.path.join(directory, self.basename(path))

            # Get the contents and put it into the temporay directory
            if obj:
                self.get(path, local_path)

                if os.path.isdir(local_path):
                    # To collected item is a directory - walk the directory and record its state
                    checksum = self._parseHierarchy(local_path)

                else:
                    # Generate a checksum for the file
                    checksum = self.md5(local_path)

            else:
                # No checksum for no file
                checksum = None

            # Return the local path to the object
            yield local_path

            # The user has stopped interacting with the artefact - resovle any differences with manager
            if checksum:
                if os.path.isdir(local_path):
                    # Compare the new hiearchy - update only affected files/directories
                    put, delete = self._compareHierarhy(checksum, self._parseHierarchy(local_path))

                    # Define the method for converting the abspath back to the manager relative path
                    contexualise = lambda x: self.join(path, self.relpath(x[len(local_path):]))

                    # Put/delete the affected artefacts
                    for abspath in put: self.put(abspath, contexualise(abspath))
                    for abspath in delete: self.rm(contexualise(abspath), recursive=True)

                elif self.md5(local_path) != checksum:
                    # The file has been changed - upload the file's contents
                    self.put(local_path, path)

            else:
                # New item - put the artefact into the manager
                self.put(local_path, path)
