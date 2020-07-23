import os
from abc import ABC, abstractmethod
import typing
import shutil
import tempfile
import contextlib
import hashlib
import re

from . import exceptions
from .artefacts import Artefact, File, Directory, SubFile, SubDirectory

class Manager(ABC):
    """ Manager Abstract base class - expressed the interface of a Manager which governs a storage option and allows
    extraction and placement of files in that storage container

    """

    _ROOT_PATH = "/"
    _PLACEHOLDER = "placeholder.ignore"
    _READONLYMODES = ["r", "rb"]

    _RELPATH_REGEX = re.compile(r"^([a-zA-Z0-9]+:)?([/\\\w\.!-_*'() ]*)$")

    def __init__(self):
        self._root = Directory(self, self._ROOT_PATH)
        self._paths = {self._ROOT_PATH: self._root}
        self._submanagers = {}

    @abstractmethod
    def __repr__(self): pass

    @abstractmethod
    def abspath(self, relpath: str) -> str:
        """ Return the most accurate path to an object in the managers vernacular. Opposite of relpath

        examples:
            local managers shall convert a relative path to its full absolute os compatible filepath
            s3 shall convert the relative path to a s3 valid key

        Params:
            artefact (str): The artefact object or it's relative path which is to be converted
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

    def basename(self, relpath: str) -> str:
        """ Return the basename of the provided artefact/relative path. The base name of a filepath is the name of the
        file/folder at the end of the hierarchy.

        Params:
            artefact (Artefact/str): the artefact to have it's name extracted

        Returns:
            str: the base name of the arteface
        """
        return os.path.basename(self.relpath(relpath))

    def dirname(self, relpath: str) -> str:
        """ Return the dirname of the provided artefact/relative path. The base name string representation of the
        hierarchy of a file/folder. Returns is the path of the owning directory for the provided object

        Params:
            artefact (Artefact/str): the artefact to have it's dirname extracted

        Returns:
            str: relative path for the owning directory
        """
        return "/".join(relpath.split('/')[:-1]) or '/'

    def join(self, *components) -> str:
        """ Join a relative path with another path for sub and return a manager relative path
        """
        return self.relpath("/".join(components))

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

    def __contains__(self, artefact: typing.Union[Artefact, str]):
        if isinstance(artefact, Artefact): return artefact.manager is self
        try:
            self[artefact]
            return True
        except exceptions.ArtefactNotFound:
            return False

    def _add(self, art: Artefact):
        """ Add an artefact object into the manager data structures

        Parmas:
            art (Artefact): The artefact object to be added
        """
        assert art.manager is self
        owner = self._backfillHierarchy(self.dirname(art.path))
        owner._add(art)
        self._paths[art.path] = art

        if self._submanagers:
            # Ensure that the artefact has been added to any sub managers this artefact resides in

            for uri, manager in self._submanagers.items():
                if art.path.startswith(uri):
                    # The artefact exists within the sub manager - pass the parent object
                    manager._addMain(art)

    @abstractmethod
    def _get(self, source: Artefact, destination: str):
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
    def _put(self, source_filepath: str, destinationAbsPath: str):
        """ Put the local filesystem object onto the underlying manager implementation using the absolute path given.

        Params:
            source_filepath (str): The local filesystem filepath to source object
            destinationAbsPath (str): Remote absolute path
        """
        pass

    @abstractmethod
    def _putBytes(self, fileBytes: bytes, destinationAbsPath: str):
        """ Put the bytes of a file object onto the underlying manager implementation using the absolute path given.

        Params:
            fileBytes (bytes): files bytes
            destinationAbsPath (str): Remote absolute path
        """
        pass

    def __putArtefact(
        self,
        source: typing.Union[str, bytes],
        destinationArtifact: Artefact,
        desintationPath: str
        ) -> Artefact:

        # Clean up any files that current exist at the location
        if destinationArtifact is not None: self._rm(destinationArtifact)

        # Put the local file onto the remote using the manager definition
        if isinstance(source, str):
            # The artefact is a local object is persistent storage
            self._put(source, self.abspath(desintationPath))

            # Extract the artefact depending on the type of input
            if os.path.isdir(source):
                # Source is a directory

                if destinationArtifact is not None:
                    # An object original existed - identify type of object and handle accordingly

                    if isinstance(destinationArtifact, Directory):
                        # The original object was a directory - compare downloaded objects with objects to remove no longer
                        # present files and update files to the newly uploaded content
                        self._refresh(destinationArtifact)
                        return destinationArtifact

                    else:
                        # File is being replaced with a directory - delete the file and create a new directory object
                        self._remove(destinationArtifact)

                return self._backfillHierarchy(desintationPath)

            else:
                # Source is a file
                art = self._makefile(desintationPath)

                if destinationArtifact is not None:
                    if isinstance(destinationArtifact, File):
                        # The artefact has overwritten a previous file - update it and return it
                        original = self._paths[desintationPath]
                        original._update(art)
                        return original

                    else:
                        self._remove(destinationArtifact)

            # Add the new artefact and return it
            self._add(art)
            return art

        else:
            # The artefact is a file binary
            self._putBytes(source, self.abspath(desintationPath))

            art = self._makefile(desintationPath)
            self._add(art)
            return art

    def put(self, source: typing.Union[Artefact, str, bytes], destination: typing.Union[Artefact, str]) -> Artefact:
        """ Put a local artefact onto the remote at the location given.

        Params:
            src_local (str): The path to the local artefact that is to be put on the remote
            dest_remote (Artefact/str): A file object to overwrite or the relative path to a destination on the
                remote
        """

        # Verify that the destination is valid
        destinationArtifact, destinationPath = self._artefactFormStandardise(destination)
        if destinationArtifact is not None and destinationArtifact.manager is not self:
                raise exceptions.ArtefactNotMember("Destination artefact is not a member of the manager")

        if isinstance(source, Artefact):
            with source.manager.localise(source) as sourceAbsPath:
                return self.__putArtefact(sourceAbsPath, destinationArtifact, destinationPath)

        else:
            # The source is a local filepath or byte stream
            return self.__putArtefact(source, destinationArtifact, destinationPath)

    @abstractmethod
    def _cp(self, srcObj: Artefact, destPath: str):
        pass

    def cp(self, source: typing.Union[Artefact, str], destination: typing.Union[Artefact, str]):
        """ Move the artefacts at the source location to the provided destination location. Overwriting items at the
        destination

        Params:
            source (typing.Union[Artefact, str]): source path or artefact
            destination (typing.Union[Artefact, str]): destination path or artefact
        """

        # Understand the objects being moved
        srcObj, _ = self._artefactFormStandardise(source)
        destObj, destPath = self._artefactFormStandardise(destination)

        # Destination content is being overwritten
        if destObj: self._remove(destObj)  # Remove references in the manager and set the objects._exist = False

        # Call the lower level content move function on the manager and convert all paths on the manager
        self._cp(srcObj, destPath)

    @abstractmethod
    def _mv(self, srcObj: Artefact, destPath: str):
        pass

    def _move(self, srcObj: Artefact, destPath):
        if isinstance(srcObj, Directory):

            for art in self._ls(srcObj, recursive=True):
                # Get the downloaded content for from the directory object

                path = art.path

                # Update the object with it's new path
                art._path = self.join(destPath, art.path[len(srcObj.path):])

                # Update its membership
                del self._paths[path]
                self._paths[art.path] = art

        if self.dirname(srcObj.path) != self.dirname(destPath):
            # Unconnect object with the directories that it exists in and add it to the destination location
            self[self.dirname(srcObj.path)]._remove(srcObj)
            self._backfillHierarchy(self.dirname(destPath))._add(srcObj)

        # Move the file info across
        del self._paths[srcObj.path]
        self._paths[destPath] = srcObj
        source_path = srcObj.path
        srcObj._path = destPath

        if self._submanagers:
            for uri, manager in self._submanagers.items():
                if srcObj.path.startswith(uri):
                    # The originating files have moved within the sub manager

                    if destPath.startswith(uri):
                        # The destination is within the sub-manager also
                        manager._moveMain(source_path, destPath)

                    else:
                        # The destination is outside the sub-manager - the subfiles need to be deleted
                        manager._removeMain(srcObj)

    def mv(self, source: typing.Union[Artefact, str], destination: typing.Union[Artefact, str]):
        """ Move the artefacts at the source location to the provided destination location. Overwriting items at the
        destination

        Params:
            artefact (typing.Union[Artefact, str]): source path or artefact
            recursive (typing.Union[Artefact, str]): destination path or artefact
        """

        # Understand the objects being moved
        srcObj, _ = self._artefactFormStandardise(source)
        destObj, destPath = self._artefactFormStandardise(destination)

        # Destination content is being overwritten
        if destObj: self.rm(destObj, recursive=True)

        # Call the lower level content move function on the manager and convert all paths on the manager
        self._mv(srcObj, destPath)
        self._move(srcObj, destPath)

    @abstractmethod
    def _rm(self, artefact: Artefact):
        pass

    def _remove(self, artefact: Artefact):
        """ Remove an artefact from the manager but do not check against/remove objects from the underlying
        implementation. This is to be used in conjuction with `_rm()` or to clean up artefacts that could have been
        affected as a side effect

        Params:
            artefact (Artefact): Manager artefact that is to be deleted
        """
        if isinstance(artefact, Directory):
            # NOTE we avoid calling this function recursively to avoid issues with of removing directories
            # and their subelements. Additionally as the directories keep weakreferences to their contents items will
            # not keep each other alive and shall be removed when the GC deems it apprioprate
            for art in self._ls(artefact):
                del self._paths[art.path]
                art._exists = False

        # Delete references to the object and set it's existence to false
        self[self.dirname(artefact.path)]._remove(artefact)
        del self._paths[artefact.path]
        artefact._exists = False

        if self._submanagers:
            for uri, manager in self._submanagers.items():
                if artefact.path.startswith(uri):
                    manager._removeMain(artefact)

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
        self._rm(obj)  # Remove the underlying data objects
        self._remove(obj)  # Remove references in the manager and set the objects._exist = False

    @abstractmethod
    def _collectDirectoryContents(self, directory: Directory) -> None:
        """ Collect and instatiate the contents of a directory making a directory object _collected = True

        Params:
            directory (Directory): The directory which is to be checked
        """
        pass


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

    def ls(self, art: typing.Union[Directory, str] = '/', recursive: bool = False) -> typing.Set[Artefact]:
        """ List contents of the directory path/artefact given.

        Params:
            art (Directory/str): The Directory artefact or the relpath to the directory to be listed
            recursive (bool) = False: Return subdirectory contents aswell

        Returns:
            {Artefact}: The artefact objects which are within the directory
        """

        # Convert the incoming artefact reference - requre that the object exist and that it is a directory
        artobj, path = self._artefactFormStandardise(art, require=True)
        if not isinstance(artobj, Directory): raise TypeError("Cannot perform ls action on File artefact: {}".format(artobj))

        # Perform JIT download of directory contents
        if not artobj._collected:
            self._collectDirectoryContents(artobj)

            # # Identify what has already been downloaded into the directory object
            # ddirs, dfiles = set(), set()
            # for art in artobj._contents:
            #     if isinstance(art, Directory): ddirs.add(art.path)
            #     else: dfiles.add(art.path)

            # # For all the other objects that have not yet been downloaded for the object - download them
            # dirs, files = self._listdir(path)
            # for directory in dirs.difference(ddirs): self._backfillHierarchy(directory)
            # for file in files.difference(dfiles): self._add(self._makefile(file))

            # # Signal that the directory contents has been downloaded NOTE not recursive information
            # artobj._collected = True

        if recursive:

            # Iterate through contents and recursively add lower level artifacts
            contents = set()
            for art in artobj._contents:
                if isinstance(art, Directory): contents |= self.ls(art, recursive)
                contents.add(art)

            # Return all child content
            return contents

        return set(artobj._contents)

    def mkdir(self, path: str):
        with tempfile.TemporaryDirectory() as directory:
            return self.put(directory, path)

    def touch(self, relpath: str) -> Artefact:
        with tempfile.TemporaryDirectory() as directory:
            emptyFile = os.path.join(directory, 'empty_file')
            open(emptyFile, 'w').close()
            return self.put(emptyFile, relpath)

    @abstractmethod
    def _makefile(self, relpath: str) -> Artefact:
        """ Make a file object using the underlying implementation objects from a manager relative path

        Params:
            relpath (str): Relative manager file position
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
        """ Perform a update for the manager on the contents of a directory which has been editted on mass and whose
        content is likely inconsistent with the current state of the manager. Only previously known files are checked as
        new files are to be loaded JIT and can be added at that stage.

        Params:
            artobj (Directory): The directory to perform the refresh on
        """

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

    def submanager(self, uri: str):
        """ Create a submanager at the given uri which shall behave like a conventional manager, however, its actions
        shall be relative to the given uri and shall update the main manager.

        If a manager exists at the uri specified already, then it is returned.

        Params:
            uri (str): The uri of the target location for the manager to be setup. If the uri does not exist, a
                directory shall be created. If it exists, the manager shall require it be a Directory object

        Returns:
            SubManager: A sub manager at the given uri

        Raises:
            ValueError: Raised if uri is top level directory
            ArtefactTypeError: if there exists an object at the location which isn't a directory
        """
        if uri == "/": raise ValueError("Cannot create a sub-manager at the top level of a manager")
        if uri in self._submanagers: return self._submanagers[uri]

        # Get or make the uri directory
        try:
            art = self[uri]
        except exceptions.ArtefactNotFound:
            art = self.mkdir(uri)

        # Ensure it is a directory and return + save the manager
        if isinstance(art, Directory):
            manager = SubManager(self, uri, art)
            self._submanagers[uri] = manager
            return manager
        else:
            raise exceptions.ArtefactTypeError("Cannot create a submanager with a file's path")

class SubManager(Manager):

    def __init__(self, owner: Manager, uri: str, rootDirectory: Directory):
        self._root = SubDirectory(self, self._ROOT_PATH, rootDirectory)
        self._paths = {self._ROOT_PATH: self._root}
        self._submanagers = None
        self._owner = owner
        self._uri = uri

    def __repr__(self): return '<SubManager of {} {}>'.format(self._owner, self._uri)
    def abspath(self, relpath: str): return self._owner.abspath(self.join(self._uri, relpath))
    def _subrelpath(self, mainRelpath: str): return mainRelpath[len(self._uri):]
    def _isdir(self, relpath: str) -> str: return self._owner._isdir(self.join(self._uri, relpath))

    def _makefile(self, relpath: str) -> Artefact:
        # Given that its a local relative path
        mainArt = self._owner._makefile(self.join(self._uri, relpath))
        return SubFile(self, relpath, mainArt)

    def _backfillHierarchy(self, relpath: str):
        # Ask the parent to make the directory object at this location. This shall then be added to this sub manager
        # by the parent along with any other directories that were created - by the end of this call, a directory
        # shall exist in this manager for this relpath
        self._owner._backfillHierarchy(self.join(self._uri, relpath))
        return self._paths[relpath]

    def _add(self, artefact: Artefact):
        # Add the artefact and link it to the main artefact it represents
        assert isinstance(artefact, (SubFile, SubDirectory))
        super()._add(artefact)
        self._owner._add(artefact._concrete)

    def _addMain(self, artefact: Artefact):
        # Add an artefact which has been created in the main manager
        relpath = artefact.path[len(self._uri):]
        if relpath in self._paths: return

        if isinstance(artefact, File):
            subArtefact = SubFile(self, relpath, artefact)
        else:
            subArtefact = SubDirectory(self, relpath, artefact)

        super()._add(subArtefact)

    def _get(self, source: Artefact, destination: str):
        # Switch artefact to main to download it's contents
        self._owner._get(source._concrete, destination)

    def _put(self, source_filepath, destination_abspath):
        # Abspath already surpasses this manager - pass args straight on
        self._owner._put(source_filepath, destination_abspath)

    def _putBytes(self, source, destinationAbsPath):
        self._owner._putBytes(source, destinationAbsPath)

    # NOTE movement of files handled by main manager
    def _cp(self, srcObj: Artefact, destPath: str): self._owner._cp(srcObj._concrete, self.join(self._uri, destPath))
    def _mv(self, srcObj: Artefact, destPath: str): self._owner._mv(srcObj._concrete, self.join(self._uri, destPath))
    def _move(self, srcObj: Artefact, destPath: str): self._owner._move(srcObj._concrete, self.join(self._uri,destPath))
    def _moveMain(self, srcPath: str, destPath: str): super()._move(self[self._subrelpath(srcPath)], self._subrelpath(destPath))

    # Main manager handles deleting the underlying objects
    def _rm(self, artefact: Artefact): self._owner._rm(artefact._concrete)
    def _remove(self, artefact: Artefact): self._owner._remove(artefact._concrete)
    def _removeMain(self, artefact: Artefact): super()._remove(self[self._subrelpath(artefact.path)])

    def _collectDirectoryContents(self, directory: Directory):
        self._owner._collectDirectoryContents(directory._concrete)

    def _listdir(self, relpath: str):
        dirs, files = self._owner._listdir(self.join(self._uri, relpath))

        uriLength = len(self._uri)
        dirs, files = {p[uriLength:] for p in dirs}, {p[uriLength:] for p in files}

        return dirs, files

    @contextlib.contextmanager
    def localise(self, artefact: typing.Union[Artefact, str]):
        with type(self._owner).localise(self, artefact) as abspath:
            yield abspath

    def submanager(self):
        raise NotImplementedError("A submanager cannot be created on a submanager")

class LocalManager(Manager, ABC):

    @contextlib.contextmanager
    def localise(self, artefact):
        obj, path = self._artefactFormStandardise(artefact)
        exception = None

        abspath = self.abspath(path)
        os.makedirs(os.path.dirname(abspath), exist_ok=True)

        try:
            yield abspath
        except Exception as e:
            exception = e

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

        if exception:
            raise exception

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
        exception = None

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
            try:
                yield local_path
            except Exception as e:
                exception = e

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

        if exception:
            raise exception
