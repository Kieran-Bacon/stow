import os
import re
import io
import abc
import typing
import urllib
import shutil
import tempfile
import contextlib

from .abstract_methods import AbstractManager
from .class_methods import ClassMethodManager

from ..artefacts import Artefact, File, Directory, SubFile, SubDirectory
from .. import utils
from .. import exceptions

import logging
log = logging.getLogger(__name__)

class Manager(AbstractManager, ClassMethodManager):
    """ Manager Abstract base class - expressed the interface of a Manager which governs a storage option and allows
    extraction and placement of files in that storage container

    """

    SUPPORTS_UNICODE_FILENAMES = os.path.supports_unicode_filenames

    _ROOT_PATH = "/"
    _PLACEHOLDER = "placeholder.ignore"
    _READONLYMODES = ["r", "rb"]

    _MULTI_SEP_REGEX = re.compile(r"(\\{2,})|(\/{2,})")
    _RELPATH_REGEX = re.compile(r"^([a-zA-Z0-9]+:)?([/\\\w\.!-_*'() ]*)$")

    def __init__(self):
        self._root = Directory(self, self._ROOT_PATH)
        self._paths = {self._ROOT_PATH: self._root}
        self._submanagers = {}

    def __contains__(self, artefact: typing.Union[Artefact, str]) -> bool:
        if isinstance(artefact, Artefact):
            return self is artefact.manager
        else:
            return self.exists(artefact)

    def __getitem__(self, path: str) -> Artefact:
        """ Fetch an artefact from the manager. In the event that it hasn't been cached, look it up on the underlying
        implementation and return a newly created object. If it doesn't exist raise an error

        Args:
            managerPath: The manager relative path to fine the artefact with

        Returns:
            artefact: The artefact at the provided location path

        Raises:
            ArtefactNotFound: In the event that the path does not exist
        """

        # Clean the path given - ensures its a manager path
        path = self._managerPath(path)

        # If the path exists within the manager - return it
        if path in self._paths:
            return self._paths[path]

        # Load the artefact and return it
        return self._loadArtefact(path)

    def _managerPath(self, path: str) -> str:

        if not path:
            return "/"

        # Expand any environment variables but no home path and do not make absolute relative to local
        path = self.join("/", self.normpath(self.expandvars(path)), separator='/')

        if os.name == 'nt':
            # Make Windows paths (if provided) unix like
            if path.find(":") != -1:
                # Remove the drive name
                path = path[path.index(":")+1:]

            path = path.replace("\\", "/")

        return path

    def _ensureDirectory(self, managerPath: str) -> Directory:
        """ Fetch the owning `container` for the manager relative path given. In the event that no `container` object
        exists for the path, create one and recursively find its owning `container` to add it to. The goal of this
        function is to traverse up the hierarchy and ensure all the directory objects exist, and when they do quickly
        return the container they are in

        Args:
            path (str): The manager relative path for an `Artefact`

        Returns:
            Directory: The owning directory container, which may have just been created

        Raises:
            ArtefactNotFound: If the path given doesn't exist in the manager
            ArtefactTypeError: The path exists but it isn't a directory object which is what is expected
        """

        if managerPath in self._paths:
            # The path points to an already established directory
            directory = self._paths[managerPath]
            if isinstance(directory, File):
                raise exceptions.ArtefactTypeError("Invalid path given {}. Path points to a file {}.".format(managerPath, directory))

            return directory

        if not managerPath:
            # No path given - root is being asked for
            return self._root

        # Create a directory at this location, add it to the data store and return it
        art = self._identifyPath(managerPath)
        if art is None:
            raise exceptions.ArtefactNotFound("No artefact found at location {}".format(managerPath))

        elif isinstance(art, File):
            raise exceptions.ArtefactTypeError("Invalid path given {}. Path points to a file {}.".format(managerPath, art))

        self._addArtefact(art)  # Link it with any owner + submanagers
        return art

    def _addArtefact(self, artefact: Artefact):
        """ Add an artefact object into the manager data structures - do not add if the object has already been added

        Args:
            artefact: The artefact object to be added

        Raises:
            ArtefactNotMember: in the event that the artefact that is trying to be added was not created by this manager
        """

        if artefact.manager is not self:
            raise exceptions.ArtefactNotMember("Artefact {} is not a member of {} and couldn't be added".format(artefact, self))

        if artefact.path in self._paths:
            # The artefact was already member of the manager - Updating the original artefact with new data
            self._paths[artefact.path]._update(artefact)

        else:
            # The artefact is new - ensure the parent directory and add the artefact into the manager

            # Get the directory for the artefact - add the artefact to that directories contents
            directory = self._ensureDirectory(self.dirname(artefact))
            directory._add(artefact)

            # Add the artefact into the manager store
            self._paths[artefact.path] = artefact

        if self._submanagers:
            # Ensure that the artefact has been added to any sub managers this artefact resides in

            for uri, manager in self._submanagers.items():
                if artefact.path.startswith(uri):
                    # The artefact exists within the sub manager - pass the parent object
                    manager._cascadeAddArtefact(artefact)

    def _loadArtefact(self, managerPath: str) -> Artefact:
        """ Identify the type of the artefact and then add it to the manager

        This allows you to specify the method of adding the artefact and doesn't limit you to adding a single artefact.
        It may be better to load an entire directory and then return the targeted artefact

        Args:
            managerPath: The manager relative path to the object to be loaded

        Returns:
            Artefact: the artefact created at that location

        Raises:
            ArtefactNotFound: If the path doesn't lead to an artefact
        """

        # Check to see if there is an artefact that exists on disk
        obj = self._identifyPath(managerPath)
        if obj is None:
            raise exceptions.ArtefactNotFound("Couldn't locate artefact {}".format(managerPath))

        # Add the created artefact to the manager
        self._addArtefact(obj)

        return obj

    def _findArtefact(self, source: typing.Union[Artefact, str]) -> Artefact:
        """ Find artefact even if it isn't managed by this manager. Use the public methods to create a manager for the
        incoming source object and get from it the artefact object
        """


        sourceObject, _ = self._artefactFormStandardise(source)
        if sourceObject is None:
            # The artefact wasn't given and the path doesn't lead to an artefact on the manager

            result = urllib.parse.urlparse(source)

            # Find the manager that is correct for the protocol
            if result.scheme and result.netloc:
                manager = utils.find(result.scheme)
                return manager._loadFromProtocol(result)[result.path]

            else:
                # Local manager - start it at the base of the file system
                manager = utils.connect("FS", path="/")
                return manager[self.abspath(source)]

        return sourceObject

    def _artefactFormStandardise(self, artefact: typing.Union[Artefact, str], require=False) -> typing.Tuple[Artefact, str]:
        """ Convert the incoming object which could be either an artefact or relative path into a standardised form for
        both such that functions can be easily convert and use what they require

        Args:
            artObj (typing.Union[Artefact, str]): Either the artefact object or it's relative path to be standardised
            require (str): Require that the object exists. when false return None for yet to be created objects but

        Returns:
            Artefact or None: the artefact object or None if it doesn't exists and require is False
            str: The relative path of the object/the passed value
        """
        if isinstance(artefact, Artefact):
            return artefact, artefact.path

        else:
            if require:
                # The artefact must be collected
                obj = self[artefact]
                return obj, obj.path

            # Clean and ensure the artefact path
            artefact = self._managerPath(artefact)

            if self.exists(artefact):
                # Check to see if the artefact exists - if it does then it is available
                return self._paths[artefact], artefact

            else:
                # The object doesn't exist on disk
                return None, artefact

    def _updateArtefactObjects(self, artefact: Artefact):
        """ Perform a update for the manager on the contents of a directory which has been editted on mass and whose
        content is likely inconsistent with the current state of the manager. Only previously known files are checked as
        new files are to be loaded JIT and can be added at that stage.

        Args:
            artobj (Directory): The directory to perform the refresh on
        """

        if isinstance(artefact, File):
            file = self._identifyPath(artefact.path)
            if not isinstance(file, File):
                # The object is no longer the same type - the original needs to be removed
                return self._delinkArtefactObjects(artefact)

            artefact._update(file)

        else:
            artefact: Directory

            # For the artefacts we know about - check their membership
            for artefact in list(artefact._contents):

                # Have the path checked
                check = self._identifyPath(artefact.path)

                # Update the artefact according to its state on disc
                if check is None or type(artefact) != type(check):
                    # The artefact has been deleted or the type of the artefact has changed - it needs to be delinked
                    self._delinkArtefactObjects(artefact)

                elif isinstance(artefact, File):
                    # Update the artefact with the informant as we've pulled it
                    artefact._update(check)

                else:
                    # The directory needs to be checked for issues
                    self._updateArtefactObjects(artefact)

            # Cannot be sure that all of the contents has been collected due to change
            artefact._collected = False

    def _moveArtefactObjects(self, srcObj: Artefact, destPath: str):
        """ Move a source an artefact to a new """

        if isinstance(srcObj, Directory):
            # Need to loop over directory contents and update their paths - their directory membership is fine

            for art in srcObj._ls(True):

                # Remove the artefact from its position in the manager
                del self._paths[art.path]

                # Update the object with it's new path
                art._path = self.join(destPath, srcObj.relpath(art), separator='/')

                # Update its membership
                self._paths[art.path] = art

        # Check whether the object has moved outside of the directory it was originally in
        if self.dirname(srcObj.path) != self.dirname(destPath):
            # Disconnect object with the directories that it exists in and add it to the destination location
            srcObj.directory._remove(srcObj)
            self._ensureDirectory(self.dirname(destPath))._add(srcObj)

        # Move the object in the manager state
        self._paths[destPath] = self._paths.pop(srcObj.path)

        # Update the artefacts info
        source_path = srcObj.path
        srcObj._path = destPath

        if self._submanagers:
            for uri, manager in self._submanagers.items():
                if srcObj.path.startswith(uri):
                    # The originating files have moved within the sub manager

                    if destPath.startswith(uri):
                        # The destination is within the sub-manager also
                        manager._cascadeMoveArtefactObjects(source_path, destPath)

                    else:
                        # The destination is outside the sub-manager - the subfiles need to be deleted
                        manager._cascadeDelinkArtefactObjects(srcObj)

    def _delinkArtefactObjects(self, artefact: Artefact):
        """ Unreference an artefact from the manager but do not check against/remove objects from the underlying
        implementation. This is to be used in conjunction with `_rm()` or to clean up artefacts that could have been
        affected as a side effect

        Args:
            artefact (Artefact): Manager artefact that is to be deleted
        """
        if isinstance(artefact, Directory):
            # Loop through cached directory contents and free those artefacts
            for childArtefact in artefact._ls(True):
                # NOTE directory only holds weakrefs to objects + the parent directory is being deleted so all
                # child objects should be deleted just fine.
                del self._paths[childArtefact.path]
                childArtefact._exists = False

        # Delete references to the object and set it's existence to false
        self[self.dirname(artefact.path)]._remove(artefact)
        del self._paths[artefact.path]

        # Get the artefact path before we make it existence False and cannot have access
        artefactPath = artefact.path

        # Make the artefact non existent
        artefact._exists = False

        if self._submanagers:
            for uri, manager in self._submanagers.items():
                if artefactPath.startswith(uri):
                    manager._cascadeDelinkArtefactObjects(artefactPath)

    def isfile(self, artefact: typing.Union[Artefact, str]) -> bool:
        """ Check if the artefact provided is a file

        Args:
            artefact: The artefact to be checked

        Returns:
            Bool: True or False in answer to the question
        """
        if isinstance(artefact, Artefact):
            return isinstance(artefact, File)

        return isinstance(self[artefact], File) if self.exists(artefact) else False

    def isdir(self, artefact: typing.Union[Artefact, str]) -> bool:
        """ Check if the artefact provided is a directory

        Args:
            artefact: The artefact to be checked

        Returns:
            Bool: True or False in answer to the question
        """


        if isinstance(artefact, Directory):
            return isinstance(artefact, File)

        return isinstance(self[artefact], Directory) if self.exists(artefact) else False

    def islink(self, artefact: typing.Union[Artefact, str]) -> bool:
        """ Check if the artefact provided is a link

        Will check for local managers but will default to False for remote managers

        Args:
            artefact: The artefact to be checked

        Returns:
            Bool: True or False in answer to the question
        """
        if isinstance(self, LocalManager):
            # We are local and we can use the local test
            return os.path.islink(self.abspath(artefact))

        else:
            log.warning("islink: Symbolic links are not supported - defaulting to False")
            return False

    def ismount(self, artefact: typing.Union[Artefact, str]) -> bool:
        """ Check if the artefact provided is a link

        Will check for local managers but will default to False for remote managers

        Args:
            artefact: The artefact to be checked

        Returns:
            Bool: True or False in answer to the question
        """
        if isinstance(self, LocalManager):
            # We are local and we can use the local test
            return os.path.ismount(self.abspath(artefact))

        else:
            log.warning("ismounts: mount point are not implemented on remote managers - returning False")
            return False

    def getctime(self, artefact: typing.Union[Artefact, str]) -> typing.Union[float, None]:
        """ Get the created time for the artefact as a UTC timestamp

        Args:
            artefact: the artefact whose creation datetime is to be returned

        Returns:
            timestamp: a float timestamp of creation time if manager holds such information else None

        Raises:
            ArtefactNotFound: If there is no artefact at the location
        """
        obj, _ = self._artefactFormStandardise(artefact, require=True)
        return obj.createdTime.timestamp()

    def getmtime(self, artefact: typing.Union[Artefact, str]) -> typing.Union[float, None]:
        """ Get the modified time for the artefact as a UTC timestamp

        Args:
            artefact: the artefact whose modified datetime is to be returned

        Returns:
            timestamp: a float timestamp of modified time if manager holds such information else None

        Raises:
            ArtefactNotFound: If there is no artefact at the location
        """
        obj, _ = self._artefactFormStandardise(artefact, require=True)
        return obj.modifiedTime.timestamp()

    def getatime(self, artefact: typing.Union[Artefact, str]) -> typing.Union[float, None]:
        """ Get the accessed time for the artefact as a UTC timestamp

        Args:
            artefact: the artefact whose accessed datetime is to be returned

        Returns:
            timestamp: a float timestamp of accessed time if manager holds such information else None

        Raises:
            ArtefactNotFound: If there is no artefact at the location
        """
        obj, _ = self._artefactFormStandardise(artefact, require=True)
        return obj.accessedTime.timestamp()

    def exists(self, artefact: typing.Union[Artefact, str]) -> bool:
        """ Return true if the given artefact is a member of the manager, or the path is correct for the manager and it
        leads to a File or Directory.

        Does not handle protocols

        Args:
            artefact: Artefact or path whose existence is to be checked

        Returns:
            bool: True if artefact exists else False
        """
        # Only split as we want to trigger reassessment of the underlying file
        obj, path = self._splitArtefactUnionForm(artefact)

        # Check to see if the artefact exists on disc still
        check = self._identifyPath(self._managerPath(path))

        # Check + update state if information about the object has changed
        if obj is not None:

            if check is not None:
                # Both objects exist - check that they have changed type and update if not

                if type(obj) != type(check):

                    # The obj has changed type
                    self._delinkArtefactObjects(obj)

                    # Add the artefact in
                    self._addArtefact(check)

                else:
                    # An update has occurred
                    if isinstance(obj, File):
                        obj._update(check)

            else:
                # The on disk representation has been removed
                self._delinkArtefactObjects(obj)

        elif check is not None:
            # A new artefact has been created
            self._addArtefact(check)

        # If not none then its a valid object - any updates will have taken place
        return check is not None

    def lexists(self, artefact: typing.Union[Artefact, str]) -> str:
        """ Return true if the given artefact is a member of the manager, or the path is correct for the manager and it
        leads to a File or Directory.

        Does not handle protocols

        Args:
            artefact: Artefact or path whose existence is to be checked

        Returns:
            bool: True if artefact exists else False
        """
        log.warning("lexists: Symbolic links are not supported - defaulting to exists")
        return self.exists(artefact)

    def get(self, source: typing.Union[Artefact, str], destination: str = None, overwrite: bool = False) -> Artefact:
        """ Get a remote artefact from the storage option and write it to the destination path given.

        Args:
            source (Artefact/str): The remote's file object or its path
            destination (str) = None: The local path for the artefact to be written to

        Returns:
            typing.Union[typing.Any, bytes]: Return user defined response for get if file written to destination else
                return bytes if no desintation given
        """

        # Ensure the detination - Remove or raise issue for a local artefact at the location where the get is called
        if destination is not None:
            if os.path.exists(destination):
                if os.path.isdir(destination):
                    if overwrite:
                        shutil.rmtree(destination)

                    else:
                        raise exceptions.OperationNotPermitted(
                            "Cannot replace local directory ({}) unless overwrite argument is set to True".format(destination)
                        )

                else:
                    os.remove(destination)

            else:
                # Ensure the directory that this object exists with
                os.makedirs(self.dirname(destination), exist_ok=True)

        # Split into object and path - Ensure that the artefact to get is from this manager
        obj, path = self._artefactFormStandardise(source, require=True)
        if obj.manager is not self:
            raise exceptions.ArtefactNotMember("Provided artefact is not a member of the manager")

        # Fetch the object and place it at the location
        if destination is not None:
            return self._get(path, destination)

        else:
            if not isinstance(obj, File):
                raise exceptions.ArtefactTypeError("Cannot get file bytes of {}".format(obj))

            return self._getBytes(path)

    def put(
        self,
        source: typing.Union[Artefact, str, bytes],
        destination: typing.Union[Artefact, str],
        overwrite: bool = False,
        ) -> Artefact:
        """ Put a local artefact onto the remote at the location given.

        Args:
            src_local (str): The path to the local artefact that is to be put on the remote
            dest_remote (Artefact/str): A file object to overwrite or the relative path to a destination on the
                remote
            overwrite (bool) = False: Whether to accept the overwriting of a target destination when it is a directory
        """

        # Break the destination object apart
        destinationObj, destinationPath = self._artefactFormStandardise(destination)

        # Validated and prepare the destination
        if destinationObj is not None:

            # Check destination is member
            if destinationObj.manager is not self:
                raise exceptions.ArtefactNotMember("Destination artefact is not a member of the manager")

            # Delete the object to make space for new object
            if overwrite or isinstance(destinationObj, File) :
                # Remove the destination object
                self._rm(destinationObj.path)

            else:
                raise exceptions.OperationNotPermitted(
                    "Cannot put {} as destination is a directory, and overwrite has not been set to True"
                )

        # Process the source and put it onto the manager
        if isinstance(source, bytes):
            # Source is file bytes - pass to manager implementation
            self._putBytes(source, destinationPath)

        else:
            # Source is a artefact in persisted storage
            if isinstance(source, str):
                # Turn the path into an artefact object even for external managers
                source = self._findArtefact(self.abspath(source))

            # Ensure that the artefact can be put by localising it
            with source.localise() as abspath:
                # Put the artefact into the destination
                self._put(abspath, destinationPath)

        # Post put cleanup - process the destination object
        if destinationObj is not None:
            # Validate whether the destination obj needs to be updated or removed as a consequence

            # Check where the type of the source is the same as the type of the destination object
            if isinstance(source, (bytes, File)) == isinstance(destinationObj, File):
                # The source placed is the same type as the established destination object - can update
                self._updateArtefactObjects(destinationObj)

                # Return the original and updated destination object
                return destinationObj

            else:
                # Different object type now so we need to get rid of the original artefact and replace
                self._delinkArtefactObjects(destinationObj)

        # Create a new artefact object and return
        return self._loadArtefact(destinationPath)

    def cp(
        self,
        source: typing.Union[Artefact, str],
        destination: typing.Union[Artefact, str],
        overwrite: bool = False
        ) -> Artefact:
        """ Copy the artefacts at the source location to the provided destination location. Overwriting items at the
        destination.

        Args:
            source: source path or artefact
            destination: destination path or artefact
            overwrite: Whether to overwrite directories my move

        Returns:
            Artefact: The destination artefact object
        """

        # Ensure the destination - get the destination object
        destinationObj, destinationPath = self._artefactFormStandardise(destination)
        if destinationObj:

            # Only work on targets that are on the manager
            if destinationObj.manager is not self:
                raise exceptions.ArtefactNotMember(
                    "Cannot copy onto an artefact {} not within manager {}".format(destinationObj, self)
                )

            # There is an artefact at that location that will need to be removed - check if that is allowed
            if isinstance(destinationObj, Directory) and not overwrite:
                raise exceptions.OperationNotPermitted(
                    "Cannot copy artefact to location as destination is a directory {} and overwrite has not been toggled".format(
                        destinationObj
                    )
                )

            # Remove the original object
            self._rm(destinationObj.path)
            self._delinkArtefactObjects(destinationObj)

        # Look to see if the source artefact is in the manager - if so we can try to be more efficient
        sourceObject, sourcePath = self._artefactFormStandardise(source)
        if sourceObject is None or sourceObject.manager is not self:
            # The source isn't inside this manager - we must find it and use put. No speed up to be had in manager

            if sourceObject is None:
                # Find the original source object
                sourceObject = self._findArtefact(source)

            return self.put(sourceObject, destinationPath)

        # We must be an artefact on the box copying to another location on the box - destination is clear
        self._cp(sourcePath, destinationPath)
        return self[destinationPath]

    def mv(
        self,
        source: typing.Union[Artefact, str],
        destination: typing.Union[Artefact, str],
        overwrite: bool = False
        ) -> Artefact:
        """ Copy the artefacts at the source location to the provided destination location. Overwriting items at the
        destination.

        Args:
            source: source path or artefact
            destination: destination path or artefact
            overwrite: Whether to overwrite directories my move

        Returns:
            Artefact: The destination artefact object (source object updated if source was on manager originally)
        """

        # Ensure the destination - get the destination object
        destinationObj, destinationPath = self._artefactFormStandardise(destination)
        if destinationObj:

            # Only work on targets that are on the manager
            if destinationObj.manager is not self:
                raise exceptions.ArtefactNotMember(
                    "Cannot move onto an artefact {} not within manager {}".format(destinationObj, self)
                )

            # There is an artefact at that location that will need to be removed - check if that is allowed
            if isinstance(destinationObj, Directory) and not overwrite:
                raise exceptions.OperationNotPermitted(
                    "Cannot move artefact to location as destination is a directory {} and overwrite has not been toggled".format(
                        destinationObj
                    )
                )

            # Remove the original object
            self._rm(destinationObj.path)
            self._delinkArtefactObjects(destinationObj)

        # Look to see if the source artefact is in the manager - if so we can try to be more efficient
        sourceObject, sourcePath = self._artefactFormStandardise(source)
        if sourceObject is None or sourceObject.manager is not self:
            # The source isn't inside this manager - we must find it and use put. No speed up to be had in manager

            if sourceObject is None:
                # Find the original source object
                sourceObject = self._findArtefact(source)

            artefact = self.put(sourceObject, destinationPath)
            sourceObject.manager.rm(sourceObject)  # Delete the original source as it has been moved
            return artefact

        # The source is on the manager and can be moved with the underlying infrastructure
        self._mv(sourcePath, destinationPath)
        self._moveArtefactObjects(sourceObject, destinationPath)
        return self[destinationPath]

    def rm(self, artefact: typing.Union[Artefact, str], recursive: bool = False) -> None:
        """ Remove an artefact from the manager using the artefact object or its relative path. If its a directory,
        remove it if it is empty, or all of its contents if recursive has been set to true.

        Args:
            artefact (typing.Union[Artefact, str]): the object which is to be deleted
            recursive (bool) = False: whether to accept the deletion of a directory which has contents
        """

        obj, _ = self._artefactFormStandardise(artefact, require=True)

        if obj is None or obj.manager is not self:
            raise exceptions.ArtefactNotMember("Artefact ({}) is not a member of the manager".format(artefact))

        if isinstance(obj, Directory) and len(obj) and not recursive:
            raise exceptions.OperationNotPermitted(
                "Cannot delete a container object that isn't empty - set recursive to True to proceed"
            )

        # Remove the artefact from the manager
        self._rm(obj.path)  # Remove the underlying data objects
        self._delinkArtefactObjects(obj)  # Remove references in the manager and set the objects._exist = False

    def ls(self, art: typing.Union[Directory, str] = '/', recursive: bool = False) -> typing.Set[Artefact]:
        """ List contents of the directory path/artefact given.

        Args:
            art (Directory/str): The Directory artefact or the relpath to the directory to be listed
            recursive (bool) = False: Return subdirectory contents as well

        Returns:
            {Artefact}: The artefact objects which are within the directory
        """

        # Convert the incoming artefact reference - require that the object exist and that it is a directory
        artobj, _ = self._artefactFormStandardise(art, require=True)
        if not isinstance(artobj, Directory):
            raise TypeError("Cannot perform ls action on File artefact: {}".format(artobj))

        # Perform JIT download of directory contents
        if not artobj._collected:
            self._ls(artobj.path)
            artobj._collected = True

        if recursive:

            # Iterate through contents and recursively add lower level artifacts
            contents = set()
            for art in artobj._contents:
                if isinstance(art, Directory): contents |= self.ls(art, recursive)
                contents.add(art)

            # Return all child content
            return contents

        return set(artobj._contents)

    def mkdir(self, path: str, ignoreExists: bool = True, overwrite: bool = False) -> Directory:
        """ Make a directory at the location of the path provided. By default - do nothing in the event that the
        location is already a directory object.

        Args:
            path (str): Relpath to the location where a directory is to be created
            ignoreExists (bool) = True: Whether to do nothing if a directory already exists
            overwrite (bool) = False: Whether to overwrite the directory with an empty directory

        Returns:
            Directory: The directory at the given location - it may have been created as per the call

        Raises:
            OperationNotPermitted: In the event that you try to overwrite a directory that already exists without
                passing the overwrite flag
        """

        if path in self:
            art = self[path]

            if isinstance(art, File):
                raise exceptions.OperationNotPermitted("Cannot make a directory as location {} is a file object".format(path))

            if ignoreExists and not overwrite:
                return art

        with tempfile.TemporaryDirectory() as directory:
            return self.put(directory, path, overwrite=overwrite)

    def touch(self, relpath: str) -> Artefact:
        return self.put(b'', relpath)

    def sync(self, source: typing.Union[Directory, str], destination: typing.Union[Directory, str], overwrite: bool = False, delete: bool = False) -> None:
        """ Put artefacts in the source location into the destination location if they have more recently been editted

        Args:
            source (Directory): source directory artefact
            destination (Directory): destination directory artefact on the manager
            delete: Togger the deletion of artefacts that are members of the destination which do not conflict with
                the source.

        Raises:
            ArtefactNotFound: In the event that the source directory doesn't exist

        """

        # Fetch the destination object - If None, nothing to sync so simply put the source
        destObj, destPath = self._artefactFormStandardise(destination)
        if destObj is None:
            return self.put(source, destPath)

        # Fetch the source object and require that it be an Artefact so we can check object states
        source = self._findArtefact(source)

        # Ensure that the two passed artefacts are directories
        if not (isinstance(source, Directory) and isinstance(destination, Directory)):
            raise exceptions.ArtefactTypeError("Cannot Synchronise non directory objects {} -> {} - must sync directories".format(source, destination))

        # Get the mappings of source artefacts and destination objects
        sourceMapped = {
            source.relpath(artefact): artefact
            for artefact in source.ls(recursive=True)
            if isinstance(artefact, File)
        }

        destinationMapped = {
            destination.relpath(artefact): artefact
            for artefact in destination.ls(recursive=True)
        }

        # Iterate over all the files in the source
        for relpath, sourceArtefact in sourceMapped.items():

            # Look to see if there is a conflict
            if relpath not in destinationMapped:
                # The file doesn't conflict so we will push to destination
                self.put(sourceArtefact, self.join(destination.path, relpath, separator='/'))

            else:
                # There is a conflict - lets compare local and destination
                destinationArtefact = destinationMapped.pop(relpath)

                # Don't perform sync
                if isinstance(destinationArtefact, Directory) and not overwrite:
                    raise exceptions.OperationNotPermitted(
                        "Cannot sync source file {} to destination is a directory {}, and operation not permitted".format(
                            sourceArtefact, destinationArtefact
                        )
                    )

                elif sourceArtefact.modifiedTime > destinationArtefact.modifiedTime:
                    # File is more up to date than destination
                    self.put(sourceArtefact, destinationArtefact)

        # Remove destination artefacts if delete is toggled
        if delete:
            # As updated artefacts were popped during their sync - any left File artefacts are to be deleted

            # Sort to ensure that nested files appear before their directories
            # This allows us to check to see if the directory is empty knowning that everything to be deleted from it
            # has been, as all nested artefacts will appear sooner and be removed before hand.
            for artefact in sorted(destinationMapped.values(), key=lambda x: len(x.path)):

                if isinstance(artefact, File) or (artefact.isEmpty()):
                    # Delete the file or remove the directory if it is now empty
                    self.rm(artefact)

    @contextlib.contextmanager
    def open(self, artefact: typing.Union[File, str], mode: str = "r", **kwargs) -> io.IOBase:
        """ Open a file and create a stream to that file. Expose interface of `open`

        Args:
            artefact: The object that represents the file (or path to the file) to be openned by this manager
            mode: The open method
            kwargs: kwargs to be passed to the interface of open

        Yields:
            io.IOBase: An IO object depending on the mode for interacting with the file
        """

        art, path = self._artefactFormStandardise(artefact)

        if art is None:
            if mode in self._READONLYMODES:
                raise FileNotFoundError('File does not exist in the manager')

        with self.localise(artefact) as abspath:
            with open(abspath, mode, **kwargs) as handle:
                yield handle

    def submanager(self, uri: str):
        """ Create a submanager at the given uri which shall behave like a conventional manager, however, its actions
        shall be relative to the given uri and shall update the main manager.

        If a manager exists at the uri specified already, then it is returned.

        Args:
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
    """ Created by a `Manager` instance to manage a section of the filesystem as if it were a fully fledged manager. The
    interface passes through to owning manager who executes the actions asked to the Sub Manager. Not to be instantiated
    directly or extended.

    Args:
        owner: The manager object this submanager belongs too
        path: The path in the manager the submanager exists
        rootDirectory: The owning managers root artefact object
    """

    def __init__(self, owner: Manager, path: str, rootDirectory: Directory):
        self._root = SubDirectory(self, self._ROOT_PATH, rootDirectory)
        self._path = path
        self._paths = {self._ROOT_PATH: self._root}
        self._submanagers = None

        self._owner = owner

    def __repr__(self):
        return '<SubManager of {} {}>'.format(self._owner, self._path)

    @classmethod
    def _relpath(cls, path: str, target: str):
        relpath = "/" + cls.relpath(path, target)
        if os.name == 'nt':
            relpath = relpath.replace("\\", "/")
        return relpath

    def _join(self, *paths):
        """ Join manager paths with this base manager path for full concrete manager path

        Args:
            *paths: The paths objects to join

        Returns:
            str: the concrete manager path
        """
        return self.join(self._path, *paths, joinAbsolutes=True, separator="/")

    def _cascadeAddArtefact(self, artefact: Artefact):
        # An artefact has either been updated or added to the manager that should also be present in this sub manager

        # Get the new artefact's path
        subpath = self._relpath(artefact.path, self._path)

        if subpath in self._paths:
            # The artefact is represented and out subartefact will already be updated as a result of the parent's actions
            return

        # The artefact is new - we will create the sub artefact objects and add them with the same function as our parents
        # NOTE as there cannot be any submanagers of this sub manager this is not recursively breaking
        if isinstance(artefact, File):
            subArtefact = SubFile(self, subpath, artefact)
        else:
            subArtefact = SubDirectory(self, subpath, artefact)

        super()._addArtefact(subArtefact)

    # Overload the update method - there is no cascade as child artefacts will pull from updated parents
    def _updateArtefactObjects(self, artefact: Artefact):
        return self._owner._updateArtefactObjects(artefact._concrete)

    # Overload the move method
    def _moveArtefactObjects(self, source: Artefact, destination: str):
        return self._owner._moveArtefactObjects(source._concrete, self._join(destination))

    def _cascadeMoveArtefactObjects(self, source: str, destination: str):
        # Run the parent move function on the subartefacts
        super()._moveArtefactObjects(self._paths[self._relpath(source, self._path)], self._relpath(destination, self._path))

    # Overload the delink method
    def _delinkArtefactObjects(self, artefact: Artefact):
        return self._owner._delinkArtefactObjects(artefact._concrete)
    def _cascadeDelinkArtefactObjects(self, artefact: str):
        return super()._delinkArtefactObjects(self._paths[self._relpath(artefact, self._path)])

    def _abspath(self, managerPath):
        return self._owner._abspath(self._join(managerPath))

    def _makeFile(self, managerPath: str) -> File:
        mainArt = self._owner._makeFile(self._join(managerPath))
        return SubFile(self, managerPath, mainArt)

    def _makeDirectory(self, managerPath: str) -> Directory:
        mainDirectory = self._owner._makeDirectory(self._join(managerPath))
        return SubDirectory(self, managerPath, mainDirectory)

    def _identifyPath(self, managerPath: str) -> typing.Union[Artefact, None]:
        mainCheck = self._owner._identifyPath(self._join(managerPath))

        if isinstance(mainCheck, File):
            return SubFile(self, managerPath, mainCheck)

        elif isinstance(mainCheck, Directory):
            return SubDirectory(self, managerPath, mainCheck)

        else:
            return mainCheck

    def _loadArtefact(self, managerPath: str) -> Artefact:
        self._owner._loadArtefact(self._join(managerPath))
        return self._paths[managerPath]

    def _addArtefact(self, artefact: Artefact):
        # Add the concrete artefact to the owning manager - add to local first to ensure that no new sub artefact is
        # created
        super()._addArtefact(artefact)
        self._owner._addArtefact(artefact._concrete)

    def _get(self, source: str, destination: str):
        return self._owner._get(self._join(source), destination)

    def _getBytes(self, source: str) -> bytes:
        return self._owner._getBytes(self._join(source))

    def _put(self, source: str, destination: str):
        self._owner._put(source, self._join(destination))

    def _putBytes(self, fileBytes: bytes, destination: str):
        self._owner._putBytes(fileBytes, self._join(destination))

    def _cp(self, source: str, destination: str):
        return self._owner._cp(self._join(source), self._join(destination))

    def _mv(self, source: str, destination: str):
        return self._owner._mv(self._join(source), self._join(destination))

    def _rm(self, artefact: str):
        self._owner._rm(self._join(artefact))

    def _ls(self, directory: str):
        return self._owner._ls(self._join(directory))

    @contextlib.contextmanager
    def localise(self, artefact: typing.Union[Artefact, str]):
        with type(self._owner).localise(self, artefact) as abspath:
            yield abspath

    def submanager(self):
        raise NotImplementedError("A submanager cannot be created on a submanager")

    @classmethod
    def _signatureFromURL(cls, url: urllib.parse.ParseResult):
        raise NotImplementedError("Cannot load a submanager from a protocol string")

    def toConfig(self) -> dict:
        config = self._owner.toConfig()

        # Add this managers uri as submanager point
        config["submanager"] = self._path

        return config

class LocalManager(Manager, abc.ABC):
    """ Abstract Base Class for managers that will be working with local artefacts.
    """

    @contextlib.contextmanager
    def localise(self, artefact):
        obj, path = self._artefactFormStandardise(artefact)
        exception = None

        abspath = self._abspath(path)
        os.makedirs(os.path.dirname(abspath), exist_ok=True)

        try:
            yield abspath
        except Exception as e:
            exception = e

        if obj is not None:
            # Update the original artefact with the changes
            self._updateArtefactObjects(obj)

        else:
            # Create a new artefact for the artefact
            self._loadArtefact(path)

        if exception:
            raise exception

class RemoteManager(Manager):
    """ Abstract Base Class for managers that will be working with remote artefacts so efficiency with fetching and
    pushing files is important for time and bandwidth
    """

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
                # No checksum for no object
                checksum = None

            # Return the local path to the object
            try:
                yield local_path
            except Exception as e:
                exception = e

            # The user has stopped interacting with the artefact - resolve any differences with manager
            if checksum:
                if os.path.isdir(local_path):
                    # Compare the new hiearchy - update only affected files/directories
                    put, delete = self._compareHierarhy(checksum, self._parseHierarchy(local_path))

                    # Define the method for converting the abspath back to the manager relative path
                    contexualise = lambda x: self.join(path, x[len(local_path)+1:], separator='/')

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
