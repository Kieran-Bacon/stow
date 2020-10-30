import os
from abc import ABC, abstractmethod, abstractclassmethod
import typing
import shutil
import tempfile
import contextlib
import hashlib
import re
import io
import urllib.parse

from . import exceptions
from .artefacts import Artefact, File, Directory, SubFile, SubDirectory

import logging
log = logging.getLogger(__name__)

class Manager(ABC):
    """ Manager Abstract base class - expressed the interface of a Manager which governs a storage option and allows
    extraction and placement of files in that storage container

    """

    SUPPORTS_UNICODE_FILENAMES = os.path.supports_unicode_filenames



    def toConfig(): pass

    _ROOT_PATH = "/"
    _PLACEHOLDER = "placeholder.ignore"
    _READONLYMODES = ["r", "rb"]

    _MULTI_SEP_REGEX = re.compile(r"(\\{2,})|(\/{2,})")
    _RELPATH_REGEX = re.compile(r"^([a-zA-Z0-9]+:)?([/\\\w\.!-_*'() ]*)$")

    def __init__(self):
        self._root = Directory(self, self._ROOT_PATH)
        self._paths = {self._ROOT_PATH: self._root}
        self._submanagers = {}

    @abstractmethod
    def __repr__(self): pass
    def __contains__(self, artefact: typing.Union[Artefact, str]) -> bool:
        if isinstance(artefact, Artefact):
            return self is artefact.manager
        else:
            return self.exists(artefact)
    def __getitem__(self, managerPath: str) -> Artefact:
        """ Fetch an artefact from the manager. In the event that it hasn't been cached, look it up on the underlying
        implementation and return a newly created object. If it doesn't exist raise an error

        Args:
            managerPath: The manager relative path to fine the artefact with

        Returns:
            artefact: The artefact at the provided location path

        Raises:
            ArtefactNotFound: In the event that the path does not exist
        """

        if managerPath in self._paths:
            return self._paths[managerPath]

        obj = self._identifyPath(managerPath)
        if obj is None:
            raise exceptions.ArtefactNotFound("Couldn't locate artefact {}".format(managerPath))

        return obj

    @staticmethod
    def _splitArtefactUnionForm(artefact: typing.Union[Artefact, str]) -> typing.Tuple[typing.Union[Artefact, None], str]:
        """ Take an artefact or a string and return in a strict format the object and string representation. This allows
        methods to accept both and resolve and ensure.

        Only the path is guaranteed, the artefact object will be None if it is not passed

        Args:
            artefact: Type unknown, artefact object or path

        Returns:
            artefact: An artefact object or None
            path: the path passed or pull from the artefact object

        """
        if isinstance(artefact, Artefact):
            return artefact, artefact.path

        return None, artefact


    @classmethod
    def abspath(cls, artefact: typing.Union[Artefact, str]) -> str:
        """ Return a normalized absolute version of the path or artefact given.

        Args:
            artefact: The path or object whose path is to be made absolute and returned

        Returns:
            str: the absolute path of the artefact provided

        Raises:
            ValueError: Cannot make a remote artefact object's path absolute
        """
        if isinstance(artefact, Artefact):
            if isinstance(artefact.manager, LocalManager):
                return os.path.abspath(artefact.path)

            else:
                raise ValueError("Cannot get absolute path for remote artefact {}".format(artefact))

        else:
            return os.path.abspath(artefact)

    @classmethod
    def basename(cls, artefact: typing.Union[Artefact, str]) -> str:
        """ Return the base name of an artefact or path. This is the second element of the pair returned by passing path
        to the function `split()`.

        Args:
            artefact: The path or object whose path is to have its base name extracted

        Returns:
            str: the basename
        """
        _, path = cls._splitArtefactUnionForm(artefact)
        return os.path.basename(path)

    @classmethod
    def commonpath(cls, paths: typing.Iterable[typing.Union[Artefact, str]]) -> str:
        """ Return the longest common sub-path of each pathname in the sequence paths

        Examples:
            commonpath(["/foo/bar", "/foo/ban/pip"]) == "/foo"

        Args:
            paths: Artefact/paths who will have their paths comparied to find a common path

        Returns:
            str: A valid owning directory path that is the shared owning directory for all paths

        Raises:
            ValueError: If there is no crossover at all
        """
        return os.path.commonpath([cls._splitArtefactUnionForm(path)[1] for path in paths])

    @classmethod
    def commonprefix(cls, paths: typing.Iterable[typing.Union[Artefact, str]]) -> str:
        """ Return the longest common string literal for a collection of path/artefacts

        Examples:
            commonpath(["/foo/bar", "/foo/ban/pip"]) == "/foo/ba"

        Args:
            paths: Artefact/paths who will have their paths comparied to find a common path

        Returns:
            str: A string that all paths startwith (may be empty string)
        """
        return os.path.commonprefix([cls._splitArtefactUnionForm(path)[1] for path in paths])

    @classmethod
    def dirname(cls, artefact: typing.Union[Artefact, str]) -> str:
        """ Return the directory name of path or artefact. Preserve the protocol of the path if a protocol is given

        Args:
            artefact: The artefact or path whose directory path is to be returned

        Returns:
            str: The directory path for the holding directory of the artefact
        """
        obj, path = cls._splitArtefactUnionForm(artefact)

        if obj or path.index(":") == -1:
            # Obj path or path within no protocol and therefore no need to parse
            return os.path.dirname(path)

        else:
            # Preserve protocol (if there is one) - dirname the path
            result = urllib.parse.urlparse(artefact)
            return urllib.parse.ParseResult(
                result.scheme,
                result.netloc,
                os.path.dirname(result.path),
                result.params,
                result.query,
                result.fragment
            ).geturl()

    def exists(self, artefact: typing.Union[Artefact, str]) -> str:
        """ Return true if the given artefact is a member of the manager, or the path is correct for the manager and it
        leads to a File or Directory.

        Does not handle protocols

        Args:
            artefact: Artefact or path whose existence is to be checked

        Returns:
            bool: True if artefact exists else False
        """
        # Only split as we want to trigger reassessment of the underlying file
        _, path = self._splitArtefactUnionForm(artefact)

        # If not none then its a valid object - any updates will have taken place
        return self._identifyPath(path) is not None

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

    @staticmethod
    def expanduser(path: str) -> str:
        """ On Unix and Windows, return the argument with an initial component of ~ or ~user replaced by that user’s
        home directory.

        On Unix, an initial ~ is replaced by the environment variable HOME if it is set; otherwise the current user’s
        home directory is looked up in the password directory through the built-in module pwd. An initial ~user is
        looked up directly in the password directory.

        On Windows, USERPROFILE will be used if set, otherwise a combination of HOMEPATH and HOMEDRIVE will be used.
        An initial ~user is handled by stripping the last directory component from the created user path derived above.

        If the expansion fails or if the path does not begin with a tilde, the path is returned unchanged.

        Args:
            path: the path which may contain a home variable indicator to be expanded

        Returns:
            str: A path with the home path factored in - if applicable
        """
        return os.path.expanduser(path)

    @staticmethod
    def expandvars(path: str):
        """ Return the argument with environment variables expanded. Substrings of the form $name or ${name} are
        replaced by the value of environment variable name. Malformed variable names and references to non-existing
        variables are left unchanged.

        On Windows, %name% expansions are supported in addition to $name and ${name}.

        Args:
            path: A path which might contain variables to be expanded

        Returns:
            str: A string with any environment variables added
        """
        return os.path.expandvars(path)

    @staticmethod
    def isabs(path: str) -> bool:
        """ Return True if path is an absolute pathname.
        On Unix, that means it begins with a slash,
        on Windows that it begins with a (back)slash after chopping off a potential drive letter.

        Args:
            path: the path to be checked for being absolute
        """
        return os.path.isabs(path)

    @classmethod
    def join(cls, *paths: typing.Iterable[str], separator=os.sep) -> str:
        """ Join one or more path components intelligently. The return value is the concatenation of path and any
        members of *paths with exactly one directory separator following each non-empty part except the last,
        meaning that the result will only end in a separator if the last part is empty. If a component is an absolute
        path, all previous components are thrown away and joining continues from the absolute path component.

        Protocols/drive letters are perserved in the event that an absolute is passed in.

        Args:
            *paths: segments of a path to be joined together
            separator: The character to be used to join the path segments

        Returns:
            str: A joined path
        """
        if not paths:
            return ""

        parsedResult = None
        joined = ""
        totalLength = len(paths)

        for i, segment in enumerate(paths):

            # Identify and record the last full
            presult = urllib.parse.urlparse(segment)
            if presult.scheme:
                parsedResult = presult

            # Delete previous joined segments as new absolute path is provided
            if cls.isabs(segment):
                joined = ""

            # Add the segment and a seperator
            joined += segment
            if i != totalLength:
                joined += separator

        if parsedResult:
            return urllib.parse.ParseResult(
                parsedResult.schema,
                parsedResult.netloc,
                joined,
                parsedResult.params,
                parsedResult.query,
                parsedResult.fragment
            )

        return joined

    @staticmethod
    def normcase(path: str) -> str:
        """ Normalize the case of a pathname. On Windows, convert all characters in the pathname to lowercase, and also
        convert forward slashes to backward slashes. On other operating systems, return the path unchanged.

        Args:
            path: path to normalise

        Returns:
            str: the path normalised
        """
        return os.path.normcase(path)

    @staticmethod
    def normpath(path: str) -> str:
        """ Normalize a pathname by collapsing redundant separators and up-level references so that A//B, A/B/, A/./B
        and A/foo/../B all become A/B.

        Args:
            path: the path whose to be

        Returns:
            str: The path transformed
        """
        # Check that the url is for a remote manager
        url = urllib.parse.urlparse(path)
        if url.scheme and url.netloc:
            # URL with protocol
            return urllib.parse.ParseResult(
                url.scheme,
                url.netloc,
                os.path.normpath(url.path).replace("\\", "/"),
                url.params,
                url.query,
                url.fragment
            )

        # Apply the normal path - method to the path
        return os.path.normpath(path)

    @classmethod
    def realpath(cls, path: str) -> str:
        """ Return the canonical path of the specified filename, eliminating any symbolic links encountered in the path
        (if they are supported by the operating system).

        Args:
            path: the path to have symbolic links corrected

        Returns:
            str: the path with the symbolic links corrected
        """
        return os.path.realpath(path)

    @classmethod
    def relpath(cls, path: str, start=os.curdir) -> str:
        """ Return a relative filepath to path either from the current directory or from an optional start directory

        Args:
            path: the path to be made relative
            start: the location to become relative to
        """
        return os.path.relpath(path, start)

    @classmethod
    def savefile(cls, artefact1: typing.Union[Artefact, str], artefact2: typing.Union[Artefact, str]) -> bool:
        """ Check if provided artefacts are represent the same data on disk

        Args:
            artefact1: An artefact object or path
            artefact2: An artefact object or path

        Returns:
            bool: True if the artefacts are the same
        """
        obj1, path1 = cls._splitArtefactUnionForm(artefact1)
        obj2, path2 = cls._splitArtefactUnionForm(artefact2)

        if obj1 is None or obj2 is None:
            return os.path.samefile(path1, path2)

        else:
            return obj1 is obj2

    @classmethod
    def sameopenfile(handle1: io.IOBase, handle2: io.IOBase) -> bool:
        """ Return True if the file descriptors fp1 and fp2 refer to the same file.
        """
        return os.path.sameopenfile(handle1, handle2)

    @classmethod
    def samestat(cls, artefact1: typing.Union[Artefact, str], artefact2: typing.Union[Artefact, str]) -> bool:
        """ Check if provided artefacts are represent the same data on disk

        Args:
            artefact1: An artefact object or path
            artefact2: An artefact object or path

        Returns:
            bool: True if the artefacts are the same
        """
        _, path1 = cls._splitArtefactUnionForm(artefact1)
        _, path2 = cls._splitArtefactUnionForm(artefact2)

        return os.path.samestat(path1, path2)

    @classmethod
    def split(cls, artefact: typing.Union[Artefact, str]) -> typing.Tuple[str, str]:
        """ Split the pathname path into a pair, (head, tail) where tail is the last pathname component and head is
        everything leading up to that.

        Args:
            artefact: the artefact to be split

        Returns:
            (dirname, basename): the split parts of the artefact
        """

        _, path = cls._splitArtefactUnionForm(artefact)
        return (cls.dirname(path), cls.basename(path))

    @classmethod
    def splitdrive(cls, path: str) -> typing.Tuple[str, str]:
        """ Split the pathname path into a pair (drive, tail) where drive is either a mount point or the empty string.

        Args:
            path: the path whose mount point/drive is to be removed

        Returns:
            (drive, path): tuple with drive string separated from the path
        """

        return os.path.splitdrive(path)

    @classmethod
    def splitext(cls, artefact: typing.Union[Artefact, str]) -> typing.Tuple[str, str]:
        """ Split the pathname path into a pair (root, ext) such that root + ext == path, and ext is empty or begins
        with a period and contains at most one period.

        Args:
            artefact: the artefact to have the extension extracted

        Returns:
            (root, ext): The root path without the extension and the extension
        """
        _, path = cls._splitArtefactUnionForm(artefact)
        return os.path.splitext(path)

    @abstractmethod
    def _abspath(self, managerPath):
        """ Return the most accurate path to an object in the managers vernacular. Opposite of relpath

        examples:
            local managers shall convert a relative path to its full absolute os compatible filepath
            s3 shall convert the relative path to a s3 valid key

        Args:
            artefact (str): The artefact object or it's relative path which is to be converted
        """
        pass

    @abstractmethod
    def _makeFile(self, abspath: str) -> File:
        """ Make a file object using the underlying implementation objects from a manager relative path

        Args:
            abspath: Manager absolute path

        Returns:
            File: The File object representing the on disk data object
        """
        pass

    def _makeDirectory(self, managerPath: str) -> Directory:
        """ Fetch the owning `container` for the manager relative path given. In the event that no `container` object
        exists for the path, create one and recursively find its owning `container` to add it to. The goal of this
        function is to traverse up the hierarchy and ensure all the directory objects exist, and when they do quickly
        return the container they are in

        Args:
            path (str): The manager relative path for an `Artefact`

        Returns:
            Directory: The owning directory container, which may have just been created
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
        art = Directory(self, managerPath)
        self._addArtefact(art)  # Link it with any owner + submanagers
        return art

    @abstractmethod
    def _get(self, source: Artefact, destination: str):
        """ Fetch the artefact and downloads its data to the local destination path provided

        The existence of the file to collect has already been checked so this function can be written to assume its
        existence

        Args:
            source: The source object and context that is to be downloaded
            destination: The local path to where the source is to be written
        """
        pass

    @abstractmethod
    def _getBytes(self, source: File) -> bytes:
        """ Fetch the file artefact contents directly. This is to avoid having to write the contents of files to discs
        for some of the other operations.

        The existence of the file to collect has already been checked so this function can be written to assume its
        existence

        Args:
            source: The source object and context that is to be downloaded

        Returns:
            bytes: The bytes content of the disk
        """
        pass

    @abstractmethod
    def _put(self, source: str, destination: str):
        """ Put the local filesystem object onto the underlying manager implementation using the absolute paths given.

        To avoid user error - an artefact cannot be placed onto a Directory unless an overwrite toggle has been passed
        which is False by default. This should protect them from accidentally deleting a directory.

        In the event that they want to do so - the deletion of the directory will be handled before operating this
        function. Therefore their is no need to check/protect against it. (famous last words)

        Args:
            source: A local path to an artefact (File or Directory)
            destination: A manager abspath path for the artefact
        """
        pass

    @abstractmethod
    def _putBytes(self, fileBytes: bytes, destination: str):
        """ Put the bytes of a file object onto the underlying manager implementation using the absolute path given.

        This function allows processes to avoid writing files to disc for speedier transfers.

        If its not possible to transmit bytes - I'd recommend writing the bytes to a tempfile and then operating the
        put method.

        Args:
            fileBytes (bytes): files bytes
            destinationAbsPath (str): Remote absolute path
        """
        pass

    @abstractmethod
    def _cp(self, source: Artefact, destination: str):
        """ Method for copying an artefact local to the manager to an another location on the manager. Implementation
        would avoid having to download data from a manager to re-upload that data.

        If there isn't a method of duplicating the data on the manager, you can call
            self._put(self._abspath(source.path), destination)

        Which will mean the behaviour defaults to the put action.

        Args:
            source: the manager local source file
            destination: a manager abspath path for destination
        """
        pass

    @abstractmethod
    def _mv(self, source: Artefact, destination: str):
        """ Method for moving an artefact local to the manager to an another location on the manager. Implementation
        would avoid having to download data from a manager to re-upload that data.

        If there isn't a method of duplicating the data on the manager, you can call
            self._put(self._abspath(source.path), destination)
            self._rm(self._abspath(source.path))

        Which will mean the behaviour defaults to the put action and then a delete of the original file. Achieving the
        same goal.

        Args:
            source: the manager local source file
            destination: a manager abspath path for destination
        """
        pass

    @abstractmethod
    def _ls(self, managerPath: str) -> typing.Iterable[Artefact]:
        """ List and convert to Artefact objects, the contents of the directory at the managerPath location

        The existence of the directory has already been confirmed.

        This method can be used in conjunction with self._makeFile and self._makeDirectory to great affect:

        1. You can list the items in the directory and call makeFile and makeDirectory on them and collect
        created objects to be returned or

        1. Have ls add all files and directories when called (good when you can download multiple metadata at once for
        no cost) and then have makeFile call ls on its parent directory before hand so that it can return the created
        file object by ls.

        Food for thought.

        Args:
            managerPath: the manager abspath to the directory whose content is to be indexed

        Returns:
            typing.Iterable[Artefact]: All of the artefact objects inside the directory
        """
        pass

    @abstractmethod
    def _rm(self, artefact: Artefact):
        """ Delete the underlying artefact data on the manager.

        To avoid possible user error in deleting directories, the user must have already indicated that they want to
        delete everything

        Args:
            artefact: The artefact on the manager to be deleted
        """
        pass

    @abstractclassmethod
    def _loadFromProtocol(cls, url: urllib.parse.ParseResult):
        """ Create a new instance of the manager using the information passed via the url ParseResult object that will
        have been created via the stateless interface

        Args:
            url: The result of passing the stateless path through urllib.parse.urlparse

        Returns:
            Manager: A manager of this type loaded with information from the url

        Raises:
            Error: Errors due to missing information and so on
        """
        pass

    def _addArtefact(self, artefact: Artefact):
        """ Add an artefact object into the manager data structures - do not add if the object has already been added

        Args:
            artefact: The artefact object to be added

        Raises:
            ArtefactNotMember: in the event that the artefact that is trying to be added was not created by this manager
        """

        if artefact.manager is not self:
            raise exceptions.ArtefactNotMember("Artefact {} is not a member of {} and couldn't be added".format(artefact, self))

        if artefact.path in self._path:
            # The artefact was already added to the manager
            log.warning("Artefact {} was previously been added - no action being taken".format(artefact))
            return

        # Get the directory for the artefact
        directory = self._makeDirectory(self.dirname(artefact))
        directory._add(artefact)

        # Add the artefact into the manager store
        self._paths[artefact.path] = artefact

        if self._submanagers:
            # Ensure that the artefact has been added to any sub managers this artefact resides in

            for uri, manager in self._submanagers.items():
                if artefact.path.startswith(uri):
                    # The artefact exists within the sub manager - pass the parent object
                    manager._addMain(artefact)

    def _artefactFormStandardise(self, artObj: typing.Union[Artefact, str], require=False) -> typing.Tuple[Artefact, str]:
        """ Convert the incoming object which could be either an artefact or relative path into a standardised form for
        both such that functions can be easily convert and use what they require

        Args:
            artObj (typing.Union[Artefact, str]): Either the artefact object or it's relative path to be standardised
            require (str): Require that the object exists. when false return None for yet to be created objects but

        Returns:
            Artefact or None: the artefact object or None if it doesn't exists and require is False
            str: The relative path of the object/the passed value
        """
        if isinstance(artObj, Artefact):
            return artObj, artObj.path

        else:
            # A path was given
            if require or artObj in self:
                # TODO should this convert the path to be manager clean?
                return self[artObj], artObj

            # Only return path - not attempting to fetch artefact
            return None, artObj

    def _updateArtefactObjects(self, artefact: Artefact):
        """ Perform a update for the manager on the contents of a directory which has been editted on mass and whose
        content is likely inconsistent with the current state of the manager. Only previously known files are checked as
        new files are to be loaded JIT and can be added at that stage.

        Args:
            artobj (Directory): The directory to perform the refresh on
        """

        if isinstance(artefact, File):
            file = self._identifyPath(artefact.path)
            if file is None:
                self._delinkArtefactObjects(artefact)

            artefact._update(file)

        else:
            artefact: Directory

            # For the artefacts we know about - check their membership
            for artefact in artefact._contents:

                # Have the path checked
                check = self._identifyPath(artefact.path)

                # Update the artefact according to its state on disc
                if check is None or type(artefact) != type(check):
                    # The artefact has been deleted or the type of the artefact has changed - it needs to be delinked
                    self._delink(artefact)

                elif isinstance(artefact, (File, SubFile)):
                    # Update the artefact with the informant as we've pulled it
                    artefact._update(check)

                else:
                    # The directory needs to be checked for issues
                    self._updateDirectory(artefact)

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
                art._path = self.join(destPath, srcObj.relpath(art))

                # Update its membership
                self._paths[art.path] = art

        # Check whether the object has moved outside of the directory it was originally in
        if self.dirname(srcObj.path) != self.dirname(destPath):
            # Disconnect object with the directories that it exists in and add it to the destination location
            srcObj.directory._remove(srcObj)
            self._makeDirectory(self.dirname(destPath))._add(srcObj)

        # Move and rename the object
        del self._paths[srcObj.path]
        self._paths[destPath] = srcObj

        # Update the artefacts info
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

    def _delinkArtefactObjects(self, artefact: Artefact):
        """ Unreference an artefact from the manager but do not check against/remove objects from the underlying
        implementation. This is to be used in conjunction with `_rm()` or to clean up artefacts that could have been
        affected as a side effect

        Args:
            artefact (Artefact): Manager artefact that is to be deleted
        """
        if isinstance(artefact, Directory):
            # NOTE we avoid calling this function recursively to avoid issues with of removing directories
            # and their subelements. Additionally as the directories keep weakreferences to their contents items will
            # not keep each other alive and shall be removed when the GC deems it apprioprate
            for art in artefact._contents:
                del self._paths[art.path]
                art._exists = False

        # Delete references to the object and set it's existence to false
        self[self.dirname(artefact.path)]._delink(artefact)
        del self._paths[artefact.path]
        artefact._exists = False

        if self._submanagers:
            for uri, manager in self._submanagers.items():
                if artefact.path.startswith(uri):
                    manager._removeMain(artefact)

    def get(self, src_remote: typing.Union[Artefact, str], dest_local: str, overwrite: bool = False) -> Artefact:
        """ Get a remote artefact from the storage option and write it to the destination path given.

        Args:
            src_remote (Artefact/str): The remote's file object or its path
            dest_local (str): The local path for the artefact to be written to
        """

        # Split into object and path - Ensure that the artefact to get is from this manager
        obj, _ = self._artefactFormStandardise(src_remote, require=True)
        if obj.manager is not self:
            raise exceptions.ArtefactNotMember("Provided artefact is not a member of the manager")

        # Remove or raise issue for a local artefact at the location where the get is called
        if os.path.exists(dest_local):
            if os.path.isdir(dest_local):
                if overwrite:
                    shutil.rmtree(dest_local)

                else:
                    raise exceptions.OperationNotPermitted(
                        "Cannot fetch artefact ({}) to replace local directory "
                        "({}) unless overwrite argument is set to True".format(obj, dest_local)
                    )

            else:
                os.remove(dest_local)

        else:
            # Ensure the directory that this object exists with
            destinationDirectory = os.path.dirname(dest_local)
            os.makedirs(destinationDirectory, exist_ok=True)

        # Fetch the object and place it at the location
        return self._get(obj, dest_local)


    def _putArtefact(
        self,
        source: typing.Union[str, bytes],
        destinationArtifact: Artefact,
        destinationPath: str,
        overwrite: bool = False
        ) -> Artefact:

        # Clean up any files that currently exist at the location
        if destinationArtifact is not None:

            if isinstance(destinationArtifact, File) or (overwrite):
                # Remove the destination object
                self._rm(destinationArtifact)

            else:
                raise exceptions.OperationNotPermitted(
                    "Cannot put {} as destination is a directory, and overwrite has not been set to True"
                )

        # Put the local artefact onto the remote using the manager definition
        if isinstance(source, str):
            # The artefact is a local object is persistent storage
            self._put(source, self._abspath(destinationPath))

            # Extract the artefact depending on the type of input
            if os.path.isdir(source):
                # Source is a directory

                if destinationArtifact is not None:
                    # An object original existed - identify type of object and handle accordingly

                    if isinstance(destinationArtifact, Directory):
                        # The original object was a directory - compare downloaded objects with objects to remove no longer
                        # present files and update files to the newly uploaded content
                        self._updateDirectory(destinationArtifact)
                        return destinationArtifact

                    else:
                        # File is being replaced with a directory - delete the file and create a new directory object
                        self._delink(destinationArtifact)

                return self._makeDirectory(destinationPath)

        else:
            # The artefact is a file binary
            self._putBytes(source, self._abspath(destinationPath))

        # File has been put onto the remote - get remote and update the state of the manager accordingly
        art = self._makeFile(destinationPath)

        # Update any artefacts that were previously at the location
        if destinationArtifact is not None:
            if isinstance(destinationArtifact, File):
                # The artefact has overwritten a previous file - update it and return it
                original = self._paths[destinationPath]
                original._update(art)
                return original

            else:
                # The artefact has overwritten a directory - all artefacts below this root have been deleted
                self._delink(destinationArtifact)

        # Add the new artefact and return it
        self._add(art)
        return art

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

        # Verify that the destination is valid
        if isinstance(destination, Artefact):
            destinationArtifact, destinationPath = destination, destination.path

            # The destination isn't within the manager
            if destinationArtifact.manager is not self:
                raise exceptions.ArtefactNotMember("Destination artefact is not a member of the manager")

        else:
            # Destination is a path
            if destination[-1] == "/":

                # Destination is inside the directory string passed
                destinationArtifact, _ = self._artefactFormStandardise(destination[:-1])
                destination = self.join(destination, os.path.basename(source))

                # If the object turns out to be a file then we can't place the object beneath it
                if destinationArtifact is not None and isinstance(destinationArtifact, File):
                    raise exceptions.InvalidPath("File object cannot act as directory for object being put")

            # Get the destination Artefact and rel path
            destinationArtifact, destinationPath = self._artefactFormStandardise(destination)

        # Ensure the source artefact - get a local path to the source for putting into this manager
        if isinstance(source, Artefact):
            with source.manager.localise(source) as sourceAbsPath:
                return self._putArtefact(sourceAbsPath, destinationArtifact, destinationPath, overwrite=overwrite)

        else:
            # The source is a local filepath or byte stream
            return self._putArtefact(source, destinationArtifact, destinationPath, overwrite=overwrite)

    def cp(self, source: typing.Union[Artefact, str], destination: typing.Union[Artefact, str], overwrite: bool = False):
        """ Copy the artefacts at the source location to the provided destination location. Overwriting items at the
        destination.

        Args:
            source: source path or artefact
            destination: destination path or artefact
            overwrite: Whether to overwrite directories my move
        """

        # Look to see if the source artefact is in the manager - if so we can try to be more efficient
        srcObj, srcPath = self._artefactFormStandardise(source)
        if (srcObj and srcObj.manager is not self) or srcObj is None:
            # The source cannot be from the manager and therefore there isn't an improvement that can be made over put.
            # NOTE the put method for local files is a copy
            self.put((srcObj or srcPath), destination)
            return

        # Ensure that the target location is somewhere we can copy to
        destObj, destPath = self._artefactFormStandardise(destination)
        if destObj:

            # Only work on targets that are on the manager
            if destObj.manager is not self:
                raise exceptions.ArtefactNotMember(
                    "Cannot copy onto an artefact {} not within manager {}".format(destObj, self)
                )

            # There is an artefact at that location that will need to be removed - check if that is allowed
            if isinstance(destObj, Directory) and not overwrite:
                raise exceptions.OperationNotPermitted(
                    "Cannot copy artefact {} as destination is a directory {} and overwrite has not been toggled".format(srcObj, destObj)
                )

            # Delete directly as we have verified that the item is acceptable for deletion
            self._rm(destObj)
            self._delink(destObj)

        # We must be an artefact on the box copying to another location on the box - destination is clear
        return self._cp(self._abspath(srcObj), self._abspath(destPath))

    def mv(self, source: typing.Union[Artefact, str], destination: typing.Union[Artefact, str], overwrite: bool = False):
        """ Copy the artefacts at the source location to the provided destination location. Overwriting items at the
        destination.

        Args:
            source: source path or artefact
            destination: destination path or artefact
            overwrite: Whether to overwrite directories my move
        """

        # Look to see if the source artefact is in the manager - if so we can try to be more efficient
        srcObj, srcPath = self._artefactFormStandardise(source)
        if (srcObj and srcObj.manager is not self) or srcObj is None:
            # The source cannot be from the manager and therefore there isn't an improvement that can be made over put.
            # NOTE the put method for local files is a copy
            self.put((srcObj or srcPath), destination)
            srcObj.manager.rm(srcObj)
            return

        # Ensure that the target location is somewhere we can copy to
        destObj, destPath = self._artefactFormStandardise(destination)
        if destObj:

            # Only work on targets that are on the manager
            if destObj.manager is not self:
                raise exceptions.ArtefactNotMember(
                    "Cannot copy onto an artefact {} not within manager {}".format(destObj, self)
                )

            # There is an artefact at that location that will need to be removed - check if that is allowed
            if isinstance(destObj, Directory) and not overwrite:
                raise exceptions.OperationNotPermitted(
                    "Cannot copy artefact {} as destination is a directory {} and overwrite has not been toggled".format(srcObj, destObj)
                )

            # Delete directly as we have verified that the item is acceptable for deletion
            self._rm(destObj)
            self._delink(destObj)

        # We must be an artefact on the box copying to another location on the box - destination is clear
        self._mv(self._abspath(srcObj), self._abspath(destPath))
        self._moveArtefactObjects(srcObj, destPath)

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
        self._rm(obj)  # Remove the underlying data objects
        self._delinkArtefactObjects(obj)  # Remove references in the manager and set the objects._exist = False

    def ls(self, art: typing.Union[Directory, str] = '/', recursive: bool = False) -> typing.Set[Artefact]:
        """ List contents of the directory path/artefact given.

        Args:
            art (Directory/str): The Directory artefact or the relpath to the directory to be listed
            recursive (bool) = False: Return subdirectory contents aswell

        Returns:
            {Artefact}: The artefact objects which are within the directory
        """

        # Convert the incoming artefact reference - require that the object exist and that it is a directory
        artobj, _ = self._artefactFormStandardise(art, require=True)
        if not isinstance(artobj, Directory):
            raise TypeError("Cannot perform ls action on File artefact: {}".format(artobj))

        # Perform JIT download of directory contents
        if not artobj._collected:
            self._ls(artobj)
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

    def mkdir(self, path: str, ignoreExists: bool = True, overwrite: bool = False):
        """ Make a directory at the location of the path provided. By default - do nothing in the event that the
        location is already a directory object.

        Args:
            path (str): Relpath to the location where a directory is to be created
            ignoreExists (bool) = True: Whether to do nothing if a directory already exists
            overwrite (bool) = False: Whether to overwrite the directory with an empty directory
        """

        if path in self:
            if isinstance(self[path], File):
                raise exceptions.OperationNotPermitted("Cannot make a directory as location {} is a file object".format(path))

            if ignoreExists and not overwrite:
                return

        with tempfile.TemporaryDirectory() as directory:
            return self.put(directory, path, overwrite=overwrite)

    def touch(self, relpath: str) -> Artefact:
        with tempfile.TemporaryDirectory() as directory:
            emptyFile = os.path.join(directory, 'empty_file')
            open(emptyFile, 'w').close()
            return self.put(emptyFile, relpath)

    def sync(self, source: Directory, destination: Directory, delete: bool = False) -> None:
        """ Put artefacts in the source location into the destination location if they have more recently been editted

        Args:
            source (Directory): source directory artefact
            destination (Directory): destination directory artefact on the manager
            delete: Togger the deletion of artefacts that are members of the destination which do not conflict with
                the source.
        """

        if not (isinstance(source, Directory) and isinstance(destination, Directory)):
            raise exceptions.ArtefactTypeError("Cannot Synchronise non directory objects {} -> {} - must sync directories".format(source, destination))

        # Get the mappings of source artefacts and destination objects
        sourceMapped = {source.relpath(artefact): artefact for artefact in source.ls(recursive=True) if isinstance(artefact, File)}
        destinationMapped = {source.relpath(artefact): artefact for artefact in destination.ls(recursive=True)}

        # Iterate over all the files in the source
        for relpath, sourceArtefact in sourceMapped.items():

            # Look to see if there is a conflict
            if relpath not in destinationMapped:
                # The file doesn't conflict so we will push to destination
                self.put(sourceArtefact, self.join(destination, relpath))

            else:
                # There is a conflict - lets compare local and destination
                destinationArtefact = destinationMapped.pop(relpath)

                if isinstance(destinationArtefact, Directory):
                    raise OperationNotPermitted("Cannot sync source file {} as destination is a directory {}".format(sourceArtefact, destinationArtefact))

                elif sourceArtefact.modifiedTime > destinationArtefact.modifiedTime:
                    # File is more up to date than destination
                    self.put(sourceArtefact, destinationArtefact)

        # Remove destination artefacts if delete is toggled
        if delete:
            # As updated artefacts were popped during their sync - any left File artefacts are to be deleted
            for destinationArtefact in destinationMapped.values():
                if isinstance(destinationArtefact, File):
                    destinationArtefact.manager.rm(destinationArtefact)

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
    def open(self, artefact: typing.Union[File, str], mode: str, **kwargs) -> io.IOBase:
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
    """

    def __init__(self, owner: Manager, uri: str, rootDirectory: Directory):
        self._root = SubDirectory(self, self._ROOT_PATH, rootDirectory)
        self._paths = {self._ROOT_PATH: self._root}
        self._submanagers = None
        self._owner = owner
        self._uri = uri

    def __repr__(self): return '<SubManager of {} {}>'.format(self._owner, self._uri)
    def isabs(self, path: str): return self._owner.isabs(path)
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
        if not relpath:
            return self._root
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

    def _put(self, source_filepath, destination_abspath, merge: bool):
        # Abspath already surpasses this manager - pass args straight on
        self._owner._put(source_filepath, destination_abspath, merge=merge)

    def _putBytes(self, source, destinationAbsPath):
        self._owner._putBytes(source, destinationAbsPath)

    # NOTE movement of files handled by main manager
    def _cp(self, srcObj: Artefact, destPath: str): self._owner._cp(srcObj._concrete, self.join(self._uri, destPath))
    def _mv(self, srcObj: Artefact, destPath: str): self._owner._mv(srcObj._concrete, self.join(self._uri, destPath))
    def _move(self, srcObj: Artefact, destPath: str): self._owner._move(srcObj._concrete, self.join(self._uri,destPath))
    def _moveMain(self, srcPath: str, destPath: str): super()._move(self[self._subrelpath(srcPath)], self._subrelpath(destPath))

    # Main manager handles deleting the underlying objects
    def _rm(self, artefact: Artefact): self._owner._rm(artefact._concrete)
    def _delink(self, artefact: Artefact): self._owner._delink(artefact._concrete)
    def _removeMain(self, artefact: Artefact): super()._delink(self[self._subrelpath(artefact.path)])

    def _collectDirectoryContents(self, directory: Directory):
        self._owner._collectDirectoryContents(directory._concrete)

    @contextlib.contextmanager
    def localise(self, artefact: typing.Union[Artefact, str]):
        with type(self._owner).localise(self, artefact) as abspath:
            yield abspath

    def submanager(self):
        raise NotImplementedError("A submanager cannot be created on a submanager")

class LocalManager(Manager, ABC):
    """ Abstract Base Class for managers that will be working with local artefacts.
    """

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
                self._updateDirectory(obj)

        else:
            # The localised object is
            art = self._makefile(path)
            if obj is not None: obj._update(art)
            else:   self._add(art)

        if exception:
            raise exception

class RemoteManager(Manager, ABC):
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
