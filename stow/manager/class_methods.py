
import os
import io
import typing
import urllib
import hashlib
import shutil
import tempfile
import contextlib

from ..artefacts import Artefact, File, Directory
from .. import utils
from .. import exceptions
from ..class_interfaces import ManagerInterface, LocalInterface, RemoteInterface

import logging
log = logging.getLogger(__name__)

class ClassMethodManager:
    """ Class method namespace for the Manager
    """

    SEPARATORS = ['\\', '/']
    ISOLATED = False

    @classmethod
    def _cwd(cls) -> str:
        """ Return the default working directory for the manager - used to default the artefact path if no path provided

        Returns:
            str: The default path of the manager, the current working directory
        """
        return os.getcwd()

    @classmethod
    def _getManager(cls, artefact: typing.Tuple[Artefact, str, None]) -> typing.Tuple['ClassMethodManager', str]:
        """ Fetch the manager and path for the provided artefact """

        if artefact is None:
            return utils.connect("FS"), cls._cwd()

        elif isinstance(artefact, Artefact):
            return artefact.manager, artefact.path

        elif isinstance(artefact, str):
            return utils.parseURL(artefact)

        else:
            raise TypeError("Artefact reference must be either `stow.Artefact` or string not type {}".format(type(artefact)))

    @classmethod
    def _splitArtefactUnionForm(cls, artefact: typing.Union[Artefact, str, None]) -> typing.Tuple[typing.Union[Artefact, None], str]:
        """ Take an artefact or a string and return in a strict format the object and string representation. This allows
        methods to accept both and resolve and ensure.

        Only the path is guaranteed, the artefact object will be None if it is not passed

        Args:
            artefact: Type unknown, artefact object or path

        Returns:
            artefact: An artefact object or None
            path: the path passed or pull from the artefact object

        """
        if artefact is None:
            return None, cls._cwd()

        if isinstance(artefact, Artefact):
            return artefact, artefact.path

        return None, artefact

    @classmethod
    def _splitAndLoadArtefactUnionForm(cls, artefact: typing.Union[Artefact, str, None]) -> typing.Tuple[Artefact, str]:
        """ Convert the incoming object which could be either an artefact or relative path into a standardised form for
        both such that functions can be easily convert and use what they require

        Args:
            artObj (typing.Union[Artefact, str]): Either the artefact object or it's relative path to be standardised
            require (str): Require that the object exists. when false return None for yet to be created objects but

        Returns:
            Artefact or None: the artefact object or None if it doesn't exists and require is False
            str: The relative path of the object/the passed value
        """

        sourceObject, sourcePath = cls._splitArtefactUnionForm(artefact)
        if sourceObject is None:
            # The artefact wasn't given and the path doesn't lead to an artefact on the manager

            manager, path = cls._getManager(sourcePath)

            return manager[path], path

        return sourceObject, path

    @classmethod
    def _splitAllComponents(cls, artefact: typing.Union[Artefact, str]) -> typing.Tuple["ManagerInterface", typing.Union[Artefact, None], str]:
        if isinstance(artefact, Artefact):
            artefactManager, artefactObj, artefactPath = artefact._manager, artefact, artefact.path

        else:
            artefactManager, artefactPath = cls._getManager(artefact)

            try:
                artefactObj = artefactManager[artefactPath]

            except exceptions.ArtefactNotFound:
                artefactObj = None

        return artefactManager, artefactObj, artefactPath

    @classmethod
    def _localLoad(cls, path: str):
        """ Load a local file in without having to parse the path """
        return utils.connect(manager="FS")[path]

    @classmethod
    def artefact(cls, path: str) -> Artefact:
        """ Fetch an artefact object for the given path

        Params:
            stowPath: Manager relative path to artefact

        Returns:
            Arefact: The artefact object

        Raises:
            ArtefactNotFound: In the event that no artefact exists at the location given
        """
        return cls._splitAndLoadArtefactUnionForm(path)[0]

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
        _, path = cls._splitArtefactUnionForm(artefact)
        return os.path.abspath(path)

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
    def name(cls, artefact: typing.Union[Artefact, str]) -> str:
        """ Return the name of an artefact or path (basename without extension).

        Args:
            artefact: The path or object whose path is to have its base name extracted

        Returns:
            str: the name e.g. /hello/there.txt => there
        """
        _, path = cls._splitArtefactUnionForm(artefact)
        basename = os.path.basename(path)
        index = basename.rfind('.')
        if index != -1:
            return basename[:index]
        return basename

    @classmethod
    def extension(cls, artefact: typing.Union[Artefact, str]) -> str:
        """ Return the extension of an artefact or path.

        Args:
            artefact: The path or object whose path is to have its base name extracted

        Returns:
            str: the extension e.g. /hello/there.txt => txt
        """
        _, path = cls._splitArtefactUnionForm(artefact)
        basename = os.path.basename(path)
        index = basename.rfind('.')
        if index != -1:
            return basename[index+1:]
        return ''

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

        if obj is not None or path.find(":") == -1:
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
    def isfile(cls, artefact: typing.Union[Artefact, str]) -> bool:
        """ Check if the artefact provided is a file

        Args:
            artefact: The artefact to be checked

        Returns:
            Bool: True or False in answer to the question
        """

        try:
            obj, _ = cls._splitAndLoadArtefactUnionForm(artefact)
            return isinstance(obj, File)

        except exceptions.ArtefactNotFound:
            return False

    @classmethod
    def isdir(cls, artefact: typing.Union[Artefact, str]) -> bool:
        """ Check if the artefact provided is a directory

        Args:
            artefact: The artefact to be checked

        Returns:
            Bool: True or False in answer to the question
        """

        try:
            obj, _ = cls._splitAndLoadArtefactUnionForm(artefact)
            return isinstance(obj, Directory)

        except:
            return False

    @classmethod
    def islink(cls, artefact: typing.Union[Artefact, str]) -> bool:
        """ Check if the artefact provided is a link

        Will check for local managers but will default to False for remote managers

        Args:
            artefact: The artefact to be checked

        Returns:
            Bool: True or False in answer to the question
        """

        try:
            obj, _ = cls._splitAndLoadArtefactUnionForm(artefact)
            return isinstance(obj, File) and obj.isLink()

        except:
            return False


    @classmethod
    def ismount(cls, artefact: typing.Union[Artefact, str]) -> bool:
        """ Check if the artefact provided is a link

        Will check for local managers but will default to False for remote managers

        Args:
            artefact: The artefact to be checked

        Returns:
            Bool: True or False in answer to the question
        """
        try:
            obj, _ = cls._splitAndLoadArtefactUnionForm(artefact)
            return isinstance(obj, Directory) and obj.isMount()

        except:
            return False

    @classmethod
    def getctime(cls, artefact: typing.Union[Artefact, str]) -> typing.Union[float, None]:
        """ Get the created time for the artefact as a UTC timestamp

        Args:
            artefact: the artefact whose creation datetime is to be returned

        Returns:
            timestamp: a float timestamp of creation time if manager holds such information else None

        Raises:
            ArtefactNotFound: If there is no artefact at the location
        """

        return cls._splitAndLoadArtefactUnionForm(artefact)[0].createdTime.timestamp()

    @classmethod
    def getmtime(cls, artefact: typing.Union[Artefact, str]) -> typing.Union[float, None]:
        """ Get the modified time for the artefact as a UTC timestamp

        Args:
            artefact: the artefact whose modified datetime is to be returned

        Returns:
            timestamp: a float timestamp of modified time if manager holds such information else None

        Raises:
            ArtefactNotFound: If there is no artefact at the location
        """
        return cls._splitAndLoadArtefactUnionForm(artefact)[0].modifiedTime.timestamp()

    @classmethod
    def getatime(cls, artefact: typing.Union[Artefact, str]) -> typing.Union[float, None]:
        """ Get the accessed time for the artefact as a UTC timestamp

        Args:
            artefact: the artefact whose accessed datetime is to be returned

        Returns:
            timestamp: a float timestamp of accessed time if manager holds such information else None

        Raises:
            ArtefactNotFound: If there is no artefact at the location
        """
        return cls._splitAndLoadArtefactUnionForm(artefact)[0].accessedTime.timestamp()

    @classmethod
    def exists(cls, artefact: typing.Union[Artefact, str]) -> bool:
        """ Return true if the given artefact is a member of the manager, or the path is correct for the manager and it
        leads to a File or Directory.

        Does not handle protocols

        Args:
            artefact: Artefact or path whose existence is to be checked

        Returns:
            bool: True if artefact exists else False
        """
        manager, path = cls._getManater(artefact)

        return manager._exists(path)

    @classmethod
    def lexists(cls, artefact: typing.Union[Artefact, str]) -> str:
        """ Return true if the given artefact is a member of the manager, or the path is correct for the manager and it
        leads to a File or Directory.

        Does not handle protocols

        Args:
            artefact: Artefact or path whose existence is to be checked

        Returns:
            bool: True if artefact exists else False
        """
        return cls.islink(artefact)

    @classmethod
    def join(cls, *paths: typing.Iterable[str], separator=os.sep, joinAbsolutes: bool = False) -> str:
        """ Join one or more path components intelligently. The return value is the concatenation of path and any
        members of *paths with exactly one directory separator following each non-empty part except the last,
        meaning that the result will only end in a separator if the last part is empty. If a component is an absolute
        path, all previous components are thrown away and joining continues from the absolute path component.

        Protocols/drive letters are perserved in the event that an absolute is passed in.

        Args:
            *paths: segments of a path to be joined together
            separator: The character to be used to join the path segments
            joinAbsolutes: Whether to stick to normal behaviour continue from absolute paths or join them in series

        Returns:
            str: A joined path
        """
        if not paths:
            return ""

        parsedResult = None  # Store the network information while path is joined
        joined = ""  # Constructed path

        for segment in paths:
            if isinstance(segment, Artefact):
                # Convert artefacts to paths
                segment = segment.path

            # Identify and record the last full
            presult = urllib.parse.urlparse(segment)
            if presult.scheme:
                parsedResult = presult
                segment = presult.path

            if joined:
                # A path is in the midst of being created

                if any(segment.startswith(sep) for sep in cls.SEPARATORS):
                    if joinAbsolutes:
                        joined = joined.rstrip('\\/') + segment

                    else:
                        joined = segment

                else:
                    if any(joined.endswith(sep) for sep in cls.SEPARATORS):
                        joined += segment

                    else:
                        joined += separator + segment

            else:
                joined = segment

        # Add back in the protocol if given
        if parsedResult:
            return urllib.parse.ParseResult(
                parsedResult.scheme,
                parsedResult.netloc,
                joined,
                parsedResult.params,
                parsedResult.query,
                parsedResult.fragment
            ).geturl()

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
            ).geturl()

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
    def samefile(cls, artefact1: typing.Union[Artefact, str], artefact2: typing.Union[Artefact, str]) -> bool:
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

    @staticmethod
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

    @staticmethod
    def md5(path):
        """ TODO """
        hash_md5 = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)

        return hash_md5.hexdigest()

    @classmethod
    def get(cls, source: typing.Union[Artefact, str], destination: typing.Union[str, None] = None, overwrite: bool = False) -> typing.Union[Artefact, bytes]:
        """ Get an artefact from a local or remote source and download the artefact either to a local artefact or as bytes

        Args:
            source (Artefact|str): The source artefact to be downloaded
            destination (str) = None: The local path to write the artefact. If None return file as bytes
            overwrite (bool) = False: local directory protection - to overwrite a directory with overwrite must be True

        Return:
            Artefact|bytes: The local artefact downloaded, or the bytes of the source artefact.
        """

        # Split into object and path - Ensure that the artefact to get is from this manager
        obj, _ = cls._splitAndLoadArtefactUnionForm(source)

        # Ensure the destination - Remove or raise issue for a local artefact at the location where the get is called
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
                os.makedirs(cls.dirname(destination), exist_ok=True)

            # Get the object using the underlying manager implementation
            obj.manager._get(obj, destination)

            # Load the downloaded artefact from the local location and return
            return cls._localLoad(destination)

        else:
            if not isinstance(obj, File):
                raise exceptions.ArtefactTypeError("Cannot get file bytes of {}".format(obj))

            return obj.manager._getBytes(obj)

    @classmethod
    def put(
        cls,
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

        # Validate source before deleting destination
        if not isinstance(source, bytes):
            sourceObj, _ = cls._splitAndLoadArtefactUnionForm(source)

        # Delete if exists the destination artefacts
        try:
            destinationObj, destinationPath = cls._splitAndLoadArtefactUnionForm(destination)


            destinationManager = destinationObj.manager

            # Delete the destination object that is able to be overwritten
            if overwrite or isinstance(destinationObj, File):
                destinationObj.manager._rm(destinationObj)
            else:
                raise exceptions.OperationNotPermitted(
                    "Cannot put {} as destination is a directory, and overwrite has not been set to True"
                )

        except exceptions.ArtefactNotFound:
            # The file doesn't exist to be deleted
            destinationManager, destinationPath = cls._getManager(destination)

        if isinstance(source, bytes):
            return destinationManager._putBytes(source, destinationPath)

        else:
            with sourceObj.localise() as abspath:
                return destinationManager._put(abspath, destinationPath)

    @classmethod
    def cp(
        cls,
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

        # Load the source object that is to be copied
        sourceObj, _ = cls._splitAndLoadArtefactUnionForm(source)

        # Check the destination
        if isinstance(destination, Artefact):
            destinationManager = destination._manager

            if isinstance(destination, Directory) and not overwrite:
                raise exceptions.OperationNotPermitted("Cannot replace directory without passing overwrite True")

        else:
            destinationManager, destinationRelpath = utils.parseURL(destination)

            try:
                destination = destinationManager[destinationRelpath]
                if isinstance(destination, Directory) and not overwrite:
                    raise exceptions.OperationNotPermitted("Cannot replace directory without passing overwrite True")

            except exceptions.ArtefactNotFound:
                pass

        # Check if the source and destination are from the same
        if type(sourceObj._manager) == type(destinationManager):
            return sourceObj._manager._cp(sourceObj, destination)

        return cls.put(sourceObj, destination, overwrite=overwrite)

    @classmethod
    def mv(
        cls,
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

        # Load the source object that is to be copied
        sourceObj, sourcePath = cls._splitAndLoadArtefactUnionForm(source)
        destinationManager, destinationObj, destinationPath = cls._splitAllComponents(destination)

        # Prevent the overwriting of a directory without permission
        if destinationObj is not None and isinstance(destination, Directory) and not overwrite:
            raise exceptions.OperationNotPermitted("Cannot replace directory without passing overwrite True")

        # Check if the source and destination are from the same manager class
        if type(sourceObj._manager) == type(destinationManager):
            return sourceObj._manager._mv(
                sourceObj._manager._abspath(sourcePath),
                destinationManager._abspath(destinationPath)
            )

        # Moving between manager types - put the object and then delete the old one
        object = cls.put(sourceObj, destination, overwrite=overwrite)
        sourceObj._manager._rm(sourceObj)
        return object

    @classmethod
    def rm(cls, artefact: typing.Union[Artefact, str], recursive: bool = False) -> None:
        """ Remove an artefact from the manager using the artefact object or its relative path. If its a directory,
        remove it if it is empty, or all of its contents if recursive has been set to true.

        Args:
            artefact (typing.Union[Artefact, str]): the object which is to be deleted
            recursive (bool) = False: whether to accept the deletion of a directory which has contents
        """

        obj, _ = cls._splitAndLoadArtefactUnionForm(artefact)
        if isinstance(obj, Directory) and not recursive:
            raise exceptions.OperationNotPermitted(
                "Cannot delete a container object that isn't empty - set recursive to True to proceed"
            )

        # Remove the artefact from the manager
        cls._rm(obj)  # Remove the underlying data objects

    @classmethod
    def sync(cls, source: typing.Union[Directory, str], destination: typing.Union[Directory, str], overwrite: bool = False, delete: bool = False) -> None:
        """ Put artefacts in the source location into the destination location if they have more recently been edited

        Args:
            source (Directory): source directory artefact
            destination (Directory): destination directory artefact on the manager
            delete: Togger the deletion of artefacts that are members of the destination which do not conflict with
                the source.

        Raises:
            ArtefactNotFound: In the event that the source directory doesn't exist

        """

        # Fetch the destination - sync target
        try:
            destinationObj, destinationPath = cls._splitAndLoadArtefactUnionForm(destination)

        except exceptions.ArtefactNotFound:
            # There is no destination to sync with, therefore we can put the entire source
            log.debug("Syncing: No destination therefore putting entire source")
            return cls.put(source, destinationPath)

        # Fetch the source object and require that it be an Artefact so we can check object states
        sourceObj, sourcePath = cls._splitAndLoadArtefactUnionForm(source)

        # Ensure that the two passed artefacts are directories
        if not (isinstance(sourceObj, Directory) and isinstance(destinationObj, Directory)):
            raise exceptions.ArtefactTypeError("Cannot Synchronise non directory objects {} -> {} - must sync directories".format(sourceObj, destinationObj))

        # Get the mappings of source artefacts and destination objects
        sourceMapped = {
            source.relpath(artefact): artefact
            for artefact in source.ls(recursive=True)
            if isinstance(artefact, File)
        }

        destinationMapped = {
            destinationObj.relpath(artefact): artefact
            for artefact in destinationObj.ls(recursive=True)
        }

        # Iterate over all the files in the source
        for relpath, sourceArtefact in sourceMapped.items():

            # Look to see if there is a conflict
            if relpath not in destinationMapped:
                # The file doesn't conflict so we will push to destination
                log.debug(f'Syncing: Putting source object {sourceArtefact}')
                cls.put(sourceArtefact, cls.join(destinationObj.path, relpath, separator='/'))

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
                    log.debug(f'Syncing: Updating destination object {destinationArtefact} with {sourceArtefact}')
                    cls.put(sourceArtefact, destinationArtefact, overwrite=overwrite)

        # Remove destination artefacts if delete is toggled
        if delete:
            # As updated artefacts were popped during their sync - any left File artefacts are to be deleted

            # Sort to ensure that nested files appear before their directories
            # This allows us to check to see if the directory is empty knowning that everything to be deleted from it
            # has been, as all nested artefacts will appear sooner and be removed before hand.
            for artefact in sorted(destinationMapped.values(), key=lambda x: len(x.path)):

                if isinstance(artefact, File) or (artefact.isEmpty()):
                    # Delete the file or remove the directory if it is now empty
                    log.debug(f"Syncing: Deleting unfound source object from destionation {artefact}")
                    artefact.manager._rm(artefact)

    @classmethod
    def iterls(cls, artefact: typing.Union[Directory, str, None] = None, recursive: bool = False) -> typing.Generator[Artefact, None, Artefact]:
        """ List contents of the directory path/artefact given.

        Args:
            art (Directory/str): The Directory artefact or the relpath to the directory to be listed
            recursive (bool) = False: Return subdirectory contents as well

        Returns:
            {Artefact}: The artefact objects which are within the directory
        """
        # Convert the incoming artefact reference - require that the object exist and that it is a directory
        artobj, _ = cls._splitAndLoadArtefactUnionForm(artefact)
        if not isinstance(artobj, Directory):
            raise TypeError("Cannot perform ls action on File artefact: {}".format(artobj))

        # Yield the contents of the directory
        for subArtefact in artobj.manager._ls(artobj):
            if recursive and isinstance(subArtefact, Directory):
                yield from cls.iterls(subArtefact, recursive=recursive)
            yield subArtefact

    @classmethod
    def ls(cls, art: typing.Union[Directory, str, None] = None, recursive: bool = False) -> typing.Set[Artefact]:
        """ List contents of the directory path/artefact given.

        Args:
            art (Directory/str): The Directory artefact or the relpath to the directory to be listed
            recursive (bool) = False: Return subdirectory contents as well

        Returns:
            {Artefact}: The artefact objects which are within the directory
        """
        return set(cls.iterls(art, recursive))

    @classmethod
    def mkdir(cls, path: str, ignoreExists: bool = True, overwrite: bool = False) -> Directory:
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

        try:
            artefact, path = cls._splitAndLoadArtefactUnionForm(path)
            if isinstance(artefact, File):
                raise exceptions.OperationNotPermitted("Cannot make a directory as location {} is a file object".format(path))

            if ignoreExists and not overwrite:
                return artefact

        except:
            pass

        with tempfile.TemporaryDirectory() as directory:
            return cls.put(directory, artefact, overwrite=overwrite)

    @classmethod
    def touch(cls, relpath: str) -> Artefact:
        """ Perform the linux touch command to create a empty file at the path provided, or for existing files, update
        their modified timestamps as if there where just created.

        Args:
            relpath (str): Path to new file location
        """

        manager, path = cls._getManager(relpath)

        try:
            artefact = manager[path]
            cls.cp(artefact, artefact)

        except exceptions.ArtefactNotFound:
            pass

        return cls.put(b'', relpath)

    @classmethod
    @contextlib.contextmanager
    def open(cls, artefact: typing.Union[File, str], mode: str = "r", **kwargs) -> io.IOBase:
        """ Open a file and create a stream to that file. Expose interface of `open`

        Args:
            artefact: The object that represents the file (or path to the file) to be openned by this manager
            mode: The open method
            kwargs: kwargs to be passed to the interface of open

        Yields:
            io.IOBase: An IO object depending on the mode for interacting with the file
        """

        manager, _ = cls._getManager(artefact)

        if mode in cls._READONLYMODES:
            if not manager.exists(artefact):
                raise exceptions.ArtefactNotFound(f"Could not open {artefact} to read since it doesn't exist")

        with manager.localise(artefact) as abspath:
            with open(abspath, mode, **kwargs) as handle:
                yield handle

    @classmethod
    @contextlib.contextmanager
    def localise(cls, artefact: typing.Union[Artefact, str]) -> str:

        # Get the manager instance to handle the localise method
        manager, path = cls._getManager(artefact)

        # Call localise on the manager with the path
        with manager.localise(path) as handle:
            yield handle