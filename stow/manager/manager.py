import os
import re
import io
import typing
import urllib
import shutil
import tempfile
import mimetypes
import contextlib
import datetime
import hashlib

from .abstract_methods import AbstractManager

from ..artefacts import Artefact, PartialArtefact, File, Directory, HashingAlgorithm
from ..callbacks import AbstractCallback
from .. import _utils as utils
from .. import exceptions

import logging
log = logging.getLogger(__name__)

class ManagerReloader:
    """ Class to manage the reloading of a reduced Manager """
    def __new__(cls, config):
        # This will create a new manager if it doesn't exist of fetch the one globally created
        return utils.connect(**config)

class Manager(AbstractManager):
    """ Manager Abstract base class - expressed the interface of a Manager which governs a storage
    option and allows extraction and placement of files in that storage container

    """

    SEPARATORS = ['\\', '/']
    ISOLATED = False

    _READONLYMODES = ["r", "rb"]
    _MULTI_SEP_REGEX = re.compile(r"(\\{2,})|(\/{2,})")
    _RELPATH_REGEX = re.compile(r"^([a-zA-Z0-9]+:)?([/\\\w\.!-_*'() ]*)$")

    def __contains__(self, artefact: typing.Union[Artefact, str]) -> bool:
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

        artefact = self._identifyPath(path)
        if artefact is None:
            raise exceptions.ArtefactNotFound(f"No artefact exists at: {path}")
        return artefact

    def __reduce__(self):
        return (ManagerReloader, (self.toConfig(),))

    def _ensurePath(self, artefact: typing.Union[Artefact, str, None]) -> str:
        """ Collapse artefact into path - Convert an artefact into a path or return. This returns
        the manager relative path instead. To be used when the response is to be in turns of the
        manager. For simple checks, artefacts can be used directly (which uses the abspath)

        Args:
            artefact: The artefact to be ensured is a path str
        """
        return artefact.path if isinstance(artefact, Artefact) else artefact

    def _splitExternalArtefactForm(
        self,
        artefact: typing.Union[Artefact, str, None],
        load: bool = True,
        require: bool = True
        ) -> typing.Tuple['Manager', Artefact, str]:
        """ Convert the incoming object which could be either an artefact or relative path into a
        standardised form for both such that functions can be easily convert and use what they
        require.

        Args:
            artObj (typing.Union[Artefact, str]): Either the artefact object or it's relative path to be standardised
            require (str): Require that the object exists. when false return None for yet to be created objects but

        Returns:
            Artefact or None: the artefact object or None if it doesn't exists and require is False
            str: The relative path of the object/the passed value
        """

        if isinstance(artefact, Artefact):
            return artefact.manager, artefact, artefact.path

        else:
            obj = None

            if artefact is None:
                manager, path = utils.connect("FS"), self._cwd()

            elif isinstance(artefact, str):
                manager, path = utils.parseURL(artefact)

            else:
                t = type(artefact)
                raise TypeError(
                    f"Artefact reference must be either `stow.Artefact` or string not type {t}"
                )


            if load:

                try:
                    obj = manager[path]

                except exceptions.ArtefactNotFound:
                    if require:
                        raise

            return manager, obj, path

    def _splitManagerArtefactForm(
        self,
        artefact: typing.Union[Artefact, str, None],
        load: bool = True,
        require: bool = True
        ) -> typing.Tuple['Manager', typing.Union[File, Directory], str]:
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
            return artefact.manager, artefact, artefact.path

        else:
            obj = None

            if artefact in [None, '/']:
                return self, Directory(self, '/'), '/'

            elif isinstance(artefact, str):
                manager, path = utils.parseURL(
                    artefact,
                    default_manager=self if type(self) != Manager else None
                )

            else:
                t = type(artefact)
                raise TypeError(
                    f"Artefact reference must be either `stow.Artefact` or string not type {t}"
                )

            if load:

                try:
                    obj = manager[path]

                except exceptions.ArtefactNotFound:
                    if require:
                        raise

            return manager, obj, path

    def _get_content_type(self, path: str) -> str:
        """ Get the content type for the path given """
        contentType, _ = mimetypes.guess_type(path)
        contentType = (contentType or 'application/octet-stream')
        return contentType

    def _set_content_type(self, path: str, content_type: str) -> str:
        """ Set the content type of the file """
        raise NotImplementedError('Manager does not have an method for changing the content-type for the path given')

    def _cwd(self) -> str:
        """ Return the default working directory for the manager - used to default the artefact path if no path provided

        Returns:
            str: The default path of the manager, the current working directory
        """
        return os.getcwd()

    def manager(self, artefact: typing.Union[Artefact, str]) -> 'Manager':
        """ Fetch the manager object for the artefact

        Params:
            artefact: The artefact whose manager is to be returned

        Returns:
            Manager: The Manager that produced the artefact
        """
        return self._splitManagerArtefactForm(artefact)[0]

    def artefact(self, path: str) -> Artefact:
        """ Fetch an artefact object for the given path

        Params:
            stowPath: Manager relative path to artefact

        Returns:
            Arefact: The artefact object

        Raises:
            ArtefactNotFound: In the event that no artefact exists at the location given
        """
        return self._splitManagerArtefactForm(path)[1]

    def abspath(self, artefact: typing.Union[Artefact, str]) -> str:
        """ Return a normalized absolute version of the path or artefact given.

        Args:
            artefact: The path or object whose path is to be made absolute and returned

        Returns:
            str: the absolute path of the artefact provided

        Raises:
            ValueError: Cannot make a remote artefact object's path absolute
        """
        manager, _, path = self._splitManagerArtefactForm(artefact, load=False)
        return manager._abspath(path)

    def basename(self, artefact: typing.Union[Artefact, str]) -> str:
        """ Return the base name of an artefact or path. This is the second element of the pair returned by passing path
        to the function `split()`.

        Args:
            artefact: The path or object whose path is to have its base name extracted

        Returns:
            str: the basename
        """
        return os.path.basename(self._ensurePath(artefact))

    def name(self, artefact: typing.Union[Artefact, str]) -> str:
        """ Return the name of an artefact or path (basename without extension).

        Args:
            artefact: The path or object whose path is to have its base name extracted

        Returns:
            str: the name e.g. /hello/there.txt => there
        """
        basename = os.path.basename(self._ensurePath(artefact))
        index = basename.rfind('.')
        if index != -1:
            return basename[:index]
        return basename

    def extension(self, artefact: typing.Union[Artefact, str]) -> str:
        """ Return the extension of an artefact or path.

        Args:
            artefact: The path or object whose path is to have its base name extracted

        Returns:
            str: the extension e.g. /hello/there.txt => txt
        """

        basename = os.path.basename(self._ensurePath(artefact))
        index = basename.rfind('.')
        if index != -1:
            return basename[index+1:]
        return ''

    def commonpath(self, paths: typing.Iterable[typing.Union[Artefact, str]]) -> str:
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
        return os.path.commonpath([self._ensurePath(path) for path in paths])

    def commonprefix(self, paths: typing.Iterable[typing.Union[Artefact, str]]) -> str:
        """ Return the longest common string literal for a collection of path/artefacts

        Examples:
            commonpath(["/foo/bar", "/foo/ban/pip"]) == "/foo/ba"

        Args:
            paths: Artefact/paths who will have their paths comparied to find a common path

        Returns:
            str: A string that all paths startwith (may be empty string)
        """
        return os.path.commonprefix([self._ensurePath(path) for path in paths])

    def dirname(self, artefact: typing.Union[Artefact, str]) -> str:
        """ Return the directory name of path or artefact. Preserve the protocol of the path if a protocol is given

        Args:
            artefact: The artefact or path whose directory path is to be returned

        Returns:
            str: The directory path for the holding directory of the artefact
        """

        # Covert the path - do not parse the path
        path = self._ensurePath(artefact)

        if path.find(":") == -1:
            # Path with no protocol and therefore no need to url parse
            return os.path.dirname(path)

        else:
            # Preserve protocol (if there is one) - dirname the path
            result = urllib.parse.urlparse(path)
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

    def isfile(self, artefact: typing.Union[Artefact, str]) -> bool:
        """ Check if the artefact provided is a file

        Args:
            artefact: The artefact to be checked

        Returns:
            Bool: True or False in answer to the question
        """
        return isinstance(self._splitManagerArtefactForm(artefact, require=False)[1], File)

    def isdir(self, artefact: typing.Union[Artefact, str]) -> bool:
        """ Check if the artefact provided is a directory

        Args:
            artefact: The artefact to be checked

        Returns:
            Bool: True or False in answer to the question
        """
        return isinstance(self._splitManagerArtefactForm(artefact, require=False)[1], Directory)

    def islink(self, artefact: typing.Union[Artefact, str]) -> bool:
        """ Check if the artefact provided is a link

        Will check for local managers but will default to False for remote managers

        Args:
            artefact: The artefact to be checked

        Returns:
            Bool: True or False in answer to the question
        """
        manager, _, path = self._splitManagerArtefactForm(artefact, load = False)
        return manager._isLink(path)

    def ismount(self, artefact: typing.Union[Artefact, str]) -> bool:
        """ Check if the artefact provided is a link

        Will check for local managers but will default to False for remote managers

        Args:
            artefact: The artefact to be checked

        Returns:
            Bool: True or False in answer to the question
        """
        manager, _, path = self._splitManagerArtefactForm(artefact, load = False)
        return manager._isMount(path)

    def getctime(self, artefact: typing.Union[Artefact, str]) -> typing.Union[float, None]:
        """ Get the created time for the artefact as a UTC timestamp

        Args:
            artefact: the artefact whose creation datetime is to be returned

        Returns:
            timestamp: a float timestamp of creation time if manager holds such information else None

        Raises:
            ArtefactNotFound: If there is no artefact at the location
        """
        return self._splitManagerArtefactForm(artefact)[1].createdTime.timestamp()

    def getmtime(self, artefact: typing.Union[Artefact, str]) -> typing.Union[float, None]:
        """ Get the modified time for the artefact as a UTC timestamp

        Args:
            artefact: the artefact whose modified datetime is to be returned

        Returns:
            timestamp: a float timestamp of modified time if manager holds such information else
                None

        Raises:
            ArtefactNotFound: If there is no artefact at the location
        """
        return self._splitManagerArtefactForm(artefact)[1].modifiedTime.timestamp()

    def _setmtime(self, *args, **kwargs):
        raise NotImplementedError(
            f"Managers of type {type(self)} do not support modified time updates"
        )
    def setmtime(
        self,
        artefact: typing.Union[Artefact, str],
        _datetime: typing.Union[float, datetime.datetime]
        ) -> datetime.datetime:
        """ Update the artefacts modified time

        Args:
            artefact (Artefact): The artefact to update
            _datetime (float, datetime): The time to set against the artefact
        """
        manager, artefact, _ = self._splitManagerArtefactForm(artefact, require=True)
        return manager._setmtime(artefact, _datetime)

    def getatime(self, artefact: typing.Union[Artefact, str]) -> typing.Union[float, None]:
        """ Get the accessed time for the artefact as a UTC timestamp

        Args:
            artefact: the artefact whose accessed datetime is to be returned

        Returns:
            timestamp: a float timestamp of accessed time if manager holds such information else
                None

        Raises:
            ArtefactNotFound: If there is no artefact at the location
        """
        return self._splitManagerArtefactForm(artefact)[1].accessedTime.timestamp()

    def _setatime(self, *args, **kwargs):
        raise NotImplementedError(
            f"Managers of type {type(self)} do not support access time updates"
        )
    def setatime(
        self,
        artefact: typing.Union[Artefact, str],
        _datetime: typing.Union[float, datetime.datetime]
        ) -> datetime.datetime:
        """ Update the artefacts access time

        Args:
            artefact (Artefact): The artefact to update
            _datetime (float, datetime): The time to set against the artefact
        """
        manager, artefact, _ = self._splitManagerArtefactForm(artefact, require=True)
        return manager._setatime(artefact, _datetime)

    def exists(self, artefact: typing.Union[Artefact, str]) -> bool:
        """ Return true if the given artefact is a member of the manager, or the path is correct for the manager and it
        leads to a File or Directory.

        Does not handle protocols

        Args:
            artefact: Artefact or path whose existence is to be checked

        Returns:
            bool: True if artefact exists else False
        """
        manager, _, path = self._splitManagerArtefactForm(artefact, load=False)
        return manager._exists(path)

    def lexists(self, artefact: typing.Union[Artefact, str]) -> str:
        """ Return true if the given artefact is a member of the manager, or the path is correct for the manager and it
        leads to a File or Directory.

        Does not handle protocols

        Args:
            artefact: Artefact or path whose existence is to be checked

        Returns:
            bool: True if artefact exists else False
        """
        return os.path.lexists(artefact)

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

    def realpath(self, path: str) -> str:
        """ Return the canonical path of the specified filename, eliminating any symbolic links encountered in the path
        (if they are supported by the operating system).

        Args:
            path: the path to have symbolic links corrected

        Returns:
            str: the path with the symbolic links corrected
        """
        return os.path.realpath(path)

    def relpath(self, path: str, start: str = os.curdir, separator: str = os.sep) -> str:
        """ Return a relative filepath to path either from the current directory or from an optional start directory

        Args:
            path: the path to be made relative
            start: the location to become relative to
        """
        relpath = os.path.relpath(path, start)
        return relpath if separator == os.sep else relpath.replace(os.sep, separator)

    def samefile(self, artefact1: typing.Union[Artefact, str], artefact2: typing.Union[Artefact, str]) -> bool:
        """ Check if provided artefacts are represent the same data on disk

        Args:
            artefact1: An artefact object or path
            artefact2: An artefact object or path

        Returns:
            bool: True if the artefacts are the same
        """
        return os.path.samefile(artefact1, artefact2)

    @staticmethod
    def sameopenfile(handle1: io.IOBase, handle2: io.IOBase) -> bool:
        """ Return True if the file descriptors fp1 and fp2 refer to the same file.
        """
        return os.path.sameopenfile(handle1, handle2)

    def samestat(self, artefact1: typing.Union[Artefact, str], artefact2: typing.Union[Artefact, str]) -> bool:
        """ Check if provided artefacts are represent the same data on disk

        Args:
            artefact1: An artefact object or path
            artefact2: An artefact object or path

        Returns:
            bool: True if the artefacts are the same
        """
        return os.path.samestat(artefact1, artefact2)

    def split(self, artefact: typing.Union[Artefact, str]) -> typing.Tuple[str, str]:
        """ Split the pathname path into a pair, (head, tail) where tail is the last pathname component and head is
        everything leading up to that.

        Args:
            artefact: the artefact to be split

        Returns:
            (dirname, basename): the split parts of the artefact
        """
        path = artefact.path if isinstance(artefact, Artefact) else artefact
        return (self.dirname(path), self.basename(path))

    def splitdrive(self, path: str) -> typing.Tuple[str, str]:
        """ Split the pathname path into a pair (drive, tail) where drive is either a mount point or the empty string.

        Args:
            path: the path whose mount point/drive is to be removed

        Returns:
            (drive, path): tuple with drive string separated from the path
        """

        return os.path.splitdrive(path)

    def splitext(self, artefact: typing.Union[Artefact, str]) -> typing.Tuple[str, str]:
        """ Split the pathname path into a pair (root, ext) such that root + ext == path, and ext is empty or begins
        with a period and contains at most one period.

        Args:
            artefact: the artefact to have the extension extracted

        Returns:
            (root, ext): The root path without the extension and the extension
        """
        return os.path.splitext(artefact)

    @staticmethod
    def md5(path):
        """ TODO """
        hash_md5 = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)

        return hash_md5.hexdigest()

    def digest(
        self,
        artefact: typing.Union[File, str],
        algorithm: HashingAlgorithm = HashingAlgorithm.MD5
        ):

        manager, obj, _ = self._splitManagerArtefactForm(artefact)
        if isinstance(obj, File):
            return manager._digest(obj, algorithm)

        else:
            raise ValueError(f'Cannot get file digest for directory {obj}')

    def get(
        self,
        source: typing.Union[Artefact, str],
        destination: typing.Union[str, None] = None,
        *,
        overwrite: bool = False,
        callback: typing.Type[AbstractCallback] = None
        ) -> typing.Union[Artefact, bytes]:
        """ Get an artefact from a local or remote source and download the artefact either to a local artefact or as bytes

        Args:
            source (Artefact|str): The source artefact to be downloaded
            destination (str) = None: The local path to write the artefact. If None return file as bytes
            overwrite (bool) = False: local directory protection - to overwrite a directory with overwrite must be True

        Return:
            Artefact|bytes: The local artefact downloaded, or the bytes of the source artefact.
        """

        # Split into object and path - Ensure that the artefact to get is from this manager
        _, obj, _ = self._splitManagerArtefactForm(source)

        if callback is not None and issubclass(callback, AbstractCallback):
            callback = callback('get')

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
                os.makedirs(self.dirname(destination), exist_ok=True)

            # Get the object using the underlying manager implementation
            obj.manager._get(obj, destination, callback=callback)

            # Load the downloaded artefact from the local location and return
            return PartialArtefact(utils.connect(manager="FS"), destination)

        else:
            if not isinstance(obj, File):
                raise exceptions.ArtefactTypeError("Cannot get file bytes of {}".format(obj))

            return obj.manager._getBytes(obj, callback=callback)

    def put(
        self,
        source: typing.Union[Artefact, str, bytes],
        destination: typing.Union[Artefact, str],
        overwrite: bool = False,
        *,
        metadata: typing.Dict[str, str] = None,
        callback: typing.Type[AbstractCallback] = None
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
            _, sourceObj, _ = self._splitExternalArtefactForm(source)

        # Load in the information about the destination
        destinationManager, destinationObj, destinationPath = self._splitManagerArtefactForm(destination, require=False)

        if destinationObj is not None:
            # Delete the destination object that is able to be overwritten
            if overwrite or isinstance(destinationObj, File):
                destinationManager._rm(destinationObj)
            else:
                raise exceptions.OperationNotPermitted(
                    "Cannot put {} as destination is a directory, and overwrite has not been set to True"
                )

        if callback is not None and issubclass(callback, AbstractCallback):
            # Need to instantiate the callback
            callback = callback('put')

        if isinstance(source, bytes):
            return destinationManager._putBytes(
                source,
                destinationPath,
                metadata=metadata,
                callback=callback
            )

        else:
            return destinationManager._put(
                sourceObj,
                destinationPath,
                metadata=metadata,
                callback=callback
            )

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

        # Load the source object that is to be copied
        _, sourceObj, sourcePath = self._splitManagerArtefactForm(source)
        destinationManager, destinationObj, destinationPath = self._splitManagerArtefactForm(destination, require=False)

        # Prevent the overwriting of a directory without permission
        if destinationObj is not None and isinstance(destinationObj, Directory):
            if not overwrite:
                raise exceptions.OperationNotPermitted("Cannot replace directory without passing overwrite True")
            destinationManager._rm(destinationObj)

        # Check if the source and destination are from the same manager class
        if type(sourceObj._manager) == type(destinationManager) and not sourceObj._manager.ISOLATED:
            return destinationManager._cp(sourceObj, destinationPath)

        return self.put(sourceObj, destination)

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

        # Load the source object that is to be copied
        _, sourceObj, sourcePath = self._splitManagerArtefactForm(source)
        destinationManager, destinationObj, destinationPath = self._splitManagerArtefactForm(destination, require=False)

        # Prevent the overwriting of a directory without permission
        if destinationObj is not None:
            if isinstance(destinationObj, Directory) and not overwrite:
                raise exceptions.OperationNotPermitted("Cannot replace directory without passing overwrite True")
            destinationManager._rm(destinationObj)

        # Check if the source and destination are from the same manager class
        if type(sourceObj._manager) == type(destinationManager) and not sourceObj._manager.ISOLATED:
            return destinationManager._mv(sourceObj, destinationPath)

        # Moving between manager types - put the object and then delete the old one
        object = self.put(sourceObj, destination, overwrite=overwrite)
        sourceObj._manager._rm(sourceObj)
        return object

    def rm(self, artefact: typing.Union[Artefact, str], recursive: bool = False) -> None:
        """ Remove an artefact from the manager using the artefact object or its relative path. If its a directory,
        remove it if it is empty, or all of its contents if recursive has been set to true.

        Args:
            artefact (typing.Union[Artefact, str]): the object which is to be deleted
            recursive (bool) = False: whether to accept the deletion of a directory which has contents
        """

        manager, obj, _ = self._splitManagerArtefactForm(artefact)
        if isinstance(obj, Directory) and not recursive and not obj.isEmpty():
            raise exceptions.OperationNotPermitted(
                "Cannot delete a container object that isn't empty - set recursive to True to proceed"
            )

        # Remove the artefact from the manager
        manager._rm(obj)  # Remove the underlying data objects


    def sync(
        self,
        source: typing.Union[File, Directory, str],
        destination: typing.Union[Artefact, str],
        *,
        overwrite: bool = False,
        delete: bool = False,
        check_modified_times: bool = True,
        digest_comparator: typing.Callable[[File, File], bool] = None
        ) -> None:
        """ Put artefacts from the source location into the destination location if they have more recently been edited.

        Args:
            source (Directory): source directory artefact
            destination (Directory): destination directory artefact on the manager
            delete: Togger the deletion of artefacts that are members of the destination which do not conflict with
                the source.
            check_modified_times (bool): Prevent sync even if digest is different, if destination is newer than source
            digest_comparator: (Callable): A comparison method that takes possible syncing targets and allows for
                dynamic sync checking of tags or custom digests

        Raises:
            ArtefactNotFound: In the event that the source directory doesn't exist
        """

        # Fetch the source object
        _, sourceObj, sourcePath = self._splitManagerArtefactForm(source)

        # Fetch the destination object
        destinationManager, destinationObj, destinationPath = self._splitExternalArtefactForm(destination, require=False)

        if destinationObj is None:
            # The destination doesn't exist - sync the entire source

            log.debug("Syncing: No destination therefore putting entire source")
            self.put(sourceObj, destination)

        elif isinstance(destinationObj, File):
            if isinstance(sourceObj, Directory):
                # The source is a directory - we simply replace the file
                self.put(sourceObj, destinationObj, overwrite=overwrite)

            elif (
                (not check_modified_times or destinationObj.modifiedTime < sourceObj.modifiedTime) and
                (digest_comparator is None or not digest_comparator(sourceObj, destinationObj))
                ):
                self.put(sourceObj, destinationObj)

            else:
                log.debug('%s already synced', destination)

        else:
            # Desintation object is a dictionary
            if isinstance(sourceObj, File):
                # We are trying to sync a file to a directory - this is a put
                return self.put(sourceObj, destinationObj, overwrite=overwrite)

            # Syncing a source directory to a destination directory
            destinationMap = {artefact.basename: artefact for artefact in destinationObj.ls()}

            # Recursively fill in destination at this recursion level
            for artefact in sourceObj.ls():
                if artefact.basename in destinationMap:
                    self.sync(artefact, destinationMap.pop(artefact.basename))

                else:
                    self.put(artefact, self.join(destinationObj.path, artefact.basename))

            # Any remaining destionation objects were not targets of sync - delete if argument passed
            if delete:
                for artefact in destinationMap.values():
                    destinationManager.rm(artefact, recursive=overwrite)

    def iterls(
        self,
        artefact: typing.Union[Directory, str, None] = None,
        recursive: bool = False,
        *,
        ignore_missing: bool = False
        ) -> typing.Generator[typing.Union[File, Directory], None, None]:
        """ List contents of the directory path/artefact given.

        Args:
            art (Directory/str): The Directory artefact or the relpath to the directory to be listed
            recursive (bool) = False: Return subdirectory contents as well
            *,
            ignore_missing (bool) = False: Ignore whether the artefact exists, if it doesn't exist return empty generator

        Returns:
            {Artefact}: The artefact objects which are within the directory
        """
        # Convert the incoming artefact reference - require that the object exist and that it is a directory
        try:
            _, artobj, artPath = self._splitManagerArtefactForm(artefact)
            if not isinstance(artobj, Directory):
                raise TypeError("Cannot perform ls action on File artefact: {}".format(artobj))

        except exceptions.ArtefactNotFound:
            if ignore_missing:
                return
            raise

        # Yield the contents of the directory
        for subArtefact in artobj.manager._ls(artPath):
            yield subArtefact

            if recursive and isinstance(subArtefact, Directory):
                yield from self.iterls(subArtefact, recursive=recursive)

    def ls(
        self,
        art: typing.Union[Directory, str, None] = None,
        recursive: bool = False,
        *,
        ignore_missing: bool = False
        ) -> typing.Set[typing.Union[File, Directory]]:
        """ List contents of the directory path/artefact given.

        Args:
            art (Directory/str): The Directory artefact or the relpath to the directory to be listed
            recursive (bool) = False: Return subdirectory contents as well
            ignore_missing: bool = False

        Returns:
            {Artefact}: The artefact objects which are within the directory
        """
        return set(self.iterls(art, recursive, ignore_missing=ignore_missing))

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

        try:
            _, artefact, path = self._splitManagerArtefactForm(path)
            if isinstance(artefact, File):
                raise exceptions.OperationNotPermitted("Cannot make a directory as location {} is a file object".format(path))

            if ignoreExists and not overwrite:
                return artefact

        except exceptions.ArtefactNotFound:
            pass

        with tempfile.TemporaryDirectory() as directory:
            return self.put(Directory(utils.connect("FS"), directory), path, overwrite=overwrite)

    def _mklink(self, *args, **kwargs):
        raise NotImplementedError(f'Manager {self} does not support symbolic links')

    def mklink(self, source: Artefact, destination: str) -> Artefact:
        """ Create a symbolic link

        Args:
            source (Artefact): The concrete artefact to belinked to
            destination (str): The path to where the link should be created

        Returns:
            Artefact: The link artefact object
        """
        manager, artefact, path = self._splitManagerArtefactForm(source)
        return manager._mklink(path, destination)

    def touch(self, relpath: str) -> Artefact:
        """ Perform the linux touch command to create a empty file at the path provided, or for existing files, update
        their modified timestamps as if there where just created.

        Args:
            relpath (str): Path to new file location
        """

        manager, artefact, path = self._splitManagerArtefactForm(relpath, require=False)

        if artefact is not None:
            return self.cp(artefact, artefact)

        return self.put(b'', relpath)

    _READONLYMODES = ["r", "rb"]

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

        manager, obj, path = self._splitManagerArtefactForm(artefact, load=mode in self._READONLYMODES)

        with manager.localise(path) as abspath:
            with open(abspath, mode, **kwargs) as handle:
                yield handle

    @contextlib.contextmanager
    def localise(self, artefact: typing.Union[Artefact, str]) -> str:

        # Get the manager instance to handle the localise method
        manager, obj, path = self._splitManagerArtefactForm(artefact, load=False)

        # Call localise on the manager with the path
        with manager.localise(path) as handle:
            yield handle
