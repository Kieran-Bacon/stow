import os
import re
import io
import typing
from typing import (Any, Literal, Union, Optional, Dict, Iterable, Tuple, List, Type, TypeVar, Generic, overload)
from typing_extensions import Self
import urllib
import urllib.parse
import tempfile
import mimetypes
import datetime
import concurrent.futures
import dataclasses
import collections
import functools
import pkg_resources

from .abstract_methods import AbstractManager
from ..worker_config import WorkerPoolConfig
from ..localiser import Localiser
from ..types import StrOrPathLike, TimestampLike, TimestampAble, HashingAlgorithm
from ..storage_classes import StorageClass
from ..artefacts import Artefact, File, Directory, PartialArtefact, ArtefactType, ArtefactOrPathLike, Metadata, FrozenMetadata, Callable
from ..callbacks import AbstractCallback, DefaultCallback
from .. import utils as utils
from .. import exceptions

import logging
log = logging.getLogger(__name__)


_M = TypeVar('_M')

class ParsedURL(Generic[_M]):

    def __init__(self, manager: _M, relpath: str):
        self.manager = manager
        self.relpath = relpath

    def __iter__(self) -> Tuple[_M, str]:
        return (self.manager, self.relpath)

class ManagerReloader:
    """ Class to manage the reloading of a reduced Manager """
    def __new__(cls, protocol: str, config):
        # This will create a new manager if it doesn't exist of fetch the one globally created
        return Manager.connect(protocol, **config)

class Manager(AbstractManager):
    """ Manager Abstract base class - expressed the interface of a Manager which governs a storage
    option and allows extraction and placement of files in that storage container

    """

    SEPARATOR = os.sep
    SEPARATORS = ['\\', '/']
    ISOLATED = False
    SAFE_FILE_OVERWRITE = False
    SAFE_DIRECTORY_OVERWRITE = False

    _READONLYMODES = ["r", "rb"]
    _MULTI_SEP_REGEX = re.compile(r"(\\{2,})|(\/{2,})")
    _RELPATH_REGEX = re.compile(r"^([a-zA-Z0-9]+:)?([/\\\w\.!-_*'() ]*)$")

    # Parsed URL tuple definition

    __MANAGERS = {}
    __INITIALISED_MANAGERS = {}  # TODO replace with weaklink dict

    @classmethod
    def _clearManagerCache(cls):
        cls.__MANAGERS = {}
        cls.__INITIALISED_MANAGERS = {}

    @classmethod
    def find(cls, manager: str) -> Type[Self]:
        """ Fetch the `Manager` class hosted on the 'stow_managers' entrypoint with
        the given name `manager` entry name.

        Args:
            manager: The name of the `Manager` class to be returned

        Returns:
            Manager: The `Manager` class for the manager name provided

        Raises:
            ValueError: In the event that a manager with the provided name couldn't be found
        """

        # Get the manager class for the manager type given - load the manager type if not already loaded
        lmanager = manager.lower()

        if lmanager in cls.__MANAGERS:
            mClass = cls.__MANAGERS[lmanager]

        else:
            foundManagerNames = []

            for entry_point in pkg_resources.iter_entry_points('stow_managers'):

                foundManagerNames.append(entry_point.name)

                if entry_point.name == lmanager:
                    mClass = cls.__MANAGERS[lmanager] = entry_point.load()
                    break

            else:
                raise ValueError(
                    f"Couldn't find a manager called '{manager}'"
                    f" - found {len(foundManagerNames)} managers: {foundManagerNames}"
                )

        return mClass

    @staticmethod
    def _managerIdentifierCalculator(manager_key: str, arguments: dict) -> int:
        identifier = hash((
            manager_key, "-".join([f"{k}-{v}" for k,v in sorted(arguments.items(), key=lambda x: x[0])])
        ))
        return identifier

    @classmethod
    def connect(cls, manager: str, **kwargs) -> Self:
        """ Find and connect to a `Manager` using its name (entrypoint name) and return an instance of that `Manager`
        initialised with the kwargs provided. A path can be provided as the location on the manager for a sub manager to be
        created which will be returned instead.

        Args:
            manager: The name of the manager class
            **kwargs: Keyworded arguments to be passed to the Manager init

        Returns:
            Manager: A storage manager or sub manager which can be used to put and get artefacts

        Note:
            References to `Manager` created by this method are stored to avoid multiple definitions of managers on similar
            locations.

            The stateless interface uses this method as the backend for its functions and as such you can fetch any active
            session by using this function rather than initalising a `Manager` directly
        """

        identifier = cls._managerIdentifierCalculator(manager, kwargs)
        if identifier in cls.__INITIALISED_MANAGERS:
            return cls.__INITIALISED_MANAGERS[identifier]

        # Find the class for the manager and initialise it with the arguments
        managerObj = cls.find(manager)(**kwargs)

        # Get the config for the manager given the defaults
        config = managerObj.config

        # Record against the identifier the mananger object for
        cls.__INITIALISED_MANAGERS[identifier] = managerObj
        cls.__INITIALISED_MANAGERS[cls._managerIdentifierCalculator(manager, config)] = managerObj

        return managerObj

    @classmethod
    @functools.lru_cache
    def parseURL(cls, stowURL: str, default_manager = None) -> ParsedURL:
        """ Parse the passed stow URL and return a ParsedURL a named tuple of manager and relpath

        Example:
            manager, relpath = stow.parseURL("s3://example-bucket/path/to/file)

            result = stow.parseURL("s3://example-bucket/path/to/file)
            result.manager
            result.relpath

        Args:
            stowURL: The path to be parsed and manager identified

        Returns:
            typing.NamedTuple: Holding the manager and relative path of
        """

        # Parse the url provided
        parsedURL = urllib.parse.urlparse(stowURL)

        # Handle protocol managers vs local file system
        if len(parsedURL.scheme) > 1:
            manager = cls.find(parsedURL.scheme)
            scheme = parsedURL.scheme

        elif default_manager is not None:
            return ParsedURL(default_manager, stowURL)

        else:
            manager = cls.find("FS")
            scheme = "FS"

        # Get the signature for the manager from the url
        signature, relpath = manager._signatureFromURL(parsedURL)

        # Has to use connect otherwise it will just create lots and lots of new managers
        return ParsedURL(cls.connect(scheme, **signature), relpath)


    def __contains__(self, artefact: ArtefactOrPathLike) -> bool:
        return self.exists(artefact)

    def __getitem__(self, path: str) -> ArtefactType:
        """ Fetch an artefact from the manager. In the event that it hasn't been cached, look it up on the underlying
        implementation and return a newly created object. If it doesn't exist raise an error

        Args:
            managerPath: The manager relative path to fine the artefact with

        Returns:
            artefact: The artefact at the provided location path

        Raises:
            ArtefactNotFound: In the event that the path does not exist
        """

        log.debug('fetching stat data for %s', path)
        artefact = self._identifyPath(path)
        if artefact is None:
            raise exceptions.ArtefactNotFound(f"No artefact exists at: {path}")
        return artefact

    def __reduce__(self):
        return (ManagerReloader, (self.protocol, self.config,))

    def _ensurePath(self, artefact: ArtefactOrPathLike) -> str:
        """ Collapse artefact into path - Convert an artefact into a path or return. This returns
        the manager relative path instead. To be used when the response is to be in turns of the
        manager. For simple checks, artefacts can be used directly (which uses the abspath)

        Args:
            artefact: The artefact to be ensured is a path str
        """
        return artefact.__fspath__() if isinstance(artefact, os.PathLike) else artefact

    @staticmethod
    def _freezeMetadata(metadata: Metadata, artefact: ArtefactType) -> FrozenMetadata:

        frozenMetadata = {}

        for key, value in metadata.items():
            if callable(value):
                val = value(artefact)
                if isinstance(val, str):
                    frozenMetadata[key] = val

            else:
                frozenMetadata[key] = str(value)

        return frozenMetadata

    @overload
    def _splitArtefactForm(
        self,
        artefact: ArtefactType,
        load: bool = ...,
        require: bool = ...,
        external: bool = ...,
    ) -> typing.Tuple[Self, Union[File, Directory], str]:
        ...
    @overload
    def _splitArtefactForm(
        self,
        artefact: Optional[StrOrPathLike],
        load: bool = ...,
        require: typing.Literal[True] = ...,
        external: bool = ...,
    ) -> typing.Tuple[Self, Union[File, Directory], str]:
        ...
    @overload
    def _splitArtefactForm(
        self,
        artefact: Optional[StrOrPathLike],
        load: bool = ...,
        require: bool = ...,
        external: bool = ...,
    ) -> typing.Tuple[Self, Union[File, Directory, None], str]:
        ...
    def _splitArtefactForm(
        self,
        artefact: Union[ArtefactOrPathLike, str, None],
        load: bool = True,
        require: bool = True,
        external: bool = True,
        ) -> typing.Tuple[Self, Union[File, Directory, None], str]:
        """ Convert the incoming object which could be either an artefact or relative path into a standardised form for
        both such that functions can be easily convert and use what they require

        Args:
            artObj (ArtefactOrPathLike): Either the artefact object or it's relative path to be standardised
            load (bool): Whether to load the object if it exists
            require (bool): Require that the object exists. If False then None when object doesn't exist

        Returns:
            Artefact or None: the artefact object or None if it doesn't exists and require is False
            str: The relative path of the object/the passed value
        """

        if isinstance(artefact, (File, Directory)):
            return artefact._manager, artefact, artefact.path  # type: ignore

        else:

            if self.__class__ == Manager:
                external = True

            obj = None
            default_manager = None if external else self

            if artefact is None:
                if external:
                    manager = self.connect("FS")
                else:
                    manager = self
                path = manager._cwd()

            elif isinstance(artefact, (os.PathLike, str)):
                parsedUrl = self.parseURL(os.fspath(artefact), default_manager=default_manager)
                manager, path = parsedUrl.manager, parsedUrl.relpath

            else:
                t = type(artefact)
                raise TypeError(
                    f"Artefact reference must be either `stow.Artefact` or string not type {t}"
                )

            if load or require:

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

    def manager(self, artefact: ArtefactOrPathLike) -> Self:
        """ Fetch the manager object for the artefact

        Params:
            artefact: The artefact whose manager is to be returned

        Returns:
            Manager: The Manager that produced the artefact
        """
        return self._splitArtefactForm(artefact, external=False)[0]

    @overload
    def artefact(self, path: ArtefactOrPathLike, *, type: Type[File]) -> File:
        ...
    @overload
    def artefact(self, path: ArtefactOrPathLike, *, type: Type[Directory]) -> Directory:
        ...
    @overload
    def artefact(self, path: ArtefactOrPathLike) -> ArtefactType:
        ...
    def artefact(self, path: ArtefactOrPathLike, *, type: Optional[Type[ArtefactType]] = None) -> ArtefactType:
        """ Fetch an artefact object for the given path

        Params:
            stowPath: Manager relative path to artefact

        Returns:
            Arefact: The artefact object

        Raises:
            ArtefactNotFound: In the event that no artefact exists at the location given
        """
        return self._splitArtefactForm(path, external=False)[1]

    def abspath(self, artefact: ArtefactOrPathLike) -> str:
        """ Return a normalized absolute version of the path or artefact given.

        Args:
            artefact: The path or object whose path is to be made absolute and returned

        Returns:
            str: the absolute path of the artefact provided

        Raises:
            ValueError: Cannot make a remote artefact object's path absolute
        """
        manager, _, path = self._splitArtefactForm(artefact, load=False, require=False, external=False)
        return manager._abspath(path)

    def basename(self, artefact: ArtefactOrPathLike) -> str:
        """ Return the base name of an artefact or path. This is the second element of the pair returned by passing path
        to the function `split()`.

        Args:
            artefact: The path or object whose path is to have its base name extracted

        Returns:
            str: the basename
        """
        return os.path.basename(self._ensurePath(artefact))

    def name(self, artefact: ArtefactOrPathLike) -> str:
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

    def extension(self, artefact: ArtefactOrPathLike) -> str:
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

    def commonpath(self, paths: Iterable[ArtefactOrPathLike]) -> str:
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

    def commonprefix(self, paths: Iterable[ArtefactOrPathLike]) -> str:
        """ Return the longest common string literal for a collection of path/artefacts

        Examples:
            commonpath(["/foo/bar", "/foo/ban/pip"]) == "/foo/ba"

        Args:
            paths: Artefact/paths who will have their paths comparied to find a common path

        Returns:
            str: A string that all paths startwith (may be empty string)
        """
        return os.path.commonprefix([self._ensurePath(path) for path in paths])

    def dirname(self, artefact: ArtefactOrPathLike) -> str:
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

    def isfile(self, artefact: ArtefactOrPathLike) -> bool:
        """ Check if the artefact provided is a file

        Args:
            artefact: The artefact to be checked

        Returns:
            Bool: True or False in answer to the question
        """
        return isinstance(self._splitArtefactForm(artefact, require=False, external=False)[1], File)

    def isdir(self, artefact: ArtefactOrPathLike) -> bool:
        """ Check if the artefact provided is a directory

        Args:
            artefact: The artefact to be checked

        Returns:
            Bool: True or False in answer to the question
        """
        return isinstance(self._splitArtefactForm(artefact, require=False, external=False)[1], Directory)

    def islink(self, artefact: ArtefactOrPathLike) -> bool:
        """ Check if the artefact provided is a link

        Will check for local managers but will default to False for remote managers

        Args:
            artefact: The artefact to be checked

        Returns:
            Bool: True or False in answer to the question
        """
        manager, _, path = self._splitArtefactForm(artefact, load=False, require=False, external=False)
        return manager._isLink(path)

    def ismount(self, artefact: ArtefactOrPathLike) -> bool:
        """ Check if the artefact provided is a link

        Will check for local managers but will default to False for remote managers

        Args:
            artefact: The artefact to be checked

        Returns:
            Bool: True or False in answer to the question
        """
        manager, _, path = self._splitArtefactForm(artefact, load=False, require=False)
        return manager._isMount(path)

    def getctime(self, artefact: ArtefactOrPathLike) -> float:
        """ Get the created time for the artefact as a UTC timestamp

        Args:
            artefact: the artefact whose creation datetime is to be returned

        Returns:
            timestamp: a float timestamp of creation time if manager holds such information else None

        Raises:
            ArtefactNotFound: If there is no artefact at the location
            ArtefactMetadataUndefined: The storage medium does not record artefact create time.
        """
        try:
            return self._splitArtefactForm(artefact, require=True, external=False)[1].createdTime.timestamp()
        except:
            raise exceptions.ArtefactMetadataUndefined(f"Artefact {artefact} does not have a created time recorded")

    def getmtime(self, artefact: ArtefactOrPathLike) -> float:
        """ Get the modified time for the artefact as a UTC timestamp

        Args:
            artefact: the artefact whose modified datetime is to be returned

        Returns:
            timestamp: a float timestamp of modified time if manager holds such information else
                None

        Raises:
            ArtefactNotFound: If there is no artefact at the location
        """
        try:
            return self._splitArtefactForm(
                artefact,
                require=True,
                external=False
            )[1].modifiedTime.timestamp()  # type: ignore
        except AttributeError:
                raise exceptions.ArtefactMetadataUndefined(
                    f"Artefact {artefact} does not have a modified time recorded"
                )

    def _setmtime(self, *args, **kwargs):
        raise NotImplementedError(
            f"Managers of type {type(self)} do not support modified time updates"
        )
    def setmtime(
        self,
        artefact: ArtefactOrPathLike,
        _datetime: Union[float, datetime.datetime]
        ) -> datetime.datetime:
        """ Update the artefacts modified time

        Args:
            artefact (Artefact): The artefact to update
            _datetime (float, datetime): The time to set against the artefact
        """
        mtime, _ = self.set_artefact_time(artefact, modified_datetime=_datetime)
        return mtime

    def getatime(self, artefact: ArtefactOrPathLike) -> float:
        """ Get the accessed time for the artefact as a UTC timestamp

        Args:
            artefact: the artefact whose accessed datetime is to be returned

        Returns:
            timestamp: a float timestamp of accessed time if manager holds such information else
                None

        Raises:
            ArtefactNotFound: If there is no artefact at the location
        """
        try:
            return self._splitArtefactForm(artefact, require=True, external=False)[1].accessedTime.timestamp()
        except:
            raise exceptions.ArtefactMetadataUndefined(f"Artefact {artefact} does not have a accessed time recorded")

    def _setatime(self, *args, **kwargs):
        raise NotImplementedError(
            f"Managers of type {type(self)} do not support access time updates"
        )
    def setatime(
        self,
        artefact: ArtefactOrPathLike,
        _datetime: Union[float, datetime.datetime]
        ) -> datetime.datetime:
        """ Update the artefacts access time

        Args:
            artefact (Artefact): The artefact to update
            _datetime (float, datetime): The time to set against the artefact
        """
        _, atime = self.set_artefact_time(artefact, accessed_datetime=_datetime)
        return atime

    def _set_artefact_time(
            self,
            artefact: ArtefactOrPathLike,
            modified_time: Optional[TimestampLike] = None,
            accessed_time: Optional[TimestampLike] = None
        ) -> Tuple[datetime.datetime, datetime.datetime]:
        raise NotImplementedError(f'Manager {self} does not implement setting artefact modified or accessed times')

    def set_artefact_time(
        self,
        artefact: ArtefactOrPathLike,
        modified_datetime: Optional[TimestampLike] = None,
        accessed_datetime: Optional[TimestampLike] = None
        ) -> Tuple[datetime.datetime, datetime.datetime]:
        """ Update the artefacts access time """
        manager, artefact, _ = self._splitArtefactForm(artefact, require=True, external=False)
        return manager._set_artefact_time(artefact, modified_datetime, accessed_datetime)

    def exists(self, *artefacts: ArtefactOrPathLike) -> bool:
        """ Return true if the given artefact is a member of the manager, or the path is correct for the manager and it
        leads to a File or Directory.

        Does not handle protocols

        Args:
            artefact: Artefact or path whose existence is to be checked

        Returns:
            bool: True if artefact exists else False
        """
        for artefact in artefacts:
            manager, _, path = self._splitArtefactForm(artefact, load=False, require=False, external=False)
            if not manager._exists(path):
                return False
        return True

    def lexists(self, artefact: ArtefactOrPathLike) -> bool:
        """ Return true if the given artefact is a member of the manager, or the path is correct for the manager and it
        leads to a File or Directory.

        Does not handle protocols

        Args:
            artefact: Artefact or path whose existence is to be checked

        Returns:
            bool: True if artefact exists else False
        """
        return os.path.lexists(artefact)

    # @classmethod
    def join(self, *paths: ArtefactOrPathLike, separator=None, joinAbsolutes: bool = False) -> str:
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

        separator = separator or self.SEPARATOR

        parsedResult = None  # Store the network information while path is joined
        joined = ""  # Constructed path

        for segment in paths:

            if isinstance(segment, Artefact):
                # Convert artefacts to paths
                segment = segment.path

            elif isinstance(segment, os.PathLike):
                segment = os.fspath(segment)

            if not segment:
                continue

            # Identify and record the last full
            presult = urllib.parse.urlparse(segment)
            if presult.scheme:
                parsedResult = presult
                segment = presult.path

            if joined:
                # A path is in the midst of being created

                if any(segment.startswith(sep) for sep in self.SEPARATORS):
                    if joinAbsolutes:
                        joined = joined.rstrip('\\/') + segment

                    else:
                        joined = segment

                else:
                    if any(joined.endswith(sep) for sep in self.SEPARATORS):
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
    def normpath(path: StrOrPathLike) -> str:
        """ Normalize a pathname by collapsing redundant separators and up-level references so that A//B, A/B/, A/./B
        and A/foo/../B all become A/B.

        Args:
            path: the path whose to be

        Returns:
            str: The path transformed
        """
        # Check that the url is for a remote manager
        url = urllib.parse.urlparse(os.fspath(path))
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

    def relpath(self, path: StrOrPathLike, start: StrOrPathLike = os.curdir, separator: str = os.sep) -> str:
        """ Return a relative filepath to path either from the current directory or from an optional start directory

        Args:
            path: the path to be made relative
            start: the location to become relative to
        """
        relpath = os.path.relpath(path, start)
        return relpath if separator == os.sep else relpath.replace(os.sep, separator)

    def samefile(self, artefact1: ArtefactOrPathLike, artefact2: ArtefactOrPathLike) -> bool:
        """ Check if provided artefacts are represent the same data on disk

        Args:
            artefact1: An artefact object or path
            artefact2: An artefact object or path

        Returns:
            bool: True if the artefacts are the same
        """
        return os.path.samefile(artefact1, artefact2)

    @staticmethod
    def sameopenfile(handle1: int, handle2: int) -> bool:
        """ Return True if the file descriptors fp1 and fp2 refer to the same file.
        """
        return os.path.sameopenfile(handle1, handle2)

    def samestat(self, artefact1: os.stat_result, artefact2: os.stat_result) -> bool:
        """ Check if provided artefacts are represent the same data on disk

        Args:
            artefact1: An artefact object or path
            artefact2: An artefact object or path

        Returns:
            bool: True if the artefacts are the same
        """
        return os.path.samestat(artefact1, artefact2)

    def split(self, artefact: ArtefactOrPathLike) -> typing.Tuple[str, str]:
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

    def splitext(self, artefact: ArtefactOrPathLike) -> typing.Tuple[str, str]:
        """ Split the pathname path into a pair (root, ext) such that root + ext == path, and ext is empty or begins
        with a period and contains at most one period.

        Args:
            artefact: the artefact to have the extension extracted

        Returns:
            (root, ext): The root path without the extension and the extension
        """
        return os.path.splitext(artefact)

    def digest(
        self,
        artefact: Union[File, str],
        algorithm: HashingAlgorithm = HashingAlgorithm.MD5
        ):

        manager, obj, _ = self._splitArtefactForm(artefact, external=False)
        if isinstance(obj, File):
            return manager._digest(obj, algorithm)

        else:
            raise TypeError(f'Cannot get file digest for directory {obj}')

    @overload
    def get(
        self,
        source: ArtefactOrPathLike,
        destination: Literal[None] = None,
        overwrite: bool = False,
        *,
        callback: AbstractCallback = DefaultCallback(),
        modified_time: Optional[TimestampLike] = None,
        accessed_time: Optional[TimestampLike] = None,
        worker_config: Optional[WorkerPoolConfig] = None,
        ) -> bytes:
        pass
    @overload
    def get(
        self,
        source: ArtefactOrPathLike,
        destination: str,
        overwrite: bool = False,
        *,
        callback: AbstractCallback = DefaultCallback(),
        modified_time: Optional[TimestampLike] = None,
        accessed_time: Optional[TimestampLike] = None,
        worker_config: Optional[WorkerPoolConfig] = None,
        ) -> ArtefactType:
        pass
    def get(
        self,
        source: ArtefactOrPathLike,
        destination: typing.Optional[str] = None,
        overwrite: bool = False,
        *,
        callback: AbstractCallback = DefaultCallback(),
        modified_time: Optional[TimestampLike] = None,
        accessed_time: Optional[TimestampLike] = None,
        worker_config: Optional[WorkerPoolConfig] = None,
        ) -> Union[Artefact, bytes]:
        """ Get an artefact from a local or remote source and download the artefact either to a local artefact or as bytes

        Args:
            source (Artefact|str): The source artefact to be downloaded
            destination (str) = None: The local path to write the artefact. If None return file as bytes
            overwrite (bool) = False: local directory protection - to overwrite a directory with overwrite must be True

        Return:
            Artefact|bytes: The local artefact downloaded, or the bytes of the source artefact.
        """

        worker_config = worker_config or WorkerPoolConfig(shutdown=True)

        try:
            # Split into object and path - Ensure that the artefact to get is from this manager
            manager, obj, _ = self._splitArtefactForm(source, external=False)

            # Ensure the destination - Remove or raise issue for a local artefact at the location where the get is called
            if destination is not None:
                localManager: Manager = self.connect(manager="FS")
                destinationAbspath = os.path.join(os.getcwd(), destination)

                if os.path.exists(destinationAbspath):
                    localManager.rm(destinationAbspath, recursive=overwrite)

                else:
                    # Ensure the directory that this object exists with
                    os.makedirs(self.dirname(destinationAbspath), exist_ok=True)

                # Get the object using the underlying manager implementation
                manager._get(
                    obj,
                    destinationAbspath,
                    callback=callback,
                    modified_time=utils.timestampToFloatOrNone(modified_time),
                    accessed_time=utils.timestampToFloatOrNone(accessed_time),
                    worker_config=worker_config
                )

                # Load the downloaded artefact from the local location and return
                gottenArtefact = PartialArtefact(self.connect(manager="FS"), destination)

            else:
                if not isinstance(obj, File):
                    raise exceptions.ArtefactTypeError("Cannot get file bytes of {}".format(obj))
                gottenArtefact = manager._getBytes(obj, callback=callback)

            return gottenArtefact

        finally:
            worker_config.conclude()

    def _overwrite(
            self,
            manager: "Manager",
            artefact: Optional[ArtefactType],
            overwrite: bool,
            callback: AbstractCallback,
            worker_config: WorkerPoolConfig
        ):

        if artefact is None:
            return

        if isinstance(artefact, File):
            if manager.SAFE_FILE_OVERWRITE:
                return
        else:
            if not overwrite and not artefact.isEmpty():
                raise exceptions.OperationNotPermitted('Cannot overwrite %s as it is not empty - pass overwrite=True')

            if manager.SAFE_DIRECTORY_OVERWRITE:
                return

        return self.rm(
            artefact,
            recursive=overwrite,
            callback=callback,
            ignore_missing=True,
            worker_config=worker_config
        )

    @overload
    def put(
        self,
        source: Union[File, bytes],
        destination: ArtefactOrPathLike,
        overwrite: bool = False,
        *,
        callback: AbstractCallback = DefaultCallback(),
        metadata: Optional[Metadata] = None,
        modified_time: Optional[TimestampLike] = None,
        accessed_time: Optional[TimestampLike] = None,
        content_type: Optional[str] = None,
        storage_class: Optional[StorageClass] = None,
        worker_config: Optional[WorkerPoolConfig] = None,
        ) -> File:
        pass
    @overload
    def put(
        self,
        source: Directory,
        destination: ArtefactOrPathLike,
        overwrite: bool = False,
        *,
        metadata: Optional[Metadata] = None,
        callback: AbstractCallback = DefaultCallback(),
        modified_time: Optional[TimestampLike] = None,
        accessed_time: Optional[TimestampLike] = None,
        content_type: Optional[str] = None,
        storage_class: Optional[StorageClass] = None,
        worker_config: Optional[WorkerPoolConfig] = None,
        ) -> Directory:
        pass
    @overload
    def put(
        self,
        source: ArtefactOrPathLike,
        destination: ArtefactOrPathLike,
        overwrite: bool = False,
        *,
        metadata: Optional[Metadata] = None,
        callback: AbstractCallback = DefaultCallback(),
        modified_time: Optional[TimestampLike] = None,
        accessed_time: Optional[TimestampLike] = None,
        content_type: Optional[str] = None,
        storage_class: Optional[StorageClass] = None,
        worker_config: Optional[WorkerPoolConfig] = None,
        ) -> ArtefactType:
        pass
    def put(
        self,
        source: Union[ArtefactOrPathLike, bytes],
        destination: ArtefactOrPathLike,
        overwrite: bool = False,
        *,
        metadata: Optional[Metadata] = None,
        callback: AbstractCallback = DefaultCallback(),
        modified_time: Optional[TimestampLike] = None,
        accessed_time: Optional[TimestampLike] = None,
        content_type: Optional[str] = None,
        storage_class: Optional[StorageClass] = None,
        worker_config: Optional[WorkerPoolConfig] = None,
        ) -> ArtefactType:
        """ Put a local artefact onto the remote at the location given.

        Args:
            source (Artefact, str, bytes): The artefact to be put, either artefact, path to file or file bytes.
            destination (Artefact, str): The artefact of the location object or the path to destination.
            overwrite (bool) = False: Protection against overwritting directories
            *,
            metadata (Dict[str,str]): A dictionary of metadata to write with the file artefact
            callback (AbstractCallback): A callback method to monitor file upload progress
            modified_time (Optional[datetime.datetime]): The modified time of the new artefact (if manager supports)
            accessed_time (Optional[datetime.datetime]): The accessed time of the new artefact (if manager supports)

        Returns:

        """

        worker_config = worker_config or WorkerPoolConfig(shutdown=True)

        try:
            # Load in the information about the destination
            destinationManager, destinationObj, destinationPath = self._splitArtefactForm(
                destination, require=False, external=False
            )

            # Note - we are not deleting the destination until after we have validated the source
            if isinstance(source, (bytes, bytearray, memoryview)):

                self._overwrite(
                    destinationManager,
                    destinationObj,
                    overwrite=overwrite,
                    callback=callback,
                    worker_config=worker_config
                )

                putArtefact = destinationManager._putBytes(
                    source,
                    destinationPath,
                    metadata=metadata,
                    callback=callback,
                    modified_time=utils.timestampToFloatOrNone(modified_time),
                    accessed_time=utils.timestampToFloatOrNone(accessed_time),
                    content_type=content_type,
                    storage_class=storage_class,
                )

            else:

                # Validate source before deleting destination
                _, sourceObj, _ = self._splitArtefactForm(source)

                self._overwrite(
                    destinationManager,
                    destinationObj,
                    overwrite=overwrite,
                    callback=callback,
                    worker_config=worker_config
                )

                putArtefact = destinationManager._put(
                    sourceObj,
                    destinationPath,
                    metadata=metadata,
                    callback=callback,
                    modified_time=modified_time,
                    accessed_time=accessed_time,
                    content_type=content_type,
                    storage_class=storage_class,
                    worker_config=worker_config
                )

            return putArtefact

        finally:
            worker_config.conclude()

    def cp(
        self,
        source: ArtefactOrPathLike,
        destination: ArtefactOrPathLike,
        overwrite: bool = False,
        *,
        callback: AbstractCallback = DefaultCallback(),
        metadata: Optional[Metadata] = None,
        modified_time: Optional[datetime.datetime] = None,
        accessed_time: Optional[datetime.datetime] = None,
        storage_class: Optional[StorageClass] = None,
        content_type: Optional[str] = None,
        worker_config: Optional[WorkerPoolConfig] = None,
        ) -> ArtefactType:
        """ Copy the artefacts at the source location to the provided destination location. Overwriting items at the
        destination.

        Args:
            source: source path or artefact
            destination: destination path or artefact
            overwrite: Whether to overwrite directories my move

        Returns:
            Artefact: The destination artefact object
        """

        worker_config = worker_config or WorkerPoolConfig(shutdown=True)

        try:
            log.debug('copying %s into %s', source, destination)

            # Load the source object that is to be copied
            sourceManager, sourceObj, _ = self._splitArtefactForm(source, external=False)
            destinationManager, destinationObj, destinationPath = self._splitArtefactForm(
                destination, require=False, external=False
            )

            # Prevent the overwriting of a directory without permission
            self._overwrite(
                destinationManager,
                destinationObj,
                overwrite=overwrite,
                callback=callback,
                worker_config=worker_config
            )

            # Check if the source and destination are from the same manager class
            # TODO it may not be possible to copy from one manager of the same type to another manager of the same type
            # but be possible to copy within a manager - need more dials for this.
            if type(sourceManager) == type(destinationManager) and not sourceManager.ISOLATED:
                copiedArtefact = destinationManager._cp(
                    sourceObj,
                    destinationPath,
                    callback=callback,
                    metadata=metadata,
                    modified_time=utils.timestampToFloatOrNone(modified_time),
                    accessed_time=utils.timestampToFloatOrNone(accessed_time),
                    storage_class=storage_class,
                    content_type=content_type,
                    worker_config=worker_config,
                )

            else:

                log.warning('Cannot perform copy on manager - defaulting to put for %s->%s', source, destination)
                copiedArtefact = self.put(sourceObj, destination, callback=callback, worker_config=worker_config)

            return copiedArtefact

        finally:
            worker_config.conclude()

    def mv(
        self,
        source: ArtefactOrPathLike,
        destination: ArtefactOrPathLike,
        overwrite: bool = False,
        *,
        callback: AbstractCallback = DefaultCallback(),
        metadata: Optional[Metadata] = None,
        content_type: Optional[str] = None,
        modified_time: Optional[datetime.datetime] = None,
        accessed_time: Optional[datetime.datetime] = None,
        storage_class: Optional[StorageClass] = None,
        worker_config: Optional[WorkerPoolConfig] = None,
        ) -> ArtefactType:
        """ Copy the artefacts at the source location to the provided destination location. Overwriting items at the
        destination.

        Args:
            source: source path or artefact
            destination: destination path or artefact
            overwrite: Whether to overwrite directories my move

        Returns:
            Artefact: The destination artefact object (source object updated if source was on manager originally)
        """

        worker_config = worker_config or WorkerPoolConfig(shutdown=True)

        try:
            # Load the source object that is to be copied
            sourceManager, sourceObj, _ = self._splitArtefactForm(source, external=False)
            destinationManager, destinationObj, destinationPath = self._splitArtefactForm(
                destination, require=False, external=False
            )

            # Prevent the overwriting of a directory without permission
            self._overwrite(
                destinationManager,
                destinationObj,
                overwrite=overwrite,
                callback=callback,
                worker_config=worker_config
            )

            # Check if the source and destination are from the same manager class
            if type(sourceManager) == type(destinationManager) and not sourceManager.ISOLATED:
                movedArtefact = destinationManager._mv(
                    sourceObj,
                    destinationPath,
                    callback=callback,
                    metadata=metadata,
                    modified_time=utils.timestampToFloatOrNone(modified_time),
                    accessed_time=utils.timestampToFloatOrNone(accessed_time),
                    storage_class=storage_class,
                    content_type=content_type,
                    worker_config=worker_config or WorkerPoolConfig(shutdown=True),
                )

            else:

                # Moving between manager types - put the object and then delete the old one
                movedArtefact = self.put(
                    sourceObj,
                    destination,
                    overwrite=overwrite,
                    callback=callback,
                    metadata=metadata,
                    modified_time=utils.timestampToFloatOrNone(modified_time),
                    accessed_time=utils.timestampToFloatOrNone(accessed_time),
                    storage_class=storage_class,
                    worker_config=worker_config,
                    content_type=content_type,
                )
                sourceManager._rm(sourceObj.path, callback=callback, worker_config=worker_config)

            return movedArtefact

        finally:
            worker_config.conclude()

    def rm(
        self,
        *artefacts: Union[ArtefactOrPathLike, List[ArtefactOrPathLike]],
        recursive: bool = False,
        callback: AbstractCallback = DefaultCallback(),
        ignore_missing: bool = False,
        worker_config: Optional[WorkerPoolConfig] = None
        ) -> None:
        """ Remove an artefact from the manager using the artefact object or its relative path. If its a directory,
        remove it if it is empty, or all of its contents if recursive has been set to true.

        Args:
            artefact (ArtefactOrPathLike): the object which is to be deleted
            recursive (bool) = False: whether to accept the deletion of a directory which has contents
        """

        worker_config = worker_config or WorkerPoolConfig(shutdown=True)

        # For each of the managers groups created to delete the items
        groupedDeletes: Dict[Self, List[str]] = {}

        for artefactOrGroup in artefacts:
            for artefact in (artefactOrGroup if not isinstance(artefactOrGroup, (str, os.PathLike)) else (artefactOrGroup,)):

                # Parse the passed arguments into their components - only load if necessary
                manager, obj, path = self._splitArtefactForm(artefact, load=False, require=not ignore_missing or not recursive, external=False)

                if not recursive and isinstance(obj, Directory) and not obj.isEmpty():
                    raise exceptions.OperationNotPermitted(
                        "Cannot delete a container object that isn't empty - set recursive to True to proceed"
                    )

                groupedDeletes.setdefault(manager, []).append(path)

        try:
            for manager, group in groupedDeletes.items():
                manager._rm(*group, callback=callback, worker_config=worker_config)

        finally:
            worker_config.conclude()


    def _sync(
        self,
        sourceManager: "Manager",
        sourceObj: ArtefactType,
        destinationManager: "Manager",
        destinationObj: Union[ArtefactType, str],

        delete: bool,
        check_modified_times: bool,
        artefact_comparator: Optional[typing.Callable[[File, File], bool]],
        callback: AbstractCallback,
        overwrite: bool,

        sync_method: Callable,
        sync_arguments: Dict[str, Any],
        worker_config: WorkerPoolConfig
    ) -> ArtefactType:

        callback.reviewing(1)

        if isinstance(destinationObj, str):
            # The destination doesn't exist - sync the entire source

            sync_method(
                sourceObj,
                destinationObj,
                **sync_arguments
            )

        elif isinstance(destinationObj, File):
            if isinstance(sourceObj, Directory):
                # The source is a directory - we simply replace the file
                callback.deleting(1)
                destinationManager._rm(destinationObj.path, callback=callback, worker_config=worker_config)
                sync_method(
                    sourceObj,
                    destinationObj.path,
                    **sync_arguments
                )

            elif (
                (not check_modified_times or destinationObj.modifiedTime < sourceObj.modifiedTime) and
                (artefact_comparator is None or not artefact_comparator(sourceObj, destinationObj))
                ):

                if not destinationManager.SAFE_FILE_OVERWRITE:
                    callback.deleting(1)
                    destinationManager._rm(destinationObj.path, callback=callback, worker_config=worker_config)

                sync_method(
                    sourceObj,
                    destinationObj.path,
                    **sync_arguments
                )

            else:
                callback.reviewed(1)
                log.debug('%s already synced', destinationObj)

        else:
            # Desintation object is a dictionary
            if isinstance(sourceObj, File):
                # We are trying to sync a file to a directory - this is a put

                if not overwrite:
                    raise exceptions.OperationNotPermitted(
                        f'During sync operation - cannot overwrite directory [{destinationObj}] with file [{sourceObj}] as overwrite has not been set to true')

                if not destinationManager.SAFE_DIRECTORY_OVERWRITE:
                    callback.deleting(1)
                    destinationManager._rm(destinationObj.path, callback=callback, worker_config=worker_config)

                sync_method(
                    sourceObj,
                    destinationObj.path,
                    **sync_arguments
                )

            else:

                # Syncing a source directory to a destination directory
                destinationMap = {artefact.basename: artefact for artefact in destinationManager.ls(destinationObj)}

                # Recursively fill in destination at this recursion level
                for artefact in sourceManager.iterls(sourceObj):
                    if artefact.basename in destinationMap:

                        self._sync(
                            sourceManager,
                            artefact,
                            destinationManager,
                            destinationMap.pop(artefact.basename),
                            check_modified_times=check_modified_times,
                            artefact_comparator=artefact_comparator,
                            delete=delete,
                            callback=callback,
                            overwrite=overwrite,
                            worker_config=worker_config,
                            sync_method=sync_method,
                            sync_arguments=sync_arguments
                        )

                    else:
                        callback.reviewing(1)

                        sync_method(
                            artefact,
                            destinationManager.join(destinationObj.path, artefact.basename),
                            **sync_arguments
                        )

                # Any remaining destionation objects were not targets of sync - delete if argument passed
                if delete:
                    callback.reviewing(len(destinationMap))

                    delete_targets = [artefact.path for artefact in destinationMap.values()]

                    destinationManager._rm(
                        *delete_targets,
                        callback=callback,
                        worker_config=worker_config
                    )

                callback.reviewed(1)

        worker_config.conclude()

        return PartialArtefact(destinationManager, destinationObj if isinstance(destinationObj, str) else destinationObj.path)

    def sync(
        self,
        source: Union[File, Directory, str],
        destination: ArtefactOrPathLike,
        *,
        delete: bool = False,
        check_modified_times: bool = True,
        artefact_comparator: Optional[typing.Callable[[File, File], bool]] = None,

        content_type: Optional[str] = None,
        storage_class: Optional[StorageClass] = None,
        metadata: Optional[Metadata] = None,
        modified_time: Optional[datetime.datetime] = None,
        accessed_time: Optional[datetime.datetime] = None,
        overwrite: bool = False,
        worker_config: Optional[WorkerPoolConfig] = None,
        callback: AbstractCallback = DefaultCallback()
        ) -> ArtefactType:
        """ Put artefacts from the source location into the destination location if they have more recently been edited.

        Args:
            source (Directory): source directory artefact
            destination (Directory): destination directory artefact on the manager
            delete: Togger the deletion of artefacts that are members of the destination which do not conflict with
                the source.
            check_modified_times (bool): Prevent sync even if digest is different, if destination is newer than source
            artefact_comparator: (Callable): Callable used to compare a source and destination object, Should return True when items are equal (which will mean that there is no action to perform)

        Raises:
            ArtefactNotFound: In the event that the source directory doesn't exist
        """

        if not check_modified_times and artefact_comparator is None:
            log.warning(f'You are running a sync command with no condition checking at all between {source} and {destination}')

        # Fetch the source object
        sourceManager, sourceObj, _ = self._splitArtefactForm(source, require=True, external=False)

        # Fetch the destination object
        destinationManager, destinationObj, destinationPath = self._splitArtefactForm(destination, require=False)

        if destinationObj is None:
            log.warning("Syncing: Destination=%s doesn't exist therefore putting entire source=%s", destinationPath, sourceObj)
            return destinationManager.put(
                source,
                destinationPath,
                overwrite=overwrite,
                callback=callback,
                metadata=metadata,
                modified_time=modified_time,
                accessed_time=accessed_time,
                storage_class=storage_class,
                worker_config=worker_config
            )

        # Setup the worker_pool config
        worker_config = worker_config or WorkerPoolConfig(shutdown=True)
        extended_config = worker_config.extend()

        if type(sourceManager) == type(destinationManager) and not sourceManager.ISOLATED:
            sync_method = destinationManager._cp

        else:
            sync_method = destinationManager._put

        sync_arguments = {
            "metadata": metadata,
            "modified_time": utils.timestampToFloatOrNone(modified_time),
            "accessed_time": utils.timestampToFloatOrNone(accessed_time),
            "storage_class": storage_class,
            "overwrite": overwrite,
            "callback": callback,
            "content_type": content_type,
            "worker_config": extended_config
        }

        try:
            return self._sync(
                sourceManager,
                sourceObj,
                destinationManager,
                (destinationObj if destinationObj is not None else destinationPath),
                delete,
                check_modified_times,
                artefact_comparator,
                callback=callback,
                overwrite=overwrite,
                sync_method=sync_method,
                sync_arguments=sync_arguments,
                worker_config=extended_config
            )
        except:
            worker_config.conclude(cancel=True)
            raise

        finally:
            worker_config.conclude()

    def iterls(
        self,
        artefact: Union[Directory, str, None] = None,
        recursive: bool = False,
        *,
        ignore_missing: bool = False,
        include_metadata: bool = False,
        worker_config: Optional[WorkerPoolConfig] = None,
        ) -> typing.Generator[ArtefactType, None, None]:
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
            manager, artobj, artPath = self._splitArtefactForm(artefact, external=False)
            if not isinstance(artobj, Directory):
                raise TypeError("Cannot perform ls action on File artefact: {}".format(artobj))

        except exceptions.ArtefactNotFound:
            if ignore_missing:
                return
            raise

        # Yield the contents of the directory
        worker_config = worker_config or WorkerPoolConfig(shutdown=True)
        yield from manager._ls(
            artPath,
            recursive=recursive,
            include_metadata=include_metadata,
            worker_config=worker_config,
        )
        worker_config.conclude()

    def ls(
        self,
        art: Union[Directory, str, None] = None,
        recursive: bool = False,
        *,
        ignore_missing: bool = False,
        include_metadata: bool = False,
        worker_config: Optional[WorkerPoolConfig] = None,
        ) -> typing.Set[Union[File, Directory]]:
        """ List contents of the directory path/artefact given.

        Args:
            art (Directory/str): The Directory artefact or the relpath to the directory to be listed
            recursive (bool) = False: Return subdirectory contents as well
            ignore_missing: bool = False

        Returns:
            {Artefact}: The artefact objects which are within the directory
        """
        return set(self.iterls(art, recursive, ignore_missing=ignore_missing, include_metadata=include_metadata, worker_config=worker_config))

    def mkdir(self, path: str, ignore_exists: bool = True, overwrite: bool = False) -> Directory:
        """ Make a directory at the location of the path provided. By default - do nothing in the event that the
        location is already a directory object.

        Args:
            path (str): Relpath to the location where a directory is to be created
            ignore_exists (bool) = True: Whether to do nothing if a directory already exists
            overwrite (bool) = False: Whether to overwrite the directory with an empty directory

        Returns:
            Directory: The directory at the given location - it may have been created as per the call

        Raises:
            OperationNotPermitted: In the event that you try to overwrite a directory that already exists without
                passing the overwrite flag
        """

        try:
            _, artefact, path = self._splitArtefactForm(path, external=False)
            if isinstance(artefact, File):
                raise exceptions.OperationNotPermitted("Cannot make a directory as location {} is a file object".format(path))

            if ignore_exists and not overwrite:
                return artefact

        except exceptions.ArtefactNotFound:
            pass

        with tempfile.TemporaryDirectory() as directory:
            return self.put(
                Directory(self.connect('FS'), directory),
                # self._splitArtefactForm(directory)[1],
                path,
                overwrite=overwrite
            )

    def _mklink(self, *args, **kwargs):
        raise NotImplementedError(f'Manager {self} does not support links')

    def mklink(self, source: ArtefactOrPathLike, destination: str, soft: bool = True) -> ArtefactType:
        """ Create a symbolic link

        Args:
            artefact (Artefact): The concrete artefact the link points to
            link (str): The path of the link - the link location
            soft (bool): Indicate whether the link should be soft (hard being the alternative)

        Returns:
            Artefact: The link artefact object
        """
        manager, artefact, path = self._splitArtefactForm(source, require=False, external=False)
        return manager._mklink(path, destination, soft)

    def touch(
        self,
        relpath: str,
        modified_time: Optional[TimestampLike] = None,
        accessed_time: Optional[TimestampLike] = None,
        *,
        metadata: Optional[Metadata] = None,
        content_type: Optional[str] = None,
        storage_class: Optional[StorageClass] = None
        ) -> File:
        """ Perform the linux touch command to create a empty file at the path provided, or for existing files, update
        their modified timestamps as if there where just created.

        Args:
            relpath (str): Path to new file location
        """
        manager, artefact, path = self._splitArtefactForm(relpath, require=False, external=False)

        if artefact is not None:
            if isinstance(artefact, Directory):
                raise ValueError(f'Cannot touch directory: {artefact}')
            log.debug("artefact=%s already exists - updating artefact times mt=%s at=%s", path, modified_time, accessed_time, extra={'method': 'touch'})
            try:
                manager._set_artefact_time(artefact, modified_time=modified_time,accessed_time=accessed_time)
            except NotImplementedError:
                log.warning('%s does not support modifying artefact [%s] times', manager, artefact)
            return artefact

        else:
            log.debug("creating artefact=%s with times mt=%s at=%s", path, modified_time, accessed_time, extra={'method': 'touch'})
            return manager._putBytes(
                b'',
                path,
                metadata=metadata,
                callback=DefaultCallback(),
                modified_time=utils.timestampToFloatOrNone(modified_time),
                accessed_time=utils.timestampToFloatOrNone(accessed_time),
                storage_class=storage_class,
                content_type=content_type,
            )


    _READONLYMODES = ["r", "rb"]

    def open(self, artefact: Union[File, str], mode: str = "r", **kwargs) -> typing.IO[typing.AnyStr]:
        """ Open a file and create a stream to that file. Expose interface of `open`

        Args:
            artefact: The object that represents the file (or path to the file) to be openned by this manager
            mode: The open method
            kwargs: kwargs to be passed to the interface of open

        Yields:
            io.IOBase: An IO object depending on the mode for interacting with the file
        """

        # Parse the artefact
        shouldLoad = mode in self._READONLYMODES
        manager, obj, path = self._splitArtefactForm(artefact, load=shouldLoad, require=shouldLoad, external=False)

        # Setup a localiser for the artefact
        localiser = manager.localise(obj or path)
        abspath = localiser.start()

        # Create a handle to the file - update the close to close the localiser
        handle = open(abspath, mode, **kwargs)
        _close = handle.close
        def closer():
            _close()
            localiser.close()
        handle.close = closer

        return handle

    def localise(self, artefact: ArtefactOrPathLike) -> Localiser:

        # Get the manager instance to handle the localise method
        manager, obj, path = self._splitArtefactForm(artefact, load=False, external=False)

        return manager.localise(obj or path)
