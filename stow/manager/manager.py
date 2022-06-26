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
from .reloader import ManagerSeralisable

from ..class_interfaces import ManagerInterface, LocalInterface, RemoteInterface
from ..artefacts import Artefact, File, Directory, SubFile, SubDirectory
from .. import utils
from .. import exceptions

import logging
log = logging.getLogger(__name__)

class Manager(AbstractManager, ClassMethodManager, ManagerInterface, ManagerSeralisable):
    """ Manager Abstract base class - expressed the interface of a Manager which governs a storage option and allows
    extraction and placement of files in that storage container

    """

    _READONLYMODES = ["r", "rb"]

    _MULTI_SEP_REGEX = re.compile(r"(\\{2,})|(\/{2,})")
    _RELPATH_REGEX = re.compile(r"^([a-zA-Z0-9]+:)?([/\\\w\.!-_*'() ]*)$")

    def __init__(self):
        self._submanagers = {}

    def _getManager(self, artefact: typing.Tuple[Artefact, str, None]) -> typing.Tuple['ClassMethodManager', str]:
        """ Fetch the manager and path for the provided artefact """

        if artefact is None:
            return self, self._cwd()

        elif isinstance(artefact, Artefact):
            return artefact.manager, artefact.path

        elif isinstance(artefact, str):
            return self, artefact

        else:
            raise TypeError("Artefact reference must be either `stow.Artefact` or string not type {}".format(type(artefact)))

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

    def touch(self, relpath: str) -> File:
        return self._putBytes(b"", relpath)

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

    def _get_content_type(self, path: str) -> str:
        """ Get the content type for the path given """
        contentType, _ = mimetypes.guess_type(path)
        contentType = (content_type or 'application/octet-stream')
        return contentType

    def _set_content_type(self, path: str, content_type: str) -> str:
        """ Set the content type of the file """
        raise NotImplementedError('Manager does not have an method for changing the content-type for the path given')

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
    def _updateArtefactObjects(self, artefact: Artefact, identity: Artefact = None):
        return self._owner._updateArtefactObjects(artefact._concrete, identity=identity)

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

    def _get(self, source: Artefact, destination: str):
        return self._owner._get(source._concrete, destination)

    def _getBytes(self, source: Artefact) -> bytes:
        return self._owner._getBytes(source._concrete)

    def _put(self, source: str, destination: str):
        self._owner._put(source, self._join(destination))

    def _putBytes(self, fileBytes: bytes, destination: str):
        self._owner._putBytes(fileBytes, self._join(destination))

    def _cp(self, source: Artefact, destination: str):
        return self._owner._cp(source._concrete, self._join(destination))

    def _mv(self, source: Artefact, destination: str):
        return self._owner._mv(source._concrete, self._join(destination))

    def _rm(self, artefact: Artefact):
        self._owner._rm(artefact._concrete)

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