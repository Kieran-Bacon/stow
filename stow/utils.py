""" House utilities for the finding and creation of Managers """

import typing
import functools
import dataclasses

from . import _utils
from .manager import Manager

def find(manager: str) -> typing.Type[Manager]:
    """ Fetch the `Manager` class hosted on the 'stow_managers' entrypoint with
    the given name `manager` entry name.

    Args:
        manager: The name of the `Manager` class to be returned

    Returns:
        Manager: The `Manager` class for the manager name provided

    Raises:
        ValueError: In the event that a manager with the provided name couldn't be found
    """
    return _utils.find(manager)

def connect(manager: str, **kwargs) -> typing.Type[Manager]:
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
    return _utils.connect(manager, **kwargs)

@dataclasses.dataclass
class ParsedURL:
    """ House pointers to manager """
    manager: Manager
    relpath: str

@functools.lru_cache
def parseURL(stowURL: str, default_manager = None) -> ParsedURL:
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
    return ParsedURL(*_utils.parseURL(stowURL))
