import collections
import typing
import urllib
import functools
import pkg_resources

MANAGERS = {}
INITALISED_MANAGERS = {}

def initCache(function):
    """ Cache results and return previously created manager objects
    """

    functools.wraps(function)
    def wrapper(manager, **kwargs):

        identifier = hash((manager, "-".join(["{}-{}".format(k,v) for k,v in sorted(kwargs.items(), key=lambda x: x[0])])))

        if identifier in INITALISED_MANAGERS:
            return INITALISED_MANAGERS[identifier]

        manager = function(manager, **kwargs)

        INITALISED_MANAGERS[identifier] = manager

        return manager

    return wrapper


def find(manager: str):
    """ Fetch the `Manager` class hosted on the 'stow_managers' entrypoint with the given name `manager` entry name.

    Args:
        manager: The name of the `Manager` class to be returned

    Returns:
        Manager: The `Manager` class for the manager name provided

    Raises:
        ValueError: In the event that a manager with the provided name couldn't be found
    """

    # Get the manager class for the manager type given - load the manager type if not already loaded
    lmanager = manager.lower()

    if lmanager in MANAGERS:
        mClass = MANAGERS[lmanager]

    else:
        foundManagerNames = []

        for entry_point in pkg_resources.iter_entry_points('stow_managers'):

            foundManagerNames.append(entry_point.name)

            if entry_point.name == lmanager:
                mClass = MANAGERS[lmanager] = entry_point.load()
                break

        else:
            raise ValueError("Couldn't find a manager called '{}' - found {} managers: {}".format(
                    manager,
                    len(foundManagerNames),
                    foundManagerNames
                )
            )

    return mClass

@initCache
def connect(manager: str, *, submanager: str = None, **kwargs):
    """ Find and connect to a `Manager` using its name (entrypoint name) and return an instance of that `Manager`
    initialised with the kwargs provided. A path can be provided as the location on the manager for a sub manager to be
    created which will be returned instead.

    Args:
        manager: The name of the manager class
        submanager: A path on the manager where a submanager is to be created
        kwargs: Keyworded arguments to be passed to the Manager init

    Returns:
        Manager: A storage manager or sub manager which can be used to put and get artefacts

    Note:
        References to `Manager` created by this method are stored to avoid multiple definitions of managers on similar
        locations.

        The stateless interface uses this method as the backend for its functions and as such you can fetch any active
        session by using this function rather than initalising a `Manager` directly
    """

    if submanager is not None:
        # Create the initial manager and return a sub-manager
        return connect(manager, **kwargs).submanager(submanager)

    # Find the class for the manager
    mClass = find(manager)

    # Create the Manager - pass all the kwarg arguments
    return mClass(**kwargs)

# Parsed URL tuple definition
ParsedURL = collections.namedtuple("ParsedURL", ["manager", "relpath"])

def parseURL(stowURL: str) -> ParsedURL:
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
    if parsedURL.scheme and parsedURL.netloc:
        manager = find(parsedURL.scheme)
        scheme = parsedURL.scheme

    else:
        manager = find("FS")
        scheme = "FS"

    # Get the signature for the manager from the url
    signature, relpath = manager._signatureFromURL(parsedURL)

    # Has to use connect otherwise it will just create lots and lots of new managers
    return ParsedURL(connect(scheme, **signature), relpath)