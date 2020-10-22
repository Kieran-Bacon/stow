import functools
import pkg_resources

from .manager import Manager

MANAGERS = {}

def find(manager: str) -> Manager:
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

    if manager in MANAGERS:
        mClass = MANAGERS[lmanager]

    else:
        for entry_point in pkg_resources.iter_entry_points('stow_managers'):
            if entry_point.name == lmanager:
                mClass = MANAGERS[lmanager] = entry_point.load()
                break

        else:
            raise ValueError("Couldn't find a manager called '{}'".format(manager))

    return mClass

@functools.lru_cache(maxsize=None)
def connect(manager: str, *, submanager: str = None, **kwargs) -> Manager:
    """ Find and connect to a `Manager` using its name (entrypoint name) and return an instance of that `Manager`
    initalised with the kwargs provided. A path can be provided as the location on the manager for a sub manager to be
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