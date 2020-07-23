import functools
import pkg_resources

from .manager import Manager

MANAGERS = {}

def find(manager: str) -> Manager:
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

    if submanager is not None:
        # Create the initial manager and return a sub-manager
        return connect(manager, **kwargs).submanager(submanager)

    # Find the class for the manager
    mClass = find(manager)

    # Create the Manager - pass all the kwarg arguments
    return mClass(**kwargs)