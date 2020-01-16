import pkg_resources

from .manager import Manager

sep = '/'
MANAGERS = {}

def find(manager) -> Manager:
    # Get the manager class for the manager type given - load the manager type if not already loaded
    if manager in MANAGERS:
        mClass = MANAGERS[manager]

    else:
        for entry_point in pkg_resources.iter_entry_points('storage_managers'):
            if entry_point.name == manager:
                mClass = MANAGERS[manager] = entry_point.load()
                break

        else:
            raise ValueError("Couldn't find a manager called '{}'".format(manager))

    return mClass

def connect(name: str, *, config=None, manager: str = None, **kwargs) -> Manager:

    if config is None and manager is None:
        raise ValueError("Need to specify a config or a manager")

    if config is not None:
        # Load the contents of the config and assign it to a variable
        config.pop('name', None)
        manager = config.pop('manager')
        kwargs = {**config, **kwargs}

    # Find the class for the manager
    mClass = find(manager)

    # Create the Manager - pass all the kwarg arguments
    return mClass(name = name, **kwargs)