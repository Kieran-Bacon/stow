# Designing your own manager

If you'd like to entend the package, please feel free to make a pull request the projects github ()

However, for when you'd like to make a private manager for personal or professional reasons. Extending the functionality of the system is rather simple

## Base classes

Managers are implemented as either a `LocalManager` or `RemoteManager`

```python
from stow.manager import LocalManager, RemoteManager
```

The main difference between the managers is that a `RemoteManager` may require that the files/directories be downloaded for some operations (namely localise and open). The `RemoteManager` is responsible for keeping track of these files, and deciding when to communicate changes back to the remote. The `LocalManager` doesn't need to __download__ the files and can access them directly.

As such, for managers that can directly access the files (like on a networked drive), and there isn't a concern about making partial changes, then the `LocalManager` is likely the correct base class

```python

with local.open("file.txt", "w") as handle:
    handle.write("line")  # Written to file
    raise ValueError("Simulated Error")
    handle.write("line")  # Not written to file

with remote.open("file.txt", "w" as handle:
    handle.write("line")  # Not written to file
    raise ValueError("Simulated Error")
    handle.write("line")  # Not written to file

# Errors means that file is not updated on the remote machine

```

### Local Managers

The LocalManager is meant to be the base class for managers that use the native filesystem or a networked storage device that already appears as part of the filesystem. These managers access the files/directories directory and don't have to setup any temporay files or spaces for file manipulation.

Most usecases can be handled by `stow.managers.FS(path="/path/to/directory")`