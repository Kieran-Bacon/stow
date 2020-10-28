# Designing your own manager

If you'd like to extend the functionality of the package, please feel free to make a pull request on the project's [github](https://github.com/Kieran-Bacon/stow){target=_blank}.

To extend the functionality by supporting another storage medium, you can inherit from the `Manager` abstract base class and implement the abstract methods it declares. You can then incorporate the manager by exposing your new `Manager` via the python entry point system.

!!! Important
    **`stow` uses the entry point _`stow_managers`_ to find managers**

    Add your managers to this entry point to integrate seamlessly with the `stow` stateless interface and connect utilities.


## Base classes

Managers should be implemented as either a `LocalManager` or `RemoteManager`

```python
from stow.manager import LocalManager, RemoteManager
```

The main functions on `Manager` use a method `localise` to get an absolute path to the artefacts they want to interact with. This method is responsible for ensuring the artefacts availability for the other methods and it is the key difference between the `LocalManager` and `RemoteManager`.

**A `LocalManager` can access their artefacts directly and a `RemoteManager` must retrieve their artefacts before they can work with them.**

Each `Manager` implements a localise function for these situations respectively. The `RemoteManager` object's localise function is a lot more involved to avoid pulling and pushing information anymore more than it needs to.

`localise` makes use of your abstract methods defined below to uphold the interface of `Manager` and does not need to be re-implemented.

!!! Note
    You may inherit from the `Manager` base class directly if you wish but you will have to implement the localise method in addition to the other abstract methods. I'd only suggest doing this if you have very special behaviour you want to express.

    If you do find yourself in this situation, please consider adding this special behaviour as it's own abstract base class back to the original project to help others.

## Abstract methods

### ![mkapi](stow.manager.Manager._abspath)
### ![mkapi](stow.manager.Manager._makeFile)
### ![mkapi](stow.manager.Manager._get)
### ![mkapi](stow.manager.Manager._getBytes)
### ![mkapi](stow.manager.Manager._put)
### ![mkapi](stow.manager.Manager._putBytes)
### ![mkapi](stow.manager.Manager._cp)
### ![mkapi](stow.manager.Manager._mv)
### ![mkapi](stow.manager.Manager._rm)
### ![mkapi](stow.manager.Manager._ls)
### ![mkapi](stow.manager.Manager.toConfig)
### ![mkapi](stow.manager.Manager._loadFromProtocol)

## Extending example

```python
import typing

import stow
from stow.manager import RemoteManager

class SSH(stow.manager):

    def __init__(self):
        pass

    def _isdir(self, path: typing.Union[str, stow.Artefact]) -> bool:
        pass





```