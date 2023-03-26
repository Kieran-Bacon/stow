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

The main functions on `Manager` use a method `localise` to get an absolute path to artefacts with which they want to interact. This method is responsible for ensuring the artefacts availability for the other methods and it is the key difference between the `LocalManager` and `RemoteManager`.

**A `LocalManager` can access their artefacts directly and a `RemoteManager` must retrieve their artefacts before they can work with them.**

Each `Manager` implements a localise function for these situations respectively. The `RemoteManager` object's localise function is a lot more involved to avoid pulling and pushing information anymore more than it needs to.

`localise` makes use of your abstract methods defined below to uphold the interface of `Manager` and does not need to be re-implemented.

!!! Note
    You may inherit from the `Manager` base class directly if you wish but you will have to implement the localise method in addition to the other abstract methods. I'd only suggest doing this if you have very special behaviour you want to express.

    If you do find yourself in this situation, please consider adding this special behaviour as it's own abstract base class back to the original project to help others.

## Abstract methods

### ![mkapi](stow.manager.Manager._abspath)

<div style='margin-top: -20px'></div>

```python
def _abspath(self, managerPath: str) -> str:
    path = self.join(self._path, managerPath, joinAbsolutes=True)

    if os.name == 'nt':
        path = path.replace('/', '\\')

    return path
```

### ![mkapi](stow.manager.Manager._identifyPath)

<div style='margin-top: -20px'></div>

```python
def _identifyPath(self, managerPath: str):

    abspath = self._abspath(managerPath)

    if os.path.exists(abspath):

        stats = os.stat(abspath)

        # Created time
        createdTime = datetime.datetime.utcfromtimestamp(stats.st_ctime)
        createdTime = pytz.UTC.localize(createdTime)

        # Modified time
        modifiedTime = datetime.datetime.utcfromtimestamp(stats.st_mtime)
        modifiedTime = pytz.UTC.localize(modifiedTime)

        # Access time
        accessedTime = datetime.datetime.utcfromtimestamp(stats.st_atime)
        accessedTime = pytz.UTC.localize(accessedTime)

        if os.path.isfile(abspath):
            return File(
                self,
                managerPath,
                stats.st_size,
                modifiedTime,
                createdTime,
                accessedTime,
            )

        elif os.path.isdir(abspath):
            return Directory(
                self,
                managerPath,
                createdTime=createdTime,
                modifiedTime=modifiedTime,
                accessedTime=accessedTime,
            )

    return None
```

### ![mkapi](stow.manager.Manager._get)

<div style='margin-top: -20px'></div>

```python
 def _get(self, source: Artefact, destination: str):

    # Convert source path
    sourceAbspath = self._abspath(source.path)

    # Identify download method
    method = shutil.copytree if os.path.isdir(sourceAbspath) else shutil.copy

    # Download
    method(sourceAbspath, destination)
```

### ![mkapi](stow.manager.Manager._getBytes)

<div style='margin-top: -20px'></div>

```python
   def _getBytes(self, source: Artefact) -> bytes:
        with open(self._abspath(source.path), "rb") as handle:
            return handle.read()
```

<!-- ### ![mkapi](stow.manager.Manager._put) -->
### _put TODO fix

<div style='margin-top: -20px'></div>

```python
def _put(self, source: str, destination: str):
    # Convert destination path
    destinationAbspath = self._abspath(destination)

    # Ensure the destination
    os.makedirs(os.path.dirname(destinationAbspath), exist_ok=True)

    # Select the put method
    method = shutil.copytree if os.path.isdir(source) else shutil.copy

    # Perform the putting
    method(source, destinationAbspath)
```

### ![mkapi](stow.manager.Manager._putBytes)

<div style='margin-top: -20px'></div>

```python
def _putBytes(self, fileBytes: bytes, destination: str):

    # Convert destination path
    destinationAbspath = self._abspath(destination)

    # Makesure the destination exists
    os.makedirs(os.path.dirname(destinationAbspath), exist_ok=True)

    # Write the byte file
    with open(destinationAbspath, "wb") as handle:
        handle.write(fileBytes)
```

### ![mkapi](stow.manager.Manager._cp)

<div style='margin-top: -20px'></div>

```python
def _cp(self, source: Artefact, destination: str):
    self._put(self._abspath(source.path), destination)
```
### ![mkapi](stow.manager.Manager._mv)

<div style='margin-top: -20px'></div>

```python
def _mv(self, source: Artefact, destination: str):

    # Convert the source and destination
    source, destination = self._abspath(source.path), self._abspath(destination)

    # Ensure the destination location
    os.makedirs(os.path.dirname(destination), exist_ok=True)

    # Move the source artefact
    os.rename(source, destination)
```

### ![mkapi](stow.manager.Manager._ls)

<div style='margin-top: -20px'></div>

```python
def _ls(self, directory: str):

    # Get a path to the folder
    abspath = self._abspath(directory)

    # Iterate over the folder and identify every object - add the created
    for art in os.listdir(abspath):
        self._addArtefact(
            self._identifyPath(
                self.join(directory, art, separator='/')
            )
        )
```

### ![mkapi](stow.manager.Manager._rm)

<div style='margin-top: -20px'></div>

```python
def _rm(self, artefact: Artefact):

    # Convert the artefact
    artefact = self._abspath(artefact.path)

    # Select method for deleting
    method = shutil.rmtree if os.path.isdir(artefact) else os.remove

    # Remove the artefact
    method(artefact)
```

### ![mkapi](stow.manager.Manager._signatureFromURL)

<div style='margin-top: -20px'></div>

```python
def _signatureFromURL(cls, url: urllib.parse.ParseResult):
    return {"path": "/"}, os.path.abspath(os.path.expanduser(url.path))
```

### ![mkapi](stow.manager.Manager.toConfig)

<div style='margin-top: -20px'></div>

```python
def toConfig(self):
    return {'manager': 'FS', 'path': self._path}
```

## Special cases

Depending on the storage medium, it may be more efficient to load (read the metadata of) multiple artefacts simultaneously. `s3` for example, returns the metadata for all files at a level when asked. It would be more efficient to instantiate all of these objects at this point rather than singling out any single object.

This can be achieved by overloading the `_loadArtefact` method on the `Manager`, which is the method used internally to create/ensure an artefact object.

```python
def _loadArtefact(self, managerPath: str) -> Artefact:

    if managerPath in self._paths:
        # Artefact was previously loaded and can be returned normally
        return super()._loadArtefact(managerPath)

    try:
        # Ensure the owning directory and fetch the directory object
        directory = self._ensureDirectory(self.dirname(managerPath))

    except (exceptions.ArtefactNotFound, exceptions.ArtefactTypeError) as e:
        raise exceptions.ArtefactNotFound("Cannot locate artefact {}".format(managerPath)) from e

    # Add all artefacts of the directory into the manager at this level
    self._ls(directory.path)
    directory._collected = True

    # Return the now instantiated artefact
    if managerPath in self._paths:
        return self._paths[managerPath]

    else:
        raise exceptions.ArtefactNotFound("Cannot locate artefact {}".format(managerPath))
```