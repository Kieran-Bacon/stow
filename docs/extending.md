# Designing your own manager

If you'd like to extend the functionality of the package, please feel free to make a pull request on the project's [github](https://github.com/Kieran-Bacon/stow){target=_blank}.

To extend the functionality by supporting another storage medium, you can inherit from the `Manager` abstract base class and implement the abstract methods it declares. You can then incorporate the manager by exposing your new `Manager` via the python entry point system.

!!! Important
    **`stow` uses the entry point _`stow_managers`_ to find managers**

    Add your managers to this entry point to integrate seamlessly with the `stow` stateless interface and connect utilities.


## Threading considerations

Inside stow is a system to share and use a thread pool executor for the parallel running of operations. This provides some managers a nice performance boost, but, it complicates subsequent actions that need to take place. The most prevelant example of this is when a file is being moved: A artefact is put/copied into place, and then the source is deleted.

This is difficult to do in a threading environment because these steps must happen in this order. The put must complete beforehand and complete successfully. We are unable to create the delete operation as a parallel task as it cannot depend on the put task future or the effect of that task since this can lead to a deadlock, and we cannot allow the possibility of deleting the source before the put task is started/completed.

Therefore, it follows that any implemention should combine these operations into a single task. The implementation of put should take a delete option and handle the delete itself. The next consideration is parent directories that should be deleted.

When moving a directory with artefacts inside, the original directory has been deleted once the artefacts have been moved. For the same reasons that you cannot have the delete operation dependant on the put task, you cannot have the put directory task depending on the tasks that put it's children. Regardless of how you are defining tasks, this operations must be combined in the same task that puts a child artefact. *How you might be performing threading more generally is discussed below.*

The file upload implementation likely is unaware of the context it is being invoked in, and it is not a given that it can tell whether its parent directory should be deleted, or even if it is even ready to be deleted. I suggest that a shared data object is given to the child tasks, so that they can communicate with each other as to when they are finished. The final task to complete can then delete the parent directory (the implication of the existance of this object in the first place) knowning that it and others have completed. This relies on the GIL and the atomic nature of some types.

```python
def _put_file_task(self, source, desintation, delete: bool = False, shared_data: Optional[SharedObject] = None):

    client.upload(source, destination)

    if delete:
        source.delete()

    if shared_object:
        # We need to delete parent directory

        shared_object.incr()
        shared_object.is_complete()

        try:
            source.parent.delete()
        except:
            # Already deleted
            pass
```


The implementation of the threading is down to the manager but I believe that there are two designs for threading that have a large implementation knock on effect.

1. You thread leafs - One creates a task for every artefact to be put into the target. The main thread will iterate through the hierarchy and enqueue each artefact to be processed.

2. You thread branches - You serially iterate through a hierarchy and put its contents as you go. At some level of the hiearchy (probably the top).

### Threading the leafs - issues

- Handling of directories - If you need the directory to exist before writing to it, you can either: have the main thread, which is performing the iteration, create those directories as it goes before enqueuing the file upload tasks; or have the task for creating a directory, recursively enqueue as task to upload the contents when the directory is created. Since creating of directories is usually very quick, the inefficieny of doing building the hierachy serially is likely to be preferred to the complexity of having a recursive task upload implementation.

to have to have a task to create the directory and the subsequently enqueue it's contents to be uploaded. cannot enqueue it as a task and

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