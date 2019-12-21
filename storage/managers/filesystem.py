import os
import datetime
import shutil
import tempfile

from .. import sep
from ..interfaces import Manager, Artefact, Exceptions
from ..artefacts import File, Directory

class FS(Manager):
    """ Wrap a local filesystem (a networked drive or local directory)

    Params:
        name (str):
        path (str): The local relative path to where the manager is to be initialised
    """

    def _walk(self, directory: str) -> Directory:

        # Hold the building contents of the directory
        contents = []

        # Iterate through the contents of the directory
        for obj in os.listdir(directory):

            # Create the absolute path for the obj
            objPath = os.path.join(directory, obj)
            relativeObjPath = objPath[len(self._path):]

            # Resolve whether the obj is a directory or a file
            if os.path.isdir(objPath):
                # Recursively create the subdirectory object and add it to the contents of this directory
                art = self._walk(objPath)

            else:
                # Create the file representation
                stats = os.stat(objPath)
                art = File(
                    self,
                    relativeObjPath,
                    datetime.datetime.fromtimestamp(stats.st_mtime),
                    stats.st_size
                )

            # Record the path to the file object and add it to the collection of items found within the directory
            self._paths[relativeObjPath] = art
            contents.append(art)

        # Create a directory object for this directory that has been navigated
        d = Directory(self, directory[len(self._path):], contents)
        return d

    def __init__(self, name: str, path: str):
        super().__init__(name)

        # Set the top level of the manager - trigger a re-assessment of the state of the manager (initialise it)
        self._path = os.path.abspath(path)
        self.refresh()

    def refresh(self):
        self._paths = {}
        self._root = self._walk(self._path)
        self._paths[sep] = self._root

    def get(self, src_remote, dest_local):

        # Call the generic get function which ensures the src_remote path
        path = os.path.join(self._path, super().get(src_remote, dest_local).strip(os.path.sep))

        # Identify download method
        method = shutil.copytree if os.path.isdir(path) else shutil.copy

        # Download
        method(path, dest_local)


    def put(self, src_local, dest_remote) -> File:

        # Resolve the destination location - get relative path for the manager and the absolute path to the files
        relativePath = super().put(src_local, dest_remote)
        abspath = os.path.join(self._path, relativePath.strip(os.path.sep))

        # Remove anything that exists at the location
        #if relativePath in self._paths: self.rm(relativePath)

        # Ensure that the directories exist for the incoming files
        os.makedirs(os.path.dirname(abspath), exist_ok=True)


        # Write the files to the manager location
        if isinstance(src_local, Artefact):
            # Save the file/directory into the location
            src_local.save(abspath)

        else:
            # src_local is a local path - Identify upload method and upload the files
            method = shutil.copytree if os.path.isdir(src_local) else shutil.copy

            # Upload
            method(src_local, abspath)

        # Create objects to represent the new objects found at the location
        if os.path.isdir(abspath):
            # TODO HANDLE WHEN THE DIRECTORY ALREADY EXISTS - PROBABLY IN WALK
            # Walk the new tree and create directories/files for all the items
            art = self._walk(abspath)

        else:
            # A file was put into position
            stats = os.stat(abspath)  # Get the file information

            if relativePath in self._paths:
                # A file object already existed for this file - update its values
                self._paths[relativePath]._update(
                    datetime.datetime.fromtimestamp(stats.st_mtime),
                    stats.st_size
                )
                return self._paths[relativePath]

            # Create a new file object at the location
            art = File(
                self,
                relativePath,
                datetime.datetime.fromtimestamp(stats.st_mtime),
                stats.st_size
            )

        # Get the relative parent for the newly added items
        self._paths[relativePath] = art

        # Follow the path for the artefact back up to an established point - adding in missing directories as we go
        focus = art
        while True:
            parent = os.path.dirname(relativePath)
            if parent in self._paths:
                self._paths[parent].add(focus)
                break

            else:
                focus = Directory(self, parent, [art])
                self._paths[parent] = focus
                relativePath = parent

        return art

    def ls(self, path: str = None, recursive: bool = False):

        if path is None: return self._root.ls(recursive)

        # Get from the manager store the object for this path - If failed to collect raise membership error
        art = self._paths.get(path)
        if art is None: raise Exceptions.ArtefactNotFound("No directory found at location: {}".format(path))

        # Return the contents of the artefact - if not a container artefact raise error
        if isinstance(art, Directory): return art.ls(recursive)
        raise TypeError("None directory artefact found at location")

    def rm(self, path: str = None, recursive: bool = False):

        relativeObjPath = super().rm(path, recursive)
        obj = self._paths[relativeObjPath]

        if isinstance(obj, Directory):
            contents = obj.ls(recursive=True)
            for subObj in contents:
                del self._paths[subObj.path]
                subObj._exists = False

        del self._paths[obj.path]
        obj._exists = False

        path = os.path.join(self._path, relativeObjPath.strip(sep))
        method = shutil.rmtree if os.path.isdir(path) else os.remove
        method(path)

    @classmethod
    def CLI(cls):
        
        print('Initialising a File system manager.')

        name = input('Name of the filesystem(reference only): ')

        while True:
            path = input('Directory path: ')

            try:
                if not os.path.exists(path):
                    print("The path given doesn't exist. Please try again.\n")

                break
            except:
                continue

        return cls(name, path)

    def toConfig(self):
        return {'name': self.name, 'manager': 'FS', 'path': self._path}


class Locals(Manager):

    def __init__(self, directories):
        super().__init__('local')

        # Unpack all the directories and keep references to the original managers
        directories = [os.path.expanduser(d) for d in directories]
        self._namesToPaths = {d.split(os.path.sep)[-1]: os.path.abspath(d) for d in directories}
        self._managers = {name: connect(name, manager='FS', path=path) for name, path in self._namesToPaths.items()}

        # Set up the paths for the manager
        for name, manager in self._managers.items():
            for path, art in manager.paths().items():
                self._paths["{sep}{}{sep}{}".format(name, path.strip(sep), sep=sep)] = art

    @ staticmethod
    def _splitFilepath(filepath: str) -> (str, str):
        nodes = filepath.strip(sep).split(sep)
        return nodes[0], sep + sep.join(nodes[1:])

    def __getitem__(self, filepath: str):
        d, path = self._splitFilepath(filepath)
        return self._managers[d][path]

    def get(self, src_remote: str, dest_local):
        d, path = self._splitFilepath(src_remote)
        return self._managers[d].get(path, dest_local)

    def put(self, src_local: str, dest_remote):
        d, path = self._splitFilepath(dest_remote)
        return self._managers[d].put(src_local, path)

    def rm(self, filename):
        d, path = self._splitFilepath(filename)
        return self._managers[d].rm(path)