import os
import datetime
import shutil
import tempfile

from .. import sep
from ..interfaces import Artefact, Exceptions
from ..artefacts import File, Directory
from ..manager import Manager

class FS(Manager):
    """ Wrap a local filesystem (a networked drive or local directory)

    Params:
        name (str):
        path (str): The local relative path to where the manager is to be initialised
    """

    def _walk(self, directory: str) -> Directory:

        # Hold the building contents of the directory
        contents = set()

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
            contents.add(art)

        # Create a directory object for this directory that has been navigated
        d = Directory(self, directory[len(self._path):] or '/', contents)
        return d

    def __init__(self, name: str, path: str):
        super().__init__(name)

        # Set the top level of the manager - trigger a re-assessment of the state of the manager (initialise it)
        self._path = os.path.abspath(path)
        self.refresh()

    def refresh(self):
        #TODO set previous artefacts to not exist as the new ones have been created
        self._paths = {}
        self._root = self._walk(self._path)
        self._paths[sep] = self._root

    def __repr__(self):
        return '<Manager(FS): {} - {}>'.format(self.name, self._path)

    def get(self, src_remote, dest_local):

        # Call the generic get function which ensures the src_remote path
        path = os.path.join(self._path, super().get(src_remote, dest_local).strip(os.path.sep))

        # Identify download method
        method = shutil.copytree if os.path.isdir(path) else shutil.copy

        # Download
        method(path, dest_local)

    def _getHierarchy(self, path) -> Directory:

        if path in self._paths:
            # The path points to an already established directory

            directory = self._paths[path]

            if isinstance(directory, File):
                raise ValueError("Invalid path given. Path points to a file.")

            return directory

        # Create the directory at this location
        art = Directory(self, path)

        # Fetch the owning directory and add this diretory into it
        # NOTE os.path.dirname is os agnostic
        self._getHierarchy(os.path.dirname(path)).add(art)

        self._paths[path] = art
        return art

    def put(self, src_local, dest_remote) -> Artefact:

        with super().put(src_local, dest_remote) as (source_filepath, destination_path):

            # Convert the relative destination path to an absolute path
            abspath = os.path.abspath(os.path.join(self._path, destination_path.strip(os.path.sep)))

            # Get the owning directory of the item - Ensure that the directories exist for the incoming files
            os.makedirs(os.path.dirname(abspath), exist_ok=True)
            owning_directory = self._getHierarchy(os.path.dirname(destination_path))

            # Process the uploading item
            if os.path.isdir(source_filepath):
                # Putting a directory

                # Check that the directory doesn't already exist
                if destination_path in self._paths:
                    # It exists so remove it and all its children
                    self.rm(destination_path, recursive=True)

                # Copy the directory into place
                shutil.copytree(source_filepath, abspath)

                # Walk the directory
                art = self._walk(abspath)

            else:
                # Putting a file
                shutil.copy(source_filepath, abspath)

                stats = os.stat(abspath)  # Get the file information

                if destination_path in self._paths:
                    # A file object already existed for this file - update its values
                    self._paths[destination_path]._update(
                        datetime.datetime.fromtimestamp(stats.st_mtime),
                        stats.st_size
                    )
                    return self._paths[destination_path]

                # Create a new file object at the location
                art = File(
                    self,
                    destination_path,
                    datetime.datetime.fromtimestamp(stats.st_mtime),
                    stats.st_size
                )

            # Save the new artefact
            owning_directory.add(art)
            self._paths[destination_path] = art
            return art

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