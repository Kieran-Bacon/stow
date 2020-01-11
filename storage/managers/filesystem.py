import os
import sys
import datetime
import shutil
import tempfile

from .. import SEP
from ..interfaces import Artefact, Exceptions
from ..artefacts import File, Directory
from ..manager import Manager
from ..utils import connect

WIN32 = 'win32'

class FS(Manager):
    """ Wrap a local filesystem (a networked drive or local directory)

    Params:
        name (str): A human readable name for logging purposes
        path (str): The local relative path to where the manager is to be initialised
    """

    def __init__(self, name: str, path: str):
        super().__init__(name)

        # Set the top level of the manager - trigger a re-assessment of the state of the manager (initialise it)
        self._path = os.path.abspath(path)
        self.refresh()

    def _relpath(self, path: str):
        """ Convert the path given into a relative path for the manager """
        path = path[len(self._path):]
        if sys.platform == WIN32:
            return path.replace(os.path.sep, SEP)
        return path

    def _walk(self, directory: str) -> Directory:
        """ Walk through a directory recursively and build Artefact objects to represent its structure. Capturing the
        files metadata and paths

        Params:
            directory (str): Filepath of the os pointing to a directory to walk

        Returns:
            Directory: Returns a representation of the given directory with all its child elements nested within it
        """

        # Hold the building contents of the directory
        contents = set()

        # Iterate through the contents of the directory
        for obj in os.listdir(directory):

            # Create the absolute path for the obj
            objPath = os.path.join(directory, obj)
            relativeObjPath = self._relpath(objPath)

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
        d = Directory(self, self._relpath(directory) or SEP, contents)
        return d

    def _getHierarchy(self, path) -> Directory:
        """ Fetch the owning `container` for the manager relative path given. In the event that no `container` object
        exists for the path, create one and recursively find its owning `container` to add it to. The goal of this
        function is to traverse up the hierarchy and ensure all the directory objects exist, and when they do quickly
        return the container they are in

        Params:
            path (str): The manager relative path for an `Artefact`

        Returns:
            Directory: The owning directory container, which may have just been created
        """

        if path in self._paths:
            # The path points to an already established directory
            directory = self._paths[path]
            if isinstance(directory, File): raise ValueError("Invalid path given. Path points to a file.")
            return directory

        # Create the directory at this location
        art = Directory(self, path)

        # Fetch the owning directory and add this diretory into it
        # NOTE os.path.dirname is os agnostic
        self._getHierarchy(os.path.dirname(path)).add(art)

        self._paths[path] = art
        return art

    def refresh(self):
        #TODO set previous artefacts to not exist as the new ones have been created
        self._paths = {}
        self._paths[SEP] = self._walk(self._path)

    def __repr__(self):
        return '<Manager(FS): {} - {}>'.format(self.name, self._path)

    def get(self, src_remote, dest_local):

        # Create the relative path to the source file
        path = os.path.abspath(
            os.path.join(
                self._path,
                super().get(src_remote, dest_local).strip(SEP)
            )
        )

        # Identify download method
        method = shutil.copytree if os.path.isdir(path) else shutil.copy

        # Download
        method(path, dest_local)

    def put(self, src_local, dest_remote) -> Artefact:

        with super().put(src_local, dest_remote) as (source_filepath, destination_path):

            # Convert the relative destination path to an absolute path
            abspath = os.path.abspath(os.path.join(self._path, destination_path.strip(SEP)))

            # Get the owning directory of the item - Ensure that the directories exist for the incoming files
            os.makedirs(os.path.dirname(abspath), exist_ok=True)
            owning_directory = self._getHierarchy(os.path.dirname(destination_path))

            # Process the uploading item
            if os.path.isdir(source_filepath):
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

        path = os.path.abspath(os.path.join(self._path, relativeObjPath.strip(SEP)))
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

    def __init__(self, name, directories):
        super().__init__(name)

        # Unpack all the directories and keep references to the original managers
        directories = [os.path.expanduser(d) for d in directories]
        self._default = directories[0].split(os.path.sep)[-1]
        self._namesToPaths = {d.split(os.path.sep)[-1]: os.path.abspath(d) for d in directories}
        self._managers = {name: connect(name, manager='FS', path=path) for name, path in self._namesToPaths.items()}

    def refresh(self):
        for manager in self._managers.values():
            manager.refresh()

    def paths(self, artefactType = None):
        # Set up the paths for the manager
        return {
            "{sep}{}{sep}{}".format(name, path.strip(SEP), sep=SEP): art
            for name, manager in self._managers.items()
            for path, art in manager.paths().items()
            if artefactType is None or isinstance(art, artefactType)
        }

    @ staticmethod
    def _splitFilepath(filepath: str) -> (str, str):
        nodes = filepath.strip(SEP).split(SEP)
        return nodes[0], SEP + SEP.join(nodes[1:])

    def __getitem__(self, filepath: str):
        d, path = self._splitFilepath(filepath)
        if d not in self._managers:
            return self._managers[self._default][filepath]
        return self._managers[d][path]

    def __contains__(self, filepath: str):
        if isinstance(filepath, Artefact): return super().__contains__(filepath)
        d, path = self._splitFilepath(filepath)
        if d not in self._managers:
            return filepath in self._managers[self._default]
        return path in self._managers[d]


    def get(self, src_remote: str, dest_local):
        source_path = super().get(src_remote, dest_local)
        d, path = self._splitFilepath(source_path)
        if d not in self._managers:
            return self._managers[self._default].get(source_path, dest_local)
        return self._managers[d].get(path, dest_local)

    def put(self, src_local: str, dest_remote):
        with super().put(src_local, dest_remote) as (source_path, destination_path):
            d, path = self._splitFilepath(destination_path)

            if d not in self._managers:
                return self._managers[self._default].put(source_path, destination_path)
            return self._managers[d].put(source_path, path)

    def rm(self, filename, recursive: bool = False):
        path = super().rm(filename, recursive)
        d, path = self._splitFilepath(path)
        return self._managers[d].rm(path, recursive)