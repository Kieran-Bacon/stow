import os
import sys
import datetime
import shutil
import tempfile
import contextlib

from .. import SEP
from ..artefacts import Artefact, File, Directory
from ..manager import LocalManager
from ..utils import connect

WIN32 = 'win32'

class FS(LocalManager):
    """ Wrap a local filesystem (a networked drive or local directory)

    Params:
        path (str): The local relative path to where the manager is to be initialised
    """

    def __init__(self, path: str):
        # Record the local path to the original directory
        self._path = os.path.abspath(path)
        super().__init__()

    def _abspath(self, artefact):
        _, path = self._artefactFormStandardise(artefact)
        path = path[1:]  # NOTE removing the relative path initial sep
        return os.path.abspath(os.path.join(self._path, path))

    def _relpath(self, path):
        # TODO this just doesn't work...

        path = path[len(self._path):]
        if sys.platform == WIN32:
            return path.replace(os.path.sep, SEP)
        return path

    def _basename(self, artefact):
        _, path = self._artefactFormStandardise(artefact)
        return os.path.basename(path)

    def _dirname(self, artefact):
        _, path = self._artefactFormStandardise(artefact)
        return os.path.dirname(path)

    def _makefile(self, path) -> File:
        abspath = self._abspath(path)

        if not os.path.exists(abspath):
            with open(abspath, "w"):
                pass

        stats = os.stat(abspath)
        return File(
            self,
            path,
            datetime.datetime.fromtimestamp(stats.st_mtime),
            stats.st_size
        )

    def _walkOrigin(self, prefix=None):

        path = self._path if prefix is None else self._abspath(prefix)
        files = set()

        for dp, dn, fn in os.walk(path):
            files.add(self._relpath(os.path.join(dp, self._PLACEHOLDER)))

            for f in fn:
                files.add(self._relpath(os.path.join(dp, f)))

        return files

    def __repr__(self): return '<Manager(FS): {} - {}>'.format(self.name, self._path)

    def _get(self, src_remote: str, dest_local: str):

        # Get the absolute path to the object
        src_remote = self._abspath(src_remote)

        # Identify download method
        method = shutil.copytree if os.path.isdir(src_remote) else shutil.copy

        # Download
        method(src_remote, dest_local)

    def _put(self, src_remote: str, dest_local: str):

        # Convert the relative destination path to an absolute path
        abspath = self._abspath(dest_local)

        # Get the owning directory of the item - Ensure that the directories exist for the incoming files
        os.makedirs(os.path.dirname(abspath), exist_ok=True)
        owning_directory = self._backfillHierarchy(self._dirname(dest_local))

        # Process the uploading item
        if os.path.isdir(src_remote):
            # Check that the directory doesn't already exist
            if dest_local in self._paths:
                # It exists so remove it and all its children
                self.rm(dest_local, recursive=True)

            # Copy the directory into place
            shutil.copytree(src_remote, abspath)

            # Walk the directory
            art = self.refresh(dest_local)

        else:
            # Putting a file
            shutil.copy(src_remote, abspath)

            art = self._makefile(dest_local)

            if dest_local in self._paths:

                original = self._paths[dest_local]
                original._update(art)
                return original

        # Save the new artefact
        owning_directory._add(art)
        self._paths[dest_local] = art
        return art

    def _mv(self, srcObj: Artefact, destPath: str):

        absDestination = self._abspath(destPath)
        os.makedirs(os.path.dirname(absDestination), exist_ok=True)
        os.rename(self._abspath(srcObj.path), absDestination)

    def _rm(self, path: str):

        abspath = self._abspath(path)
        if os.path.isdir(abspath):
            shutil.rmtree(abspath)
        else:
            os.remove(abspath)

    def toConfig(self):
        return {'manager': 'FS', 'path': self._path}


class Locals(LocalManager):

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