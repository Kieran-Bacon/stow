import os
import sys
import datetime
import shutil
import tempfile
import contextlib

from .. import SEP
from ..artefacts import Artefact, File, Directory
from ..manager import LocalManager
from .. import exceptions

class FS(LocalManager):
    """ Wrap a local filesystem (a networked drive or local directory)

    Params:
        path (str): The local relative path to where the manager is to be initialised
    """

    def __init__(self, path: str):
        # Record the local path to the original directory
        self._path = os.path.abspath(path)
        super().__init__()

    def __repr__(self): return '<Manager(FS): {}>'.format(self._path)

    def _abspath(self, artefact):
        _, path = self._artefactFormStandardise(artefact)
        return os.path.abspath(os.path.join(self._path, path[1:]))  # NOTE removing the relative path initial sep

    def _relpath(self, path):
        if self._path == path[:len(self._path)]: path = path[len(self._path):]
        return super()._relpath(path)  # NOTE remove path to root of manager from path before

    def _basename(self, artefact):
        _, path = self._artefactFormStandardise(artefact)
        return os.path.basename(path)

    def _dirname(self, artefact):
        _, path = self._artefactFormStandardise(artefact)
        return os.path.dirname(path)

    def _isdir(self, relpath: str):

        abspath = self._abspath(relpath)

        if not os.path.exists(abspath):
            raise exceptions.ArtefactNotFound("Could not find an artefact at location: {}".format(relpath))

        else:
            return os.path.isdir(abspath)

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

    def _get(self, src_remote: str, dest_local: str):

        # Get the absolute path to the object
        src_remote = self._abspath(src_remote)

        # Identify download method
        method = shutil.copytree if os.path.isdir(src_remote) else shutil.copy

        # Download
        method(src_remote, dest_local)

    def _put(self, src_local, dest_remote):

        if os.path.isdir(src_local):
            # Copy the directory into place
            #if os.path.exists(dest_remote): shutil.rmtree(dest_remote)
            shutil.copytree(src_local, dest_remote)

        else:
            # Putting a file
            os.makedirs(os.path.dirname(dest_remote), exist_ok=True)
            shutil.copy(src_local, dest_remote)

    def _mv(self, srcObj: Artefact, destPath: str):

        absDestination = self._abspath(destPath)
        os.makedirs(os.path.dirname(absDestination), exist_ok=True)
        os.rename(self._abspath(srcObj.path), absDestination)

    def _listdir(self, relpath: str):

        abspath = self._abspath(relpath)

        dirs, files = set(), set()
        for art in os.listdir(abspath):
            if os.path.isdir(os.path.join(abspath, art)):   dirs.add(self._join(relpath,art))
            else:                                           files.add(self._join(relpath,art))

        return dirs, files

    def _rm(self, artefact: Artefact, path: str):


        abspath = self._abspath(path)
        if not os.path.exists(abspath): return # NOTE the file has already been deleted - copy directory has this affect

        if isinstance(artefact, Directory):
            shutil.rmtree(abspath)
        else:
            os.remove(abspath)

    def toConfig(self):
        return {'manager': 'FS', 'path': self._path}