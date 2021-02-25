import os
import tempfile
import re
import urllib.parse
import typing
import io

from ..artefacts import Artefact, File, Directory
from ..manager import Manager, RemoteManager
from .. import exceptions

class SSH(RemoteManager):

    def __init__(self):
        pass

    def __repr__(self):
        pass

    def _identifyPath(self, relpath: str) -> typing.Union[str, Artefact]:
        """ Given a manager relative path, identify what object (if any) is at that location and store its information
        as a result. Should call _addArtefact in the call stack for the returned item to ensure that things are updated

        Args:
            relpath: Manager relative path

        Returns:
            typing.Union[str, Artefact]: return None, "file", "directory" for nothing exists, file exists, directory
                respectively
        """

        # identify the artefact and then
        self._makeFile()
        # on the path - return this and do not add it
        # as it will be used in the _ls added there -
        # this would work for cases where there isn't a speed up for ls functions

        # Alternatively the identify could use the _ls and then return the artefact by this name
        base = self._basename(relpath)
        self._ls(base)
        return self._paths[relpath] if relpath in self._paths else None

        # TODO
        # Write a documentation section about the interplay between these two methods and how that can improve performance


        pass

    def _abspath(self, relpath: str):
        pass

    def _makeFile(self, relpath: str) -> Artefact:
        pass

    def _makeDirectory(self, relpath: str) -> Artefact:
        pass

    def _get(self, source: Artefact, destination: str):
        pass

    def _getBytes(self, source: Artefact) -> bytes:
        pass

    def _put(self, source: str, destination: str, merge: bool = False):
        pass

    def _putBytes(self, fileBytes: bytes, destinationAbsPath: str):
        pass

    def _cp(self, source: Artefact, destination: str):
        pass

    def _mv(self, source: Artefact, destination: str):
        pass

    def _rm(self, artefact: Artefact):
        pass

    def _ls(self, relpath: str) -> Directory:

        for obj in os.listdir(relpath):

            if obj is File:
                self._addArtefact(self._makeFile())

            else:
                self._addArtefact(self._makeDirectory())

    def toConfig(self):
        pass

    @classmethod
    def _loadFromProtocol(cls, url: str) -> Manager:
        return cls(
            a,
            b,
            c,
        )