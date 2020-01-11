import typing
import abc
import os
import tempfile

class Exceptions:

    class ArtefactNotFound(Exception):
        pass

    class ArtefactNotMember(Exception):
        pass

    class ArtefactTypeError(TypeError):
        pass

    class OperationNotPermitted(Exception):
        pass

class Container:

    @abc.abstractmethod
    def mkdir(self, path: str) -> object:
        """ Create a directory inside the container to be populated. Recursively create directories to the destination
        directory

        Params:
            path (str): The relative path to create the directory in

        Returns:
            object: The Directory artefact associated with the directory created at the path location. Other directories
                created along the way are not returned. They are accessible through the container
        """
        pass

    @abc.abstractmethod
    def touch(self, path: str) -> object:
        """ Create an empty file at the path provided, returning the file object to allow for access at that point.
        Along the path, directories that aren't yet created shall be created.

        Params:
            path (str): The relative path to the file

        Returns:
            object (File): A file artefact of the newly created file in the container.
        """
        pass

    @abc.abstractmethod
    def ls(self, path: str = None, recursive: bool = False):
        """ List the objects that can be found at the path location - if no path is given list the top level directory
        of the storage option. If recursive is true, loop through all sub-directories and include those items in the
        list

        Params:
            path (str) = None: A relative manager path
            recursive (bool) = False: Toggle recursive switch
        """
        pass

    @abc.abstractmethod
    def rm(self, path, recursive = False) -> None:
        pass

    @abc.abstractmethod
    def mv(self, artefact, destination):
        pass

class Artefact:
    """ Aretefacts are the items that are being stored - it is possible that through another mechanism that these items
    are deleted and they are no longer able to work
    """

    def __init__(self, container: Container, path: str):
        self._container = container
        self._path = path
        self._exists = True # As you are created you are assumed to exist

    def __getattr__(self, attr):
        if self.__getattribute__('_exists'):
            return self.__getattribute__(attr)
        else:
            raise FileNotFoundError(f"{self} no longer exists")

    def __hash__(self): return hash(id(self))
    def __eq__(self, other): hash(self) == hash(other)

    @property
    def manager(self): return self._container
    @property
    def path(self): return self._path
    @path.setter
    def path(self, path: str):
        """ Move the file on the target (perform the rename) - if it fails do not change the local file name """
        self._container.mv(self, path)
        self._path = path

    def save(self, path: str):
        self._container.get(self, path)
