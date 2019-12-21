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

class Manager(Container):
    """ Manager Abstract base class - expressed the interface of a Manager which governs a storage option and allows
    extraction and placement of files in that storage container

    Params:
        name (str): A human readable name for the storage option
    """

    def __init__(self, name: str):
        self.name = name
        self._paths = {}

    def __getitem__(self, item): return self._paths[item]
    def __contains__(self, item):
        if isinstance(item, Artefact): return item.manager is self
        return item in self._paths

    def paths(self, classtype = None):
        if classtype is None: return self._paths.copy()
        else: return {path: artefact for path, artefact in self._paths.items() if isinstance(artefact, classtype)}

    @abc.abstractmethod
    def get(self, src_remote: typing.Union[Artefact, str], dest_local: str) -> Artefact:
        """ Get a remote artefact from the storage option and write it to the destination path given.

        Params:
            src_remote (Artefact/str): The remote's file object or its path
            dest_local (str): The local path for the artefact to be written to
        """

        # Identify the path to be loaded
        if isinstance(src_remote, Artefact):
            if src_remote.manager is not self:
                raise Exceptions.ArtefactNotMember("Provided artefact is not a member of the manager")

            return src_remote.path

        else:
            if src_remote not in self._paths:
                Exceptions.ArtefactNotFound("There is no item at the location given: {}".format(src_remote))

            return src_remote


    @abc.abstractmethod
    def put(self, src_local: str, dest_remote: typing.Union[Artefact, str]) -> None:
        """ Put a local artefact onto the remote at the location given.

        Params:
            src_local (str): The path to the local artefact that is to be put on the remote
            dest_remote (Artefact/str): A file object to overwrite or the relative path to a destination on the
                remote
        """
        # Identify the path to be loaded
        if isinstance(dest_remote, Artefact):
            if dest_remote.manager is not self:
                raise Exceptions.ArtefactNotMember("Provided artefact is not a member of the manager")

            return dest_remote.path

        else:
            if dest_remote not in self._paths:
                Exceptions.ArtefactNotFound("There is no item at the location given: {}".format(src_local))

            return dest_remote

    @abc.abstractmethod
    def rm(self, obj: typing.Union[Artefact, str], recursive: bool = True) -> None:
        # Identify the path to be loaded
        if isinstance(obj, Artefact):
            if obj.manager is not self:
                raise Exceptions.ArtefactNotMember("Provided artefact is not a member of the manager")

        else:
            if obj not in self._paths:
                raise Exceptions.ArtefactNotFound("There is no item at the location given: {}".format(obj))

            obj = self._paths[obj]

        if isinstance(obj, Container) and len(obj) and not recursive:
            raise Exceptions.OperationNotPermitted(
                "Cannot delete a container object that isn't empty - set recursive to True to proceed"
            )

        return obj.path

    def mkdir(self, path: str):
        with tempfile.TemporaryDirectory() as directory:
            return self.put(directory, path)

    def touch(self, path: str) -> Artefact:

        with tempfile.TemporaryDirectory() as directory:
            emptyFile = os.path.join(directory, 'empty_file')
            open(emptyFile, 'w').close()
            return self.put(emptyFile, path)

    @abc.abstractmethod
    def toConfig(self):
        """ Return a config of the arguments to generate this manager again for saving and reloading of the manager """
        pass

    @abc.abstractmethod
    def refresh(self):
        """ Trigger the manager to re-assess the state of its artefacts, as to capture modifications made not using 
        this interface.
        """
        pass

    @abc.abstractclassmethod
    def CLI(self):
        """ Provide a CLI for the manager construction """
        pass