import os
import abc
import typing
import tempfile

from .interfaces import Container, Artefact, Exceptions
from .artefacts import File, Directory

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

    def ls(self, path: str = '/', recursive: bool = False):

        # Get from the manager store the object for this path - If failed to collect raise membership error
        art = self._paths.get(path)
        if art is None: raise Exceptions.ArtefactNotFound("No directory found at location: {}".format(path))

        # Return the contents of the artefact - if not a container artefact raise error
        if isinstance(art, Directory): return art.ls(recursive)
        raise TypeError("None directory artefact found at location")

    def mv(self, src_remote, dest_remote):

        with tempfile.TemporaryDirectory() as directory:

            # Resolve the artefact with it's path - declear a local path for item
            src_path = src_remote.path if isinstance(src_remote, Artefact) else src_remote
            download = os.path.abspath(os.path.join(directory, src_path.strip('/')))

            # Download the content into the local space
            self.get(src_remote, download)

            # Upload the item to where it should be
            self.put(download, dest_remote)

            # Delete the original file
            self.rm(src_remote)

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

