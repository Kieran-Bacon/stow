import os
import abc
import typing

from ..manager.localiser import Localiser
from .. import exceptions
StrOrPathLike = typing.Union[str, os.PathLike[str]]

class ManagerInterface:

    @abc.abstractmethod
    def __getitem__(self, key: StrOrPathLike) -> typing.Any:
        pass

    @abc.abstractmethod
    def _abspath(self, path: StrOrPathLike) -> str:
        pass

    @abc.abstractmethod
    def basename(self, path: StrOrPathLike) -> str:
        pass

    @abc.abstractmethod
    def dirname(self, path: StrOrPathLike) -> str:
        pass

    @abc.abstractmethod
    def join(self, *paths: StrOrPathLike) -> str:
        pass

    @abc.abstractmethod
    def mv(self, src, dest) -> None:
        pass

    @abc.abstractmethod
    def rm(self, path: StrOrPathLike, recursive: bool) -> None:
        pass

    @abc.abstractmethod
    def localise(self, path: StrOrPathLike) -> Localiser:
        ...

    @abc.abstractmethod
    def toConfig(self) -> None:
        pass


# class ArtefactInterface:


#     @property
#     @abc.abstractmethod
#     def manager(self) -> typing.Any:
#         raise NotImplementedError()

#     @property
#     @abc.abstractmethod
#     def path(self) -> str:
#         raise NotImplementedError()

# class FileInterface(ArtefactInterface):
#     pass

# class DirectoryInterface(ArtefactInterface):
#     pass
