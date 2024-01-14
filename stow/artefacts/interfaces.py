import os
import abc
import typing
from typing import (IO, Any, Union, Optional, Literal, Dict, Tuple, Generator, overload)
import datetime

from ..storage_classes import StorageClassInterface
from ..worker_config import WorkerPoolConfig
from ..callbacks import AbstractCallback, DefaultCallback
from ..types import StrOrPathLike, TimestampLike, HashingAlgorithm
from ..localiser import Localiser
from .. import exceptions


Artefact = typing.TypeVar('Artefact', bound=os.PathLike)


class ManagerInterface:

    @abc.abstractmethod
    def __getitem__(self, key: StrOrPathLike) -> Any:
        pass

    @abc.abstractmethod
    def _abspath(self, path: StrOrPathLike) -> str:
        pass

    def _metadata(self, path: str) -> Dict[str, Any]:
        ...

    def _get_content_type(self, path: str) -> str:
        ...

    def _set_content_type(self, path: str, content_type: str):
        ...

    def _isMount(self, artefact) -> bool:
        ...

    def _isLink(self, artefact) -> bool:
        ...

    def _rm(self, path: StrOrPathLike, *, callback: AbstractCallback):
        ...

    def artefact(self, artefact: StrOrPathLike, type: Optional[os.PathLike[str]] = None) -> os.PathLike[str]:
        ...

    def exists(self, artefact) -> bool:
        ...

    @abc.abstractmethod
    def basename(self, path: StrOrPathLike) -> str:
        pass

    @abc.abstractmethod
    def dirname(self, path: StrOrPathLike) -> str:
        pass

    def relpath(self, path: str, path2: str, separator: str) -> str:
        ...

    @abc.abstractmethod
    def join(self, *paths: StrOrPathLike, separator: Optional[str] = None, joinAbsolutes: bool = False) -> str:
        pass

    def mkdir(self, path: str):
        ...


    def touch(self, path: str):
        ...

    @overload
    @abc.abstractmethod
    def get(
        self,
        source: Artefact,
        destination: str,
        overwrite: bool = False,
        *,
        callback: AbstractCallback = DefaultCallback(),
        modified_time: Optional[TimestampLike] = None,
        accessed_time: Optional[TimestampLike] = None,
        worker_config: Optional[WorkerPoolConfig] = None,
        ) -> Artefact:
        ...
    @overload
    @abc.abstractmethod
    def get(
        self,
        source: os.PathLike[str],
        destination: Literal[None] = None,
        overwrite: bool = False,
        *,
        callback: AbstractCallback = DefaultCallback(),
        modified_time: Optional[TimestampLike] = None,
        accessed_time: Optional[TimestampLike] = None,
        worker_config: Optional[WorkerPoolConfig] = None,
        ) -> bytes:
        ...
    @abc.abstractmethod
    def get(
        self,
        source: Artefact,
        destination: Optional[str] = None,
        overwrite: bool = False,
        *,
        callback: AbstractCallback = DefaultCallback(),
        modified_time: Optional[TimestampLike] = None,
        accessed_time: Optional[TimestampLike] = None,
        worker_config: Optional[WorkerPoolConfig] = None,
    ) -> Union[Artefact, bytes]:
        ...

    def put(
        self,
        source: Artefact,
        path: StrOrPathLike,
        *,
        metadata: Optional[Dict[str, str]],
        content_type: Optional[str],
        storage_class: StorageClassInterface
        ) -> Artefact:
        ...

    @abc.abstractmethod
    def mv(self, src, dest) -> None:
        pass

    @abc.abstractmethod
    def rm(
        self,
        *path: StrOrPathLike,
        recursive: bool,
        callback: AbstractCallback,
        worker_config: Optional[WorkerPoolConfig],
        ignore_missing: bool = False
        ) -> None:
        pass

    def iterls(self, artefact, recursive: bool, ignore_missing: bool) -> Generator[os.PathLike, None, None]:
        ...

    def open(self, artefact, mode: str) -> IO[Any]:
        ...

    @abc.abstractmethod
    def localise(self, path: StrOrPathLike) -> Localiser:
        ...

    @abc.abstractmethod
    def setmtime(self, path: StrOrPathLike, modifiedTime: TimestampLike) -> datetime.datetime:
        ...

    @abc.abstractmethod
    def setatime(self, path: StrOrPathLike, modifiedTime: TimestampLike) -> datetime.datetime:
        ...

    def set_artefact_time(self, artefact, modified_time: Optional[TimestampLike], accessed_time: Optional[TimestampLike]) -> Tuple[float, float]:
        ...

    def digest(self, artefact, algo: HashingAlgorithm) -> str:
        ...

    @property
    @abc.abstractmethod
    def config(self) -> Dict[str, str]:
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
