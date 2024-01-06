import os
import enum
import typing

from .artefacts.artefacts import Artefact, File, Directory

PathLike = typing.Union[str, os.PathLike[str]]
ArtefactType = typing.Union[File, Directory]
ArtefactOrPathLike = typing.Union[ArtefactType, PathLike]

isinstance('hello', os.PathLike)

@typing.runtime_checkable
class TimestampAble(typing.Protocol):
    def timestamp(self) -> float:
        ...

TimestampLike = typing.Union[TimestampAble, float, int]

class HashingAlgorithm(enum.Enum):
    MD5 = enum.auto()
    CRC32 = enum.auto()
    CRC32C = enum.auto()
    SHA1 = enum.auto()
    SHA256 = enum.auto()