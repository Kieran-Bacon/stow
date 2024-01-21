import os
import enum
import typing

StrOrPathLike = typing.Union[str, os.PathLike[str]]

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