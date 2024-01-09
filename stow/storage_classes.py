import abc
import enum
from typing import (Optional, Union)
from typing_extensions import Self

class StorageClassInterface(enum.Enum):

    @classmethod
    def toGeneric(cls, value: Self) -> "StorageClass":
        raise NotImplementedError()

    @classmethod
    def fromGeneric(cls, value: Optional["StorageClass"]) -> Self:
        raise NotImplementedError()

    @classmethod
    def convert(cls, sclass: Union["StorageClassInterface", str]) -> Self:
        if isinstance(sclass, str):
            return cls(sclass)
        elif isinstance(sclass, cls):
            return sclass
        else:
            return cls.fromGeneric(sclass.toGeneric(sclass))

class StorageClass(StorageClassInterface):
    ARCHIVE = enum.auto()
    REDUCED_REDUNDANCY = enum.auto()
    STANDARD = enum.auto()
    INFREQUENT_ACCESS = enum.auto()
    INTELLIGENT_TIERING = enum.auto()
    HIGH_PERFORMANCE = enum.auto()

    @classmethod
    def toGeneric(cls, value):
        return value

    @classmethod
    def fromGeneric(cls, value):
        return value
