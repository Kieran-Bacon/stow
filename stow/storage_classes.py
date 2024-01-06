import abc
import enum
from typing_extensions import Self

class StorageClassInterface(enum.Enum):

    @classmethod
    def toGeneric(cls, value: Self) -> "StorageClass":
        raise NotImplementedError()

    @classmethod
    def fromGeneric(cls, value: "StorageClass") -> Self:
        raise NotImplementedError()

    @classmethod
    def convert(cls, sclass: Self) -> Self:
        if isinstance(sclass, cls):
            return sclass
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
