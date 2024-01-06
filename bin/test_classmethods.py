import enum

class StorageClass(enum.Enum):
    standard = enum.auto()
    premium = enum.auto()

class A:

    @classmethod
    def chess(cls):
        return type(cls)

    __var = 10

    @classmethod
    def pool(cls):
        return cls.__var


class B(A):

    _storageClass = StorageClass.standard
    __var = 11

    def chess(self):
        return super().chess()
        # return "hello"

    @property
    def storage_class(self) -> StorageClass:
        return self._storageClass
    @storage_class.setter
    def storage_class(self, value):
        self._storageClass = StorageClass(value)

print(A.chess())
print(B().chess())
print(B().pool())


b = B()
b2 = B()

b.storage_class = StorageClass.premium
print(b.storage_class)
print(b2.storage_class)




print(issubclass(B, B))


# stow.put(file, file, Amazon.StorageClass.STANDARD)