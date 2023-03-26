class A:

    def __init__(self, name):
        self._name = name

    def basename(self):
        return self._name


class PartialA:

    def __init__(self, a: A):
        self._a = a
        self._called = False

    def __getattribute__(self, attr: str):
        print(f'called once {attr}')

        if object.__getattribute__(self, "__class__").__name__ != "PartialA":
            return object.__getattribute__(self, attr)

        # Load the A
        a = object.__getattribute__(self, '_a')
        newA = A(a)

        self.__class__ = type(newA.__class__.__name__,(newA.__class__,),{})
        self.__dict__ = newA.__dict__

        if attr == "__class__":
            return A

        return object.__getattribute__(self, attr)



something = PartialA('Kieran')

print(type(something))

print(isinstance(something, A))

print(type(something))

print(isinstance(something, A))

print(something.basename())

print(something.basename())

