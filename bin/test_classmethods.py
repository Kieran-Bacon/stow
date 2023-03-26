class A:

    @classmethod
    def chess(cls):
        return type(cls)


class B(A):
    def chess(self):
        return super().chess()
        # return "hello"

print(A.chess())
print(B().chess())