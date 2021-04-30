from ..utils import connect

class ManagerReloader:
    def __new__(self, config):
        # This will create a new manager if it doesn't exist of fetch the one globally created
        return connect(**config)

class ManagerSeralisable:

    def __reduce__(self):
        return (ManagerReloader, (self.toConfig(),))

    def toConfig(self):
        raise NotImplementedError("toConfig needs to be defined sufficiently to run stow.connect(**config)")