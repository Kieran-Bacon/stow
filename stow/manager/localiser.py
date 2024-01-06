import abc
import contextlib

class Localiser(contextlib.AbstractContextManager):

    def __init__(self):
        pass

    @abc.abstractmethod
    def start(self) -> str:
        """ Returns path to the localised artefact """
        pass

    @abc.abstractmethod
    def close(self):
        pass

    def __enter__(self) -> str:
        return self.start()

    def __exit__(self, exeception_type, exeception_value, exeception_traceback):
        self.close()

        if exeception_type:
            return False