from ..callbacks import AbstractCallback

class TestCallback(AbstractCallback):

    artefacts = {}

    def __init__(self, artefact, is_downloading):
        self.__class__.artefacts[artefact.path] = {
            "artefact": artefact,
            "is_downloading": is_downloading
        }
        self.artefact = artefact

    def __call__(self, bytes_transferred):
        self.__class__.artefacts[self.artefact.path]['bytes_transferred'] = (
            self.__class__.artefacts[self.artefact.path].get('bytes_transferred', 0) + bytes_transferred
        )