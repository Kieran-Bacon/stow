import abc
import typing

import tqdm

from .artefacts import Artefact

class AbstractCallback(abc.ABC):
    """ The base interface for callbacks to be passed to stow interface. Called by the manager when
    transfer is occuring.

    Args:
        artefact (Artefact): The artefact that is being uploaded/downloaded
        is_downloading (bool) = True: Toggled by the manager depending on what activity is happening
    """

    @abc.abstractmethod
    def __init__(self, artefact: Artefact, is_downloading: bool = True):
        pass

    @abc.abstractmethod
    def __call__(self, bytes_transfered: int) -> None:
        pass

class ProgressCallback(AbstractCallback):
    """ Create a visual progress bar on the uploading/downloading of file artefacts
    """

    def __init__(self, artefact: Artefact, is_downloading: bool = True):
        self._artefact = artefact
        self._progress = tqdm.tqdm(
            desc=f"{'Downloading' if is_downloading else 'Uploading'} {artefact.path}",
            total=len(artefact),
            unit='bytes'
        )

    def __call__(self, bytes_transfered):
        self._progress.update(bytes_transfered)


def composeCallback(callbacks: typing.Iterable[AbstractCallback]):
    """ Compile an iterable of callback methods together into a single Callback class object """

    class ComposedCallback(AbstractCallback):
        """ Composed callback object """

        # Save the callbacks on the class parameters
        _callbacks = callbacks

        def __init__(self, artefact, is_downloading):

            self._initialised_callbacks = [
                callback(artefact, is_downloading)
                for callback in self._callbacks
            ]

        def __call__(self, bytes_transfered: int):
            for callback in self._initialised_callbacks:
                callback(bytes_transfered)

    return ComposedCallback
