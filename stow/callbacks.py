import abc
import typing

import tqdm

import logging
log = logging.getLogger(__name__)

class AbstractCallback(abc.ABC): # pragma: no cover
    """ The base interface for callbacks to be passed to stow interface. Called by the manager when
    transfer is occuring.

    Args:
        artefact (Artefact): The artefact that is being uploaded/downloaded
        is_downloading (bool) = True: Toggled by the manager depending on what activity is happening
    """

    def setDescription(self, description: str):
        pass

    @abc.abstractmethod
    def addTaskCount(*args):
        pass

    @abc.abstractmethod
    def added(*args):
        pass

    @abc.abstractmethod
    def get_bytes_transfer(*args):
        pass

    @abc.abstractmethod
    def removed(*args):
        pass

def do_nothing(*args, **kwargs): # pragma: no cover
    pass

class DefaultCallback(AbstractCallback): # pragma: no cover

    def addTaskCount(*args, **kwargs):
        pass

    def added(*args):
        pass

    def get_bytes_transfer(self, *args):
        return do_nothing

    def removed(*args):
        pass

class ProgressCallback(AbstractCallback):

    def __init__(self, desc: str = ""):

        self._desc = desc
        self._addingArtefactsProgress = None
        self._removingArtefactsProgress = None
        self._bytesTransferedProgress = {}

        self._positionOffset = 0

    def _getNextPositionOffset(self):
        val = self._positionOffset
        self._positionOffset += 1
        return val

    def setDescription(self, description: str):
        if not self._desc:
            self._desc = description

    def addTaskCount(self, count: int, isAdding: bool = True):
        log.debug('Adding task counts: %s', (isAdding or -1)*count)

        # if self._addingArtefactsProgress is None:
        #     self._addingArtefactsProgress = tqdm.tqdm(
        #         desc=self._desc,
        #         total=1,
        #         unit='artefacts'
        #     )

        # self._addingArtefactsProgress.total += count

        if isAdding:
            if self._addingArtefactsProgress is None:
                self._addingArtefactsProgress = tqdm.tqdm(
                    desc=f"{self._desc}::Creating artefacts" if self._desc else "Creating artefacts",
                    total=count,
                    unit='artefacts',
                    position=self._getNextPositionOffset()
                )

            else:
                self._addingArtefactsProgress.total += count

        else:

            if self._removingArtefactsProgress is None:
                self._removingArtefactsProgress = tqdm.tqdm(
                    desc=f"{self._desc}::Removing artefacts" if self._desc else "Removing artefacts",
                    total=count,
                    unit='artefacts',
                    position=self._getNextPositionOffset()
                )

            else:
                self._removingArtefactsProgress.total += count

    def added(self, path):
        log.debug('Adding path=%s', path)

        if path in self._bytesTransferedProgress:
            pbar = self._bytesTransferedProgress.pop(path)
            pbar.close()

        if self._addingArtefactsProgress:
            self._addingArtefactsProgress.update()
            if self._addingArtefactsProgress.n >= self._addingArtefactsProgress.total:
                self._addingArtefactsProgress = None
        else:
            log.info(path + ' added')

    def get_bytes_transfer(self, path, total):
        log.debug('Initialising transfer for path=%s', path)

        self._bytesTransferedProgress[path] = pbar = tqdm.tqdm(
            desc=f'{path} transfered',
            total=total,
            unit='bytes',
            leave=False,
            position=self._getNextPositionOffset()
            # disable=True
        )

        return pbar.update

    def removed(self, path):
        log.debug('Removing path=%s', path)

        if self._removingArtefactsProgress:
            self._removingArtefactsProgress.update()
            if self._removingArtefactsProgress.n >= self._removingArtefactsProgress.total:
                self._removingArtefactsProgress = None
        else:
            log.info(path + ' removed')

def composeCallback(callbacks: typing.Iterable[AbstractCallback]):
    """ Compile an iterable of callback methods together into a single Callback class object """

    class ComposedCallback(AbstractCallback):
        """ Composed callback object """

        # Save the callbacks on the class parameters
        _callbacks = callbacks

        def addTaskCount(self, *args, **kwargs):
            for callback in self._callbacks:
                callback.addTaskCount(*args, **kwargs)

        def get_bytes_transfer(self, *args, **kwargs):
            transfers = []
            for callback in self._callbacks:
                transfers.append(callback.get_bytes_transfer(*args, **kwargs))

            def transferWrapper(*args, **kwargs):
                for transfer in transfers:
                    transfer(*args, **kwargs)

            return transferWrapper

        def added(self, *args, **kwargs):
            for callback in self._callbacks:
                callback.added(*args, **kwargs)

        def removed(self, *args, **kwargs):
            for callback in self._callbacks:
                callback.removed(*args, **kwargs)

    return ComposedCallback()
