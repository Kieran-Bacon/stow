import os
import abc
import typing
from typing import Union, Optional, Tuple, Any

import tqdm
import queue

import logging
log = logging.getLogger(__name__)

class AbstractCallback(abc.ABC): # pragma: no cover
    """ The base interface for callbacks to be passed to stow interface. Called by the manager when
    transfer is occuring.

    Args:
        artefact (Artefact): The artefact that is being uploaded/downloaded
        is_downloading (bool) = True: Toggled by the manager depending on what activity is happening
    """

    @abc.abstractmethod
    def reviewing(self, count: int):
        ...

    @abc.abstractmethod
    def reviewed(self, pathOrCount: Union[str, int]):
        ...

    @abc.abstractmethod
    def writing(self, count: int):
        ...

    @abc.abstractmethod
    def written(self, pathOrCount: Union[str, int]):
        ...

    @abc.abstractmethod
    def deleting(self, count: int):
        ...

    @abc.abstractmethod
    def deleted(self, pathOrCount: Union[str, int]):
        ...

    @abc.abstractmethod
    def get_bytes_transfer(self, path: str, bytes: int):
        pass

class NoneImplementedCallback(AbstractCallback):
    def reviewing(self, count: int): return super().reviewing(count)
    def reviewed(self, pathOrCount: Union[str,int]): return super().reviewed(pathOrCount)
    def writing(self, count: int): return super().writing(count)
    def written(self, pathOrCount: Union[str, int]): return super().written(pathOrCount)
    def deleting(self, count: int): return super().deleting(count)
    def deleted(self, pathOrCount: Union[str,int]):return super().deleted(pathOrCount)
    def get_bytes_transfer(self, path: str, bytes: int): return super().get_bytes_transfer(path, bytes)

class DefaultCallback(NoneImplementedCallback): # pragma: no cover

    _target: Optional[AbstractCallback] = None

    @classmethod
    def become(cls, target: AbstractCallback):
        cls._target = target

    def __getattribute__(self, attr):
        if attr == 'become':
            return super().__getattribute__(attr)
        if attr == 'get_bytes_transfer':
            return lambda *args, **kwargs: (lambda *args, **kwargs: None)
        return getattr(super().__getattribute__('_target'), attr, lambda *args, **kwargs: None)


import weakref
class ProgressCallback(AbstractCallback):

    def __init__(self):

        self._reviewingProgressBar = None
        self._writingProgressBar = None
        self._deletingProgressBar = None

        self._positionPool = queue.Queue()
        self._positionOffset = -1
        self._transferBars = []


    def _getNextPositionOffset(self) -> int:

        try:
            position = self._positionPool.get_nowait()
        except queue.Empty:
            self._positionOffset += 1
            position = self._positionOffset

        # print(position)
        return position

    @staticmethod
    def _updatePbar(pbar: tqdm.tqdm, pathOrCount: Union[str, int]):
        if isinstance(pathOrCount, int):
            pbar.update(pathOrCount)
        else:
            pbar.desc = pbar.desc.split(' ')[0] + " " + os.path.basename(pathOrCount)
            pbar.update()


    def _pbar(self, desc, total):
        return tqdm.tqdm(
            desc=desc,
            total=total,
            unit=' Artefacts',
            leave=True,
            position=self._getNextPositionOffset()
        )

    def reviewing(self, count: int):
        if self._reviewingProgressBar is None:
            self._reviewingProgressBar = self._pbar('Reviewing', count)
        else:
            self._reviewingProgressBar.total += count

    def reviewed(self, pathOrCount: Union[str, int]):
        if self._reviewingProgressBar:
            self._updatePbar(self._reviewingProgressBar, pathOrCount)

    def writing(self, count: int):
        if self._writingProgressBar is None:
            self._writingProgressBar = self._pbar('Writing', count)
        else:
            self._writingProgressBar.total += count

    def written(self, pathOrCount: Union[str, int]):
        self.reviewed(pathOrCount)
        if self._writingProgressBar:
            self._updatePbar(self._writingProgressBar, pathOrCount)

    def deleting(self, count: int):
        if self._deletingProgressBar is None:
            self._deletingProgressBar = self._pbar('Deleting', count)
        else:
            self._deletingProgressBar.total += count

    def deleted(self, pathOrCount: Union[str, int]):
        self.reviewed(pathOrCount)
        if self._deletingProgressBar:
            self._updatePbar(self._deletingProgressBar, pathOrCount)

    @staticmethod
    def sizeof_fmt(num, suffix="B") -> Tuple[float, int, str]:
        for i, unit in enumerate(("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi", "Yi")):
            if abs(num) < 1024.0:
                return 1024.0**i, num, unit + suffix
            num /= 1024.0
        else:
            raise RuntimeError('File size exceeds all of human data to this point - so probs a problem')

    def get_bytes_transfer(self, path, total_bytes_transfer):
        log.debug('Initialising transfer for path=%s', path)

        divisor, total, unit = self.sizeof_fmt(total_bytes_transfer)
        # divisor = 1
        total = total_bytes_transfer
        # unit = 'bytes'

        position = self._getNextPositionOffset()

        pbar = tqdm.tqdm(
            desc=f'{path} transfered',
            total=total,
            unit=unit,
            # unit_scale=divisor,
            leave=False,
            position=position
        )
        self._transferBars.append(pbar)

        transfer = pbar.update

        def onRelease():
            self._positionPool.put(position)

        weakref.finalize(transfer, onRelease)

        # def update(bytes_, released = False):
        #     if released:
        #         return
        #     pbar.update(max(pbar.total - pbar.n, bytes_/divisor))
        #     if pbar.n >= pbar.total:
        #         self._positionPool.put(abs(pbar.pos))
        #         update.__defaults__ = (True,)

        return transfer

    def close(self):
        if self._reviewingProgressBar is not None: self._reviewingProgressBar.close()
        if self._writingProgressBar is not None: self._writingProgressBar.close()
        if self._deletingProgressBar is not None: self._deletingProgressBar.close()

def composeCallback(callbacks: typing.Iterable[AbstractCallback]):
    """ Compile an iterable of callback methods together into a single Callback class object """

    class ComposedCallback(NoneImplementedCallback):
        """ Composed callback object """

        def __getattribute__(self, __name: str) -> Any:

            def apply(*args, **kwargs):
                for callback in callbacks:
                    getattr(callback, __name)(*args, **kwargs)

            return apply

    return ComposedCallback() # type: ignore
