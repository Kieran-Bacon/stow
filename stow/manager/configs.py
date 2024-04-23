import abc
from typing import List, Tuple, Dict, Any

class AbstractCommandLineConfig(abc.ABC):

    @abc.abstractmethod
    def arguments() -> List[Tuple[Tuple[str, str], Dict[str, Any]]]:
        pass

    @abc.abstractmethod
    def initialise(self, kwargs: Dict[str, Any]):
        pass