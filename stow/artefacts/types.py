from typing import Callable, Dict, Union

from ..types import StrOrPathLike
from .artefacts import Artefact, File, Directory

ArtefactType = Union[File, Directory]
ArtefactOrPathLike = Union[ArtefactType, StrOrPathLike]
MetadataDynamicField = Callable[[ArtefactType], Union[str, None]]
Metadata = Dict[str, Union[str, MetadataDynamicField]]
FrozenMetadata = Dict[str, str]