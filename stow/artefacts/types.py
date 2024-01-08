import typing

from ..types import StrOrPathLike
from .artefacts import Artefact, File, Directory

ArtefactType = typing.Union[File, Directory]
ArtefactOrPathLike = typing.Union[ArtefactType, StrOrPathLike]