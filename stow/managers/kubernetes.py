from kubernetes import client, config, stream

import datetime
from typing import Generator, Union, Tuple, Optional, Dict, Any, List

import urllib.parse
import dataclasses

from stow.artefacts.artefacts import Artefact
from stow.callbacks import AbstractCallback
from stow.storage_classes import StorageClass
from stow.types import HashingAlgorithm
from stow.worker_config import WorkerPoolConfig
from ..types import StrOrPathLike
from ..artefacts import File, Directory, ArtefactType
from ..manager.base_managers import RemoteManager

config.load_kube_config()

@dataclasses.dataclass
class Stat:
    path: str
    isDirectory: bool
    size: int
    createdTime: datetime.datetime
    modifiedTime: datetime.datetime
    accessedTime: datetime.datetime


class Kubernetes(RemoteManager):

    _STAT_COMMAND = 'stat --format="%n-:-%F-:-%s-:-%W-:-%Y-:-%X"'
    @staticmethod
    def parseStatLine(line: str, namespace: str, pod: str) -> Stat:
        """Parse a line processed according to the _STAT_COMMAND into a dict of accessible values

        Args:
            line (str): The stat output line
            namespace (str): The namespace of the pod hosting the artefact
            pod (str): The name of the pod the artefact is present on

        Returns:
            Dict[str, Any]: A dictionary mapping key to value
        """

        filepath, filetype, size, createTimestamp, modifiedtimestamp, accessedTimestamp = line.split('-:-')

        assert filepath[0] == '/', "Stat file is not abspath"

        return Stat(
            path = '/' + '/'.join((namespace, pod, filepath[1:])),
            isDirectory = filetype == 'directory',
            size = int(size),
            createdTime = datetime.datetime.fromtimestamp(int(createTimestamp), tz=datetime.timezone.utc),
            modifiedTime = datetime.datetime.fromtimestamp(int(modifiedtimestamp), tz=datetime.timezone.utc),
            accessedTime = datetime.datetime.fromtimestamp(int(accessedTimestamp), tz=datetime.timezone.utc),
        )

    def __init__(self, namespace: str = 'default'):
        self.client = client.CoreV1Api()
        self.namespace = namespace

    def _abspath(self, managerPath: str) -> str:
        return self.join(f"k8s://{self.namespace}", managerPath)

    def _pathComponents(self, path: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """ Parse a path into the kubernetes parts of the form /{namespace}/{pod}{path}

        e.g. /default/exercise-session-processor-7df79cbb4b-wfp8q/home/project/src
            namespace=default, pod=exercise-session-processor-7df79cbb4b-wfp8q, path=/home/project/src

        Args:
            path (str): The path to be broken down into components

        Returns:
            Tuple[Optional[str], Optional[str], Optional[str]]: The components of the path, the namespace, pod name, and
            absolute path on the pod
        """

        components = path.split('/')
        if len(components) <= 1:
            return None, None, None
        elif len(components) == 2:
            return components[1], None, None
        elif len(components) == 3:
            return components[1], components[2], None
        else:
            return components[1], components[2], '/' + '/'.join(components[3:])

    def _statToArtefact(self, stat: Stat) -> ArtefactType:

        if stat.isDirectory:
            return Directory(
                self,
                stat.path,
                createdTime=stat.createdTime,
                modifiedTime=stat.modifiedTime,
                accessedTime=stat.accessedTime
            )

        else:
            return File(
                self,
                stat.path,
                size=stat.size,
                modifiedTime=stat.modifiedTime,
                createdTime=stat.createdTime,
                accessedTime=stat.accessedTime
            )

    def _identifyPath(self, managerPath: str) -> Union[File, Directory, None]:
        ...

    def _exists(self, managerPath: str) -> bool:
        ...

    def _isLink(self, file: str) -> bool:
        ...

    def _isMount(self, directory: str) -> bool:
        return super()._isMount(directory)

    def _get(
            self,
            source: Artefact,
            destination: str,
            /,
            callback: AbstractCallback,
            worker_config: WorkerPoolConfig,
            modified_time: Optional[float],
            accessed_time: Optional[float]
        ):
        return super()._get(source, destination, callback, worker_config, modified_time, accessed_time)

    def _getBytes(
            self,
            source: Artefact,
            /,
            callback: AbstractCallback
        ) -> bytes:
        return super()._getBytes(source, callback)

    def _put(
            self,
            source: Artefact,
            destination: str,
            /,
            callback: AbstractCallback,
            metadata: Optional[Dict[str, str]],
            modified_time: Optional[float],
            accessed_time: Optional[float],
            content_type: Optional[str],
            storage_class: Optional[StorageClass],
            worker_config: WorkerPoolConfig
        ) -> ArtefactType:
        return super()._put(source, destination, callback, metadata, modified_time, accessed_time, content_type, storage_class, worker_config)

    def _putBytes(
            self,
            fileBytes: bytes,
            destination: str,
            *,
            callback: AbstractCallback,
            metadata: Optional[Dict[str, str]],
            modified_time: Optional[float],
            accessed_time: Optional[float],
            content_type: Optional[str],
            storage_class: Optional[StorageClass]
        ) -> File:
        return super()._putBytes(fileBytes, destination, callback=callback, metadata=metadata, modified_time=modified_time, accessed_time=accessed_time, content_type=content_type, storage_class=storage_class)

    def _cp(
            self,
            source: Artefact,
            destination: str,
            /,
            callback: AbstractCallback,
            worker_config: WorkerPoolConfig,
            metadata: Optional[Dict[str, str]],
            modified_time: Optional[float],
            accessed_time: Optional[float],
            content_type: Optional[str],
            storage_class: Optional[StorageClass],
        ) -> ArtefactType:
        return super()._cp(source, destination, callback, worker_config, metadata, modified_time, accessed_time, content_type, storage_class)

    def _mv(
            self,
            source: Artefact,
            destination: str,
            /,
            callback: AbstractCallback,
            worker_config: WorkerPoolConfig,
            metadata: Optional[Dict[str, str]],
            modified_time: Optional[float],
            accessed_time: Optional[float],
            storage_class: Optional[StorageClass],
            content_type: Optional[str]
        ) -> ArtefactType:
        return super()._mv(source, destination, callback, worker_config, metadata, modified_time, accessed_time, storage_class, content_type)

    def _ls(self, artefact: str, recursive: bool = False) -> Generator[ArtefactType, None, None]:

        # Break the artefact path down into its components
        namespace, pod, path = self._pathComponents(artefact)

        if not namespace:
            for namespace in self.client.list_namespace().items:
                namespace = namespace.to_dict()

                metadata = namespace['metadata']

                name = metadata.pop('name')
                creationTime = metadata.pop('creation_timestamp')

                yield Directory(
                    self,
                    '/' + name,
                    createdTime=creationTime,
                    modifiedTime=creationTime,
                    metadata=metadata,
                    isMount=False,
                )

        elif not pod:
            for pod in self.client.list_namespaced_pod(namespace).items:
                pod = pod.to_dict()
                metadata = pod['metadata']

                name = metadata.pop('name')
                creationTime = metadata.pop('creation_timestamp')

                yield Directory(
                    self,
                    '/' + '/'.join((namespace, name)),
                    createdTime=name,
                    modifiedTime=creationTime,
                    metadata=metadata,
                    isMount=False,
                )

        else:

            command = f"find {path} ! -path {path}"
            if not recursive:
                command += " -maxdepth 1"

            result = stream.stream(
                self.client.connect_get_namespaced_pod_exec,
                pod,
                namespace,
                command=['bash', '-c', f'{command} | xargs {self._STAT_COMMAND}'],
                stdin=True,
                stdout=True,
                stderr=True,
                tty=False
            )

            for line in result.splitlines():
                yield self._statToArtefact(self.parseStatLine(line, namespace, pod))

    def _rm(self, artefact: ArtefactType, /, callback: AbstractCallback):
        ...


    def _digest(self, file: File, algorithm: HashingAlgorithm):
        return super()._digest(file, algorithm)

    @classmethod
    def _signatureFromURL(cls, url: urllib.parse.ParseResult):
        ...

    def toConfig(self) -> dict:
        ...

    @property
    def root(self) -> str:
        ...
