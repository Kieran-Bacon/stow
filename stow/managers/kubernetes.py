from kubernetes import client, config, stream

import datetime
from typing import Generator, Union, Tuple, Optional, Dict, Any, List, Type, overload
from typing_extensions import ParamSpec
import urllib.parse
import dataclasses
import tempfile
import tarfile
import logging
import yaml

from stow.artefacts.artefacts import Artefact
from stow.callbacks import AbstractCallback
from stow.storage_classes import StorageClass
from stow.types import HashingAlgorithm
from stow.worker_config import WorkerPoolConfig
from ..types import StrOrPathLike
from ..artefacts import File, Directory, ArtefactType
from ..manager import RemoteManager, AbstractCommandLineConfig

logger = logging.getLogger(__name__)



@dataclasses.dataclass
class Stat:
    path: str
    isDirectory: bool
    size: int
    createdTime: datetime.datetime
    modifiedTime: datetime.datetime
    accessedTime: datetime.datetime

_STAT_COMMAND = 'stat --format="%n-:-%F-:-%s-:-%W-:-%Y-:-%X"'
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

    # TODO the file type will be symlink for link files
    # switch (sb.st_mode & S_IFMT) {
    # case S_IFBLK:  printf("block device\n");            break;
    # case S_IFCHR:  printf("character device\n");        break;
    # case S_IFDIR:  printf("directory\n");               break;
    # case S_IFIFO:  printf("FIFO/pipe\n");               break;
    # case S_IFLNK:  printf("symlink\n");                 break;
    # case S_IFREG:  printf("regular file\n");            break;
    # case S_IFSOCK: printf("socket\n");                  break;
    # default:       printf("unknown?\n");                break;

    return Stat(
        path = '/' + '/'.join((namespace, pod, filepath[1:])),
        isDirectory = filetype == 'directory',
        size = int(size),
        createdTime = datetime.datetime.fromtimestamp(int(createTimestamp), tz=datetime.timezone.utc),
        modifiedTime = datetime.datetime.fromtimestamp(int(modifiedtimestamp), tz=datetime.timezone.utc),
        accessedTime = datetime.datetime.fromtimestamp(int(accessedTimestamp), tz=datetime.timezone.utc),
    )


class Kubernetes(RemoteManager):
    """ Connect to the kubernetes """

    SEPARATOR = '/'

    @overload
    def __init__(self, path: str = ...):
        pass
    @overload
    def __init__(self, path: str = ..., *, context: str):
        pass
    @overload
    def __init__(self, path: str = ..., *, context: str, kube_config_path: str):
        pass
    @overload
    def __init__(self, path: str = ..., *, context: str, kube_config: Dict[str, str]):
        pass
    def __init__(self, path: str = '', *, context: Optional[str] = None, kube_config_path: Optional[str] = None, kube_config: Optional[Dict[str, str]] = None):

        self._config = {
            k: v
            for k, v in {
                'context': context,
                'kube_config_path': kube_config_path,
                'kube_config': kube_config
            }.items()
            if v is not None
        }

        # Read in user config if provided and stamp on kube_config variables
        if kube_config_path is not None:
            with open(kube_config_path) as handle:
                kube_config = yaml.safe_load(handle)

        # Select the correct environment
        if kube_config:
            self.client = config.new_client_from_config_dict(kube_config, context=context)

        else:

            config.load_kube_config(context=context)
            self.client = client.CoreV1Api()


        self._path = self._managerPath(path)


    @property
    def root(self) -> str:
        return self._path

    def _cwd(self) -> str:
        return self._path

    def _abspath(self, managerPath: str) -> str:
        namespace, pod, path = self._pathComponents(managerPath)

        if pod and namespace:
            return self.join(f"k8s://{namespace}/{pod}", managerPath)
        elif namespace:
            return self.join(f"k8s://{namespace}", managerPath)
        else:
            return self.join("k8s://", managerPath)

    def _managerPath(self, path: str) -> str:
        """ Standardise path """
        return '/' + path.replace('\\', '/').strip('/')

    def _pathComponents(self, path: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """ Parse a path into the kubernetes parts of the form /{namespace}/{pod}{path}

        /' -> error
        /default - > namespace
        /default/pod - > default, pod


        e.g. /default/exercise-session-processor-7df79cbb4b-wfp8q/home/project/src
            namespace=default, pod=exercise-session-processor-7df79cbb4b-wfp8q, path=/home/project/src

        Args:
            path (str): The path to be broken down into components

        Returns:
            Tuple[Optional[str], Optional[str], Optional[str]]: The components of the path, the namespace, pod name, and
            absolute path on the pod
        """

        # Strip the delimiters from the path
        path = self.join(self._path, path, joinAbsolutes=True).strip('/')

        # The path was empty - return None or the defaults for everything
        if not path:
            return None, None, None

        # Break down the components of the path to match with whatever data is missing
        components = path.split('/')

        if len(components) == 1:
            return components[0], None, None
        elif len(components) == 2:
            return components[0], components[1], None
        else:
            return components[0], components[1], '/' + '/'.join(components[2:])

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

    def _parseV1Namespace(self, v1namespace: client.V1Namespace) -> Directory:
        namespace = v1namespace.to_dict()

        metadata = namespace['metadata']

        name = metadata.pop('name')
        creationTime = metadata.pop('creation_timestamp')

        return Directory(
            self,
            '/' + name,
            createdTime=creationTime,
            modifiedTime=creationTime,
            metadata=metadata,
            isMount=False,
        )

    def _parseV1Pod(self, v1pod: client.V1Pod) -> Directory:
        pod = v1pod.to_dict()
        metadata = pod['metadata']

        namespace = metadata.pop('namespace')
        name = metadata.pop('name')
        creationTime = metadata.pop('creation_timestamp')

        return Directory(
            self,
            '/' + '/'.join((namespace, name)),
            createdTime=name,
            modifiedTime=creationTime,
            metadata=metadata,
            isMount=False,
        )

    def _execPodCommand(self, namespace: str, pod: str, command: str) -> str:
        return stream.stream(
            self.client.connect_get_namespaced_pod_exec,
            pod,
            namespace,
            command=['bash', '-c', command],
            stdin=True,
            stdout=True,
            stderr=True,
            tty=False
        )

    def _identifyPath(self, managerPath: str) -> Union[File, Directory, None]:

        # Identify the path
        namespace, pod, path = self._pathComponents(managerPath)

        # There is nothing selected - returning the root directory
        try:
            if namespace is None:
                return Directory(self, '/')

            elif pod is None:
                return self._parseV1Namespace(
                    self.client.read_namespace(namespace) # type: ignore
                )

            elif path is None:
                return self._parseV1Pod(
                    self.client.read_namespaced_pod(pod, namespace) # type: ignore
                )

            else:
                return self._statToArtefact(
                    parseStatLine(
                        self._execPodCommand(namespace, pod, f"{_STAT_COMMAND} {path}"),
                        namespace,
                        pod
                    )
                )

        except:
            logger.exception('Failed to find: %s', managerPath)
            return None

    def _exists(self, managerPath: str) -> bool:
        return self._identifyPath(managerPath) is not None

    def _isLink(self, file: str) -> bool:
        raise NotImplementedError('Checking file is link is not yet supported')

    def _isMount(self, directory: str) -> bool:
        raise NotImplementedError('Checking is mount is not yet supported')

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

        namespace, pod, path = self._pathComponents(source.path)

        with tempfile.TemporaryFile() as tar_buffer:

            resp = stream.stream(
                self.client.connect_get_namespaced_pod_exec,
                pod,
                namespace,
                command=['tar', 'cf', '-', path],
                stderr=True, stdin=True,
                stdout=True, tty=False,
                _preload_content=False
            )

            # Parse the file stream
            while resp.is_open():
                resp.update(timeout=1)
                if resp.peek_stdout():
                    out = resp.read_stdout()
                    tar_buffer.write(out.encode('utf-8'))
                if resp.peek_stderr():
                    logger.error(resp.read_stderr())
            resp.close()

            # Complete the write and then seek to
            tar_buffer.flush()
            tar_buffer.seek(0)

            with tarfile.open(fileobj=tar_buffer, mode='r:') as tar:
                for member in tar.getmembers():
                    if member.isdir():
                        continue
                    fname = member.name.rsplit('/', 1)[1]
                    tar.makefile(member, destination + '/' + fname)



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

    def _ls(self, artefact: str, recursive: bool = False, **kwargs) -> Generator[ArtefactType, None, None]:

        # Break the artefact path down into its components
        namespace, pod, path = self._pathComponents(artefact)

        if not namespace:
            for namespace in self.client.list_namespace().items:
                yield self._parseV1Namespace(namespace)


        elif not pod:
            for pod in self.client.list_namespaced_pod(namespace).items:
                yield self._parseV1Pod(pod)


        else:

            command = f"find {path} ! -path {path}"
            if not recursive:
                command += " -maxdepth 1"

            result = self._execPodCommand(namespace, pod, f'{command} | xargs {_STAT_COMMAND}')

            for line in result.splitlines():
                yield self._statToArtefact(parseStatLine(line, namespace, pod))

    def _rm(self, artefact: ArtefactType, /, callback: AbstractCallback):
        ...


    def _digest(self, file: File, algorithm: HashingAlgorithm):
        return super()._digest(file, algorithm)

    @classmethod
    def _signatureFromURL(cls, url: urllib.parse.ParseResult):

        # Extract the query data passed into
        queryData = urllib.parse.parse_qs(url.query)

        signature = {
            "context": queryData.get("context", [None])[0],
            "kube_config_path": queryData.get("kube_config_path", [None])[0],
        }

        return signature, (url.netloc + url.path or '/')

    @property
    def config(self) -> dict:
        return self._config

    class CommandLineConfig(AbstractCommandLineConfig):

        def __init__(self, manager: Type["Kubernetes"]):
            self._manager = manager

        @staticmethod
        def arguments() -> List[Tuple[Tuple[str, str], Dict[str, Any]]]:
            return [
                (('-k', '--kube_config_path',), {'help': 'Provide path to kube config to load for credentials'}),
                (('-c', '--context',), {'help': 'Select the kubernetes context'}),
            ]

        def initialise(self, kwargs: Dict[str, str]):
            return self._manager(
                context=kwargs.get('context'),
                kube_config_path=kwargs.get('kube_config_path'),
            )

