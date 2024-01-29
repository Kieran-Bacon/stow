from kubernetes import client, config, stream
from kubernetes.client.exceptions import ApiException

import os
import io
import json
import datetime
import typing
from typing import Generator, Union, Tuple, Optional, Dict, Any, List, Type, overload
from typing_extensions import ParamSpec
import urllib.parse
import dataclasses
import tempfile
import tarfile
import logging
import yaml
from websocket import ABNF
import select

from .. import utils
from .. import exceptions
from ..callbacks import AbstractCallback
from ..storage_classes import StorageClass
from ..types import HashingAlgorithm
from ..worker_config import WorkerPoolConfig
from ..types import StrOrPathLike
from ..artefacts import Artefact, File, Directory, ArtefactType, PartialArtefact
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

    try:
        filepath, filetype, size, createTimestamp, modifiedtimestamp, accessedTimestamp = line.split('-:-')
    except:
        raise ValueError('Failed to parse stat line provides: ' + line)

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
    SAFE_FILE_OVERWRITE = True

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

        # Test the configured client
        try:
            self.client.list_namespace()

        except ApiException as exception:
            if exception.status == 403:
                body = json.loads(exception.body)
                logger.error("Access is forbidden to the API for the following reason: %s", body['message'])

            raise exceptions.OperationNotPermitted('Failed to communicate with cluster API: ' + body['message'])

        except Exception as e:
            logger.exception('Unexpected error communciating with cluster API')
            raise

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
                stat.path.removeprefix(self._path),
                createdTime=stat.createdTime,
                modifiedTime=stat.modifiedTime,
                accessedTime=stat.accessedTime
            )

        else:
            return File(
                self,
                stat.path.removeprefix(self._path),
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
            return None

    def _exists(self, managerPath: str) -> bool:
        return self._identifyPath(managerPath) is not None

    def _isLink(self, file: str) -> bool:
        raise NotImplementedError('Checking file is link is not yet supported')

    def _isMount(self, directory: str) -> bool:
        raise NotImplementedError('Checking is mount is not yet supported')

    def _get(
            self,
            source: ArtefactType,
            destination: str,
            /,
            callback: AbstractCallback,
            worker_config: WorkerPoolConfig,
            modified_time: Optional[float],
            accessed_time: Optional[float]
        ):

        callback.writing(1)
        namespace, pod, path = self._pathComponents(source.path)
        path = (path or '/')

        with tempfile.TemporaryFile() as tar_buffer:

            streamExec = stream.stream(
                self.client.connect_get_namespaced_pod_exec,
                pod,
                namespace,
                command=['tar', 'cPf', '-', path, '--atime-preserve'],
                stderr=True,
                stdin=True,
                stdout=True,
                tty=False,
                _preload_content=False
            )
            transfer = callback.get_bytes_transfer(destination)

            while streamExec.is_open():
                if not streamExec.sock.connected:
                    break

                # Could not explain this line if I tried
                r, _, _ = select.select((streamExec.sock.sock,), (), (), 0)

                # Receive data from the stream
                op_code, frame = streamExec.sock.recv_data_frame(True)
                if op_code == ABNF.OPCODE_CLOSE:
                    break

                elif (op_code == ABNF.OPCODE_BINARY or op_code == ABNF.OPCODE_TEXT) and len(frame.data) > 1:
                    channel, data = frame.data[0], frame.data[1:]

                    if channel == 1:
                        # Stdout
                        transfer(len(data))
                        tar_buffer.write(data)

                    elif channel == 2:
                        # stderr
                        logger.error(data)

            streamExec.close()

            # Complete the write and then seek to
            tar_buffer.flush()
            tar_buffer.seek(0)

            with tarfile.open(fileobj=tar_buffer, mode='r:') as tar:
                members = tar.getmembers()
                callback.writing(len(members))

                for member in members:
                    relative = member.path.removeprefix(path).lstrip('/')
                    local_path = os.path.abspath(os.path.join(destination, relative))

                    if member.isdir():
                        tar.makedir(member, local_path)

                    else:
                        tar.makefile(member, local_path)

                    utils.utime(local_path, modified_time=(modified_time or member.mtime), accessed_time=accessed_time)

                    callback.written(local_path)

        callback.written(destination)

    def _getBytes(
            self,
            source: Artefact,
            /,
            callback: AbstractCallback
        ) -> bytes:

        namespace, pod, path = self._pathComponents(source.path)
        if namespace is None or pod is None or path is None:
            raise exceptions.OperationNotPermitted("Can only get bytes of file objects - {source} is not a file")

        # Change to be bytes by default by reading it from the pipe instread of having it converted and then converted back
        return self._execPodCommand(namespace=namespace, pod=pod, command='cat ' + path).encode('utf-8')



    def _putTarFile(
            self,
            tarFileHandle: typing.BinaryIO,
            namespace: str,
            pod: str,
            path: str,
            /,
            callback: AbstractCallback,
            modified_time: Optional[float],
            accessed_time: Optional[float],
        ):

        # Extract the paths needed to write to the target
        destinationDirectory = self.dirname(path)

        # Create a command to extract the standard input
        streamExec = stream.stream(
            self.client.connect_get_namespaced_pod_exec,
            pod,
            namespace,
            command=['tar', 'xvf', '-', '-C', destinationDirectory],
            stderr=True,
            stdin=True,
            stdout=True,
            tty=False,
            _preload_content=False
        )

        # Fetch the location of the file head which should be at the end of the file so its a good indication of file size
        transfer = callback.get_bytes_transfer(path, tarFileHandle.tell())
        tarFileHandle.seek(0)

        while streamExec.is_open():
            # Read at 4 mb/request
            segment = tarFileHandle.read(1024*1024*4)
            if segment:
                streamExec.write_stdin(segment)
                transfer(len(segment))

            else:
                break

        streamExec.close()

    def _put(
            self,
            source: Artefact,
            destination: str,
            /,
            callback: AbstractCallback,
            modified_time: Optional[float],
            accessed_time: Optional[float],
            **kwargs
        ) -> ArtefactType:

        #https://github.com/kubernetes/kubectl/blob/97bd96adbceb24fd598bdc698da8794cb0b88e3b/pkg/cmd/cp/cp.go

        namespace, pod, path = self._pathComponents(destination)

        if namespace is None or pod is None:
            raise exceptions.OperationNotPermitted('You cannot ')

        with source.localise() as abspath:
            with tempfile.TemporaryFile() as tar_buffer:
            # with open('temp-file-buffer.tar', 'wb+') as tar_buffer:

                # Create the archive file for the transport of the source
                with tarfile.open(fileobj=tar_buffer, mode='w:') as tar:
                    tar.add(abspath, arcname=self.basename(destination))

                self._putTarFile(
                    tar_buffer,
                    namespace,
                    pod,
                    path,
                    callback=callback,
                    modified_time=modified_time,
                    accessed_time=accessed_time
                )

        return PartialArtefact(self, destination)

    def _putBytes(
            self,
            fileBytes: bytes,
            destination: str,
            *,
            callback: AbstractCallback,
            modified_time: Optional[float],
            accessed_time: Optional[float],
            **kwargs
        ) -> File:

            # Extract the componets from the path
            namespace, pod, path = self._pathComponents(destination)
            if namespace is None or pod is None or path is None:
                raise exceptions.OperationNotPermitted('You cannot write file at a directory level: ' + destination)

            # Create the in memory file buffer - assumed this is posssible as bytes is already in memory
            tarFileBuffer = io.BytesIO()

            # Create the archive file for the transport of the source
            with tarfile.open(fileobj=tarFileBuffer, mode='w:') as tar:
                tarInfo = tarfile.TarInfo(self.basename(destination))
                tarInfo.size = len(fileBytes)
                tar.addfile(tarInfo, io.BytesIO(fileBytes))

            # Call the file upload method
            self._putTarFile(
                tarFileBuffer,
                namespace,
                pod,
                path,
                callback=callback,
                modified_time=modified_time,
                accessed_time=accessed_time
            )

            return PartialArtefact(self, destination)

    def _cp(
            self,
            source: Artefact,
            destination: str,
            /,
            callback: AbstractCallback,
            modified_time: Optional[float],
            accessed_time: Optional[float],
            delete: bool = False,
            **kwargs
        ) -> ArtefactType:

        sourceNamespace, sourcePod, sourcePath = self._pathComponents(source.path)
        if sourceNamespace is None or sourcePod is None or sourcePath is None:
            raise exceptions.OperationNotPermitted('Cannot copy namespace or pod objects')

        destNamespace, destPod, destPath = self._pathComponents(destination)
        if destNamespace is None or destPod is None or destPath is None:
            raise exceptions.OperationNotPermitted('Cannot copy to namespace or pod objects')

        if sourceNamespace != destNamespace or sourcePod != destPod:
            logger.warning(
                f"Cannot copy between different pods {sourceNamespace}/{sourcePod} -> {destNamespace}/{destPod} - defaulting to localise put"
            )
            return self._put(source, destination, callback=callback, modified_time=modified_time, accessed_time=accessed_time)

        self._execPodCommand(sourceNamespace, sourcePod, f"cp -r '{sourcePath}' '{destPath}'")

        if delete:
            self._execPodCommand(sourceNamespace, sourcePod, f"rm -r '{sourcePath}'")

        return PartialArtefact(self, destination)

    def _mv(self, *args, **kwargs) -> ArtefactType:
        return self._cp(*args, delete=True, **kwargs)

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
            path = path or '/'

            command = f"find {path} ! -path {path}"
            if not recursive:
                command += " -maxdepth 1"

            result = self._execPodCommand(namespace, pod, f'{command} | xargs {_STAT_COMMAND}')

            for line in result.splitlines():
                yield self._statToArtefact(parseStatLine(line, namespace, pod))

    def _rm(self, artefact: str, /, callback: AbstractCallback, **kwargs):

        namespace, pod, path = self._pathComponents(artefact)
        if namespace is None or pod is None or path is None:
            raise exceptions.OperationNotPermitted('Not allowed to deleted this things!')

        self._execPodCommand(namespace=namespace, pod=pod, command=f'rm -r {path}')


    def _digest(self, file: File, algorithm: HashingAlgorithm):

        namespace, pod, path = self._pathComponents(file.path)
        if namespace is None or pod is None or path is None:
            raise exceptions.OperationNotPermitted('Cannot get MD5 hash of namespaces or pods')

        if algorithm is HashingAlgorithm.MD5:
            return self._execPodCommand(namespace, pod, f'md5sum {path}').split(' ')[0]

        elif algorithm is HashingAlgorithm.SHA1:
            return self._execPodCommand(namespace, pod, f'sha1sum {path}').split(' ')[0]

        elif algorithm is HashingAlgorithm.SHA256:
            return self._execPodCommand(namespace, pod, f'sha256sum {path}').split(' ')[0]

        elif algorithm is HashingAlgorithm.CRC32:
            hash = self._execPodCommand(namespace, pod, f'crc32 {path}')
            if hash.startswith("Command 'crc32' not found"):
                raise NotImplementedError('Pod does not support calculating crc32')
            return hash
        # elif algorithm is HashingAlgorithm.CRC32C:

        else:
            raise NotImplementedError(f'Unsupported hashing algorithm {algorithm} for {self.__class__}')


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
                (('-p', '--path',), {'help': 'The default path for the manager'}),
                (('-k', '--kube-config-path',), {'help': 'Provide path to kube config to load for credentials'}),
                (('-c', '--context',), {'help': 'Select the kubernetes context'}),
            ]

        def initialise(self, kwargs: Dict[str, str]):
            return self._manager(
                path=kwargs.get('path') or '',
                context=kwargs.get('context'),
                kube_config_path=kwargs.get('kube_config_path'),
            )

