""" The implementation of Amazon's S3 storage manager for stow """

# pyright: reportTypedDictNotRequiredAccess=false

import os
import re
import io
import typing
from typing import Generator, List, Union, Optional, Tuple, Type, Dict, overload
from typing_extensions import Self
import urllib.parse
import hashlib
import mimetypes
import logging
import base64
import datetime
import dataclasses

import boto3
import botocore.config
from botocore.exceptions import ClientError, UnauthorizedSSOTokenError

from ..worker_config import WorkerPoolConfig
from ..types import HashingAlgorithm
from ..artefacts import Artefact, PartialArtefact, File, Directory, ArtefactType
from ..manager import RemoteManager
from ..storage_classes import StorageClass, StorageClassInterface
from ..callbacks import AbstractCallback
from .. import exceptions

log = logging.getLogger(__name__)

def calculateBytesS3ETag(bytes_readable: typing.BinaryIO) -> str:
    """ For a given set of bytes, calculate their AWS's etag value.

    The ETag of multipart uploads is not MD5 - each part has a md5 calculated and then
    they are concaticated before they are MD5'd for the final etag. It is then followed
    by a -xxxx that stored the number of parts that made up the upload.

    Args:
        bytes_readable (typing.BinaryIO): A readable object containing the file bytes

    Returns:
        str: the string literal ETag value as found in aws api responses.
    """

    md5s = []
    while True:
        data = bytes_readable.read(8 * 1024 * 1024)
        if not data:
            break
        md5s.append(hashlib.md5(data))

    if len(md5s) < 1:
        return '"{}"'.format(hashlib.md5().hexdigest())

    if len(md5s) == 1:
        return '"{}"'.format(md5s[0].hexdigest())

    digests = b''.join(m.digest() for m in md5s)
    digests_md5 = hashlib.md5(digests)
    return '"{}-{}"'.format(digests_md5.hexdigest(), len(md5s))

def etagComparator(*files: File) -> bool:
    """ A basic file comparator using S3 ETag calculation - generates ETag values for non s3 files by localising and
    processing, retrieves ETag from s3 for s3 files.

    Args:
        *files (List[File]): A list of files to compare

    Returns:
        bool: True if the files are the same
    """

    digests = []
    for f in files:
        if isinstance(f._manager, Amazon):
            digests.append(f.metadata['ETag'])

        else:
            with f.open('rb') as handle:
                digests.append(calculateBytesS3ETag(handle))

    return all(digests[0] == d for d in digests[1:])

@dataclasses.dataclass
class FileStat:
    key: str
    lastModified: datetime.datetime
    size: int
    storageClass: str

@dataclasses.dataclass
class DirectoryStat:
    prefix: str

@dataclasses.dataclass
class Dir:
    files: List[FileStat] = dataclasses.field(default_factory=list)
    directories: List[DirectoryStat] = dataclasses.field(default_factory=list)

    def __len__(self) -> int:
        return len(self.files) + len(self.directories)

class Amazon(RemoteManager):
    """ Connect to an amazon s3 bucket using an IAM user credentials or environment variables

    Params:
        bucket: The s3 bucket name
        aws_access_key_id (None): The access key for a IAM user that has permissions to the bucket
        aws_secret_access_key (None): The secret key for a IAM user that has permissions to the bucket
        region_name (None): The region of the user/bucket
        storage_class (STANDARD): The storage class type name e.g. STANDARD, REDUCED_REDUDANCY
        TODO

    """

    SEPARATOR = '/'

    # Define regex for the object key
    _LINE_SEP = "/"
    _S3_OBJECT_KEY = re.compile(r"^[a-zA-Z0-9!_.*'()\- ]+(/[a-zA-Z0-9!_.*'()\- ]+)*$")
    _S3_DISALLOWED_CHARACTERS = "{}^%`]'`\"<>[~#|"


    class AmazonStorageClass(StorageClassInterface):
        STANDARD = 'STANDARD'
        REDUCED_REDUNDANCY = 'REDUCED_REDUNDANCY'
        STANDARD_IA = 'STANDARD_IA'
        ONEZONE_IA = 'ONEZONE_IA'
        INTELLIGENT_TIERING = 'INTELLIGENT_TIERING'
        GLACIER = 'GLACIER'
        DEEP_ARCHIVE = 'DEEP_ARCHIVE'
        OUTPOSTS = 'OUTPOSTS'

        @classmethod
        def toGeneric(cls, value) -> StorageClass:
            value = cls(value)
            if value is cls.STANDARD:
                return StorageClass.STANDARD
            elif value is cls.REDUCED_REDUNDANCY:
                return StorageClass.REDUCED_REDUNDANCY
            elif value in (cls.STANDARD_IA, cls.ONEZONE_IA):
                return StorageClass.INFREQUENT_ACCESS
            elif value is cls.INTELLIGENT_TIERING:
                return StorageClass.INTELLIGENT_TIERING
            elif value in (cls.GLACIER, cls.DEEP_ARCHIVE):
                return StorageClass.ARCHIVE
            elif value is cls.OUTPOSTS:
                return StorageClass.HIGH_PERFORMANCE
            else:
                raise NotImplementedError(f'{value} has no generic mapping - Select storage class explicitly')

        @classmethod
        def fromGeneric(cls, generic: Optional[StorageClass]) -> "Amazon.AmazonStorageClass":

            if generic is StorageClass.STANDARD:
                return cls.STANDARD
            elif generic is StorageClass.REDUCED_REDUNDANCY:
                return cls.REDUCED_REDUNDANCY
            elif generic is StorageClass.INFREQUENT_ACCESS:
                log.warning('Interpreting %s as %s - could have meant %s', generic, cls.STANDARD_IA, cls.ONEZONE_IA)
                return cls.STANDARD_IA
            else:
                raise NotImplementedError(f'{generic} cannot be mapped to specific storage class in S3 - Select storage class explicitly')

    @overload
    def __init__(self, bucket: str, *, storage_class: AmazonStorageClass = AmazonStorageClass.STANDARD):
        pass
    @overload
    def __init__(self, bucket: str, *, aws_session: boto3.Session, storage_class: AmazonStorageClass = AmazonStorageClass.STANDARD):
        pass
    @overload
    def __init__(
        self,
        bucket: str,
        *,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_session_token: str,
        storage_class: AmazonStorageClass = AmazonStorageClass.STANDARD
        ):
        pass
    @overload
    def __init__(self, bucket: str, *, profile_name: str, storage_class: AmazonStorageClass = AmazonStorageClass.STANDARD):
        pass
    def __init__(
        self,
        bucket: str,
        *,
        aws_session: Optional[boto3.Session] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        region_name: Optional[str] = None,
        profile_name: Optional[str] = None,
        storage_class: AmazonStorageClass = AmazonStorageClass.STANDARD
    ):

        self._bucketName = bucket
        self._storageClass = self.AmazonStorageClass.convert(storage_class)

        # Handle the credentials for this manager
        if aws_session is None:
            aws_session = boto3.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=region_name,
                profile_name=profile_name
            )

        credentials = aws_session.get_credentials()
        if credentials is None:
            raise exceptions.OperationNotPermitted(
                f'No credentials found to connect to s3 bucket [{bucket}] - either provide credentials or setup environment variables according to https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html'
            )

        self._config = {
            'manager': 'AWS',
            'bucket': self._bucketName,
            'aws_access_key': credentials.access_key,
            'aws_secret_key': credentials.secret_key,
            'aws_session_token': credentials.token,
            'region_name': aws_session.region_name,
            'profile_name': aws_session.profile_name,
            'storage_class': storage_class.value
        }

        # NOTE
        # The max pool connections default is 10 - so machines with a large number of threads may exceed this count and
        # experience a very slow connection plus urllib3 pool warnings.
        # Thread pools created by stow will be created with the system cpu count
        cpu_count = os.cpu_count()
        if cpu_count is not None:
            cpu_count *=2
        else:
            cpu_count = 10

        self._aws_session = aws_session
        self._s3 = self._aws_session.client(
            's3',
            config=botocore.config.Config(
                max_pool_connections=cpu_count,
            )
        )

        super().__init__()

    @property
    def storage_class(self) -> AmazonStorageClass:
        return self._storageClass
    @storage_class.setter
    def storage_class(self, value: Union[StorageClass, str]):
        self._storageClass = self.AmazonStorageClass.convert(value)

    _s3_max_keys = int(os.environ.get('STOW_AMAZON_MAX_KEYS', 1000))
    @property
    def s3_max_keys(self) -> int:
        return self._s3_max_keys
    @s3_max_keys.setter
    def s3_max_keys(self, value: int):
        self._s3_max_keys = value

    def __repr__(self):
        return f'<Manager(S3): {self._bucketName}>'

    def _abspath(self, managerPath: str) -> str:

        return urllib.parse.ParseResult(
                's3',
                self._bucketName,
                managerPath,
                '',
                '',
                ''
            ).geturl()

    def _managerPath(self, managerPath: str) -> str:
        """ Difference between AWS and manager path is the removal of a leading '/'. As such remove
        the first character
        """
        abspath = managerPath.replace("\\", "/").strip("/")
        if (
            abspath and
            # self._S3_OBJECT_KEY.match(abspath) is None
            any(c in self._S3_DISALLOWED_CHARACTERS for c in abspath)
            ):
            raise ValueError(
                f"Path '{managerPath}' cannot be converted into a s3 accepted key -"
                f" regex {self._S3_OBJECT_KEY.pattern}"
            )

        return abspath

    @staticmethod
    def _getMetadataFromHead(head_object_data):

        metadata = head_object_data.get('Metadata', {})
        metadata['ETag'] = head_object_data['ETag']

        return metadata

    def _identifyPath(self, managerPath: str) -> typing.Union[Artefact, None]:

        key = self._managerPath(managerPath)
        if not key:
            return Directory(self, '/')

        try:
            response = self._s3.head_object(
                Bucket=self._bucketName,
                Key=key
            )

            return File(
                self,
                '/' + key,
                modifiedTime=response['LastModified'],
                size=response['ContentLength'],
                metadata=self._getMetadataFromHead(response),
                content_type=response['ContentType'],
            )

        except ClientError as e:

            if "ResponseMetadata" in e.response:
                if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
                    # No file existed with the given key - check if directory

                    resp = self._s3.list_objects(
                        Bucket=self._bucketName,
                        Prefix=key and key+'/',
                        Delimiter='/',
                        MaxKeys=1
                    )
                    if "Contents" in resp or "CommonPrefixes" in resp:
                        return Directory(self, '/' + key)

                    return None

            raise

    def _exists(self, managerPath: str):
        return self._identifyPath(managerPath) is not None

    def _metadata(self, managerPath: str) -> typing.Dict[str, str]:
        key = self._managerPath(managerPath)

        try:
            response = self._s3.head_object(
                Bucket=self._bucketName,
                Key=key
            )

            return self._getMetadataFromHead(response)

        except ClientError as e:
            if "ResponseMetadata" in e.response:
                if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
                    raise exceptions.ArtefactNoLongerExists(
                        f'Failed to fetch metadata information for {key}'
                    )

            raise

    def _digest(self, file: File, algorithm: HashingAlgorithm):

        if algorithm is HashingAlgorithm.MD5:
            raise NotImplementedError('AWS does not support conventional MD5 - use the ETag comparison method or localise file to calculate MD5')

        else:

            attributes = self._s3.get_object_attributes(
                Bucket=self._bucketName,
                Key=self._managerPath(file.path),
                ObjectAttributes=['Checksum']
            )
            checksums = attributes['Checksum']

            log.debug(f'Checksums collected for {file.path}: {checksums}')

            try:
                if algorithm is HashingAlgorithm.CRC32:
                    value = checksums['ChecksumCRC32']
                elif algorithm is HashingAlgorithm.CRC32C:
                    value = checksums['ChecksumCRC32C']
                elif algorithm is HashingAlgorithm.SHA1:
                    value = checksums['ChecksumSHA1']
                elif algorithm is HashingAlgorithm.SHA256:
                    value = checksums['ChecksumSHA256']
                else:
                    raise NotImplementedError(f'Amazon does not provide {algorithm} hashing')
            except KeyError:
                raise KeyError(f'S3 file {algorithm} was not calculated - checksums available: {checksums}. Localise to fetch checksum or review s3 creation an ensure hashing is enabled')

            return base64.b64decode(value).hex()

    def _isLink(self, _: str):
        return False

    def _isMount(self, _: str):
        return False

    def _download_file(
        self,
        artefactPath: str,
        path: str,
        times: Tuple[float, float],
        callback,
        transfer,
        ):
        self._s3.download_file(self._bucketName, artefactPath, path, Callback=transfer)
        os.utime(path, times)
        callback.added(artefactPath)

    def _get(
        self,
        source: ArtefactType,
        destination: str,
        *,
        callback: AbstractCallback,
        worker_config: WorkerPoolConfig,
        modified_time: Optional[float],
        accessed_time: Optional[float]
        ):

        # Convert manager path to s3
        keyName = self._managerPath(source.path)

        # If the source object is a directory
        if isinstance(source, Directory):

            # Loop through all objects under that prefix and create them locally
            for results in self._list_objects(self._bucketName, keyName):
                callback.addTaskCount(len(results), isAdding=True)

                for metadata in results.files:

                    # Get the artefact manager path
                    artefactPath = metadata.key

                    # Get the objects relative path to the directory
                    relativePath = self.relpath(artefactPath, keyName)

                    # Create object absolute path locally
                    path = os.path.join(destination, relativePath)

                    # Download the artefact to that location
                    if artefactPath[-1] != '/':

                        # Ensure the directory for that object
                        os.makedirs(os.path.dirname(path), exist_ok=True)
                        times = (accessed_time or metadata.lastModified.timestamp(), modified_time or metadata.lastModified.timestamp())
                        transfer = callback.get_bytes_transfer(path, metadata.size)

                        worker_config.submit(
                            self._download_file,
                            artefactPath,
                            path,
                            times,
                            callback=callback,
                            transfer=transfer
                        )

                    else:
                        os.makedirs(path, exist_ok=True)

        else:

            transfer = callback.get_bytes_transfer(destination, source.size)
            self._download_file(
                keyName,
                destination,
                times = (accessed_time or source.accessedTime.timestamp(), modified_time or source.modifiedTime.timestamp()),
                callback=callback,
                transfer=transfer
            )
            callback.added(destination)

    def _getBytes(self, source: File, callback: AbstractCallback) -> bytes:

        # Get buffer to recieve bytes
        bytes_buffer = io.BytesIO()

        # Fetch the file bytes and write them to the buffer
        transfer = callback.get_bytes_transfer(source.path, source.size)
        self._s3.download_fileobj(
            Bucket=self._bucketName,
            Key=self._managerPath(source.path),
            Fileobj=bytes_buffer,
            Callback=transfer
        )
        callback.added(source.path)

        # Return the bytes stored in the buffer
        return bytes_buffer.getvalue()

    def _localise_put_file(
        self,
        source: File,
        destination: str,
        extra_args: typing.Dict[str, str],
        callback: AbstractCallback,
        content_type: Optional[str],
        storage_class: AmazonStorageClass
        ):

        with source.localise() as abspath:
            self._s3.upload_file(
                abspath,
                self._bucketName,
                self._managerPath(destination),
                ExtraArgs={
                    "StorageClass": storage_class.value,
                    "ContentType": (content_type or mimetypes.guess_type(destination)[0] or 'application/octet-stream'),
                    **extra_args
                },
                Callback=callback.get_bytes_transfer(destination, source.size)
            )

        callback.added(destination)

    def _put(
        self,
        source: ArtefactType,
        destination: str,
        *,
        callback: AbstractCallback,
        worker_config: WorkerPoolConfig,
        content_type: Optional[str],
        storage_class: Optional[StorageClass],
        metadata: Dict[str, str],
        **kwargs
        ):

        # Setup metadata about the objects being put
        amazon_storage_class = self.AmazonStorageClass.convert(storage_class or self.storage_class)
        extra_args = {}
        if metadata is not None:
            extra_args['Metadata'] = {str(k): str(v) for k, v in metadata.items()}

        if isinstance(source, Directory):

            directories = [source]

            while directories:
                directory = directories.pop(0)

                artefact = None
                for artefact in directory.iterls():
                    if isinstance(artefact, File):

                        file_destination = self.join(
                            destination,
                            source.relpath(artefact),
                            separator='/'
                        )

                        worker_config.submit(
                            self._localise_put_file,
                            artefact,
                            file_destination,
                            extra_args=extra_args,
                            callback=callback,
                            content_type=content_type,
                            storage_class=amazon_storage_class
                        )

                    else:
                        directories.append(artefact)

                if artefact is None:
                    # The directory was empty and therefore nothing was uploaded.

                    file_destination = self._managerPath(
                            self.join(
                            destination,
                            source.relpath(directory),
                            separator='/'
                        )
                    ) + '/'

                    worker_config.submit(
                        self._s3.put_object,
                        Body=b'',
                        Bucket=self._bucketName,
                        Key=file_destination,
                        # StorageClass=amazon_storage_class.value  # This is a dictionary file so I don't know if its needed
                    )

        else:

            self._localise_put_file(
                source,
                destination,
                extra_args=extra_args,
                callback=callback,
                content_type=content_type,
                storage_class=amazon_storage_class
            )

        return PartialArtefact(self, destination)

    def _putBytes(
        self,
        fileBytes: bytes,
        destination: str,
        *,
        callback: AbstractCallback,
        metadata: Optional[Dict[str, str]] = None,
        content_type: Optional[str],
        storage_class: Optional[StorageClass],
        **kwargs
        ):

        amazon_storage_class = self.AmazonStorageClass.convert(storage_class or self.storage_class)

        self._s3.upload_fileobj(
            io.BytesIO(fileBytes),
            self._bucketName,
            self._managerPath(destination),
            ExtraArgs={
                "StorageClass": amazon_storage_class.value,
                "ContentType": (content_type or mimetypes.guess_type(destination)[0] or 'application/octet-stream'),
                "Metadata": ({str(k): str(v) for k, v in metadata.items()} if metadata else {})
            },
            Callback=callback.get_bytes_transfer(destination, len(fileBytes))
        )
        callback.added(destination)

        return PartialArtefact(self, destination)

    def _list_objects(self, bucket: str, key: str, delimiter: str = "") -> Generator[Dir, None, None]:

        if key:
            key += '/'

        marker = ""
        while True:
            response = self._s3.list_objects(
                Bucket=bucket,
                Prefix=key,
                Marker=marker,
                Delimiter=delimiter,
                MaxKeys=self._s3_max_keys
            )

            page = Dir()

            for metadata in response.get('Contents', []):
                page.files.append(
                    FileStat(
                        key=metadata['Key'],
                        lastModified=metadata['LastModified'],
                        storageClass=metadata['StorageClass'],
                        size=metadata['Size']
                    )
                )

            for metadata in response.get('CommonPrefixes', []):
                page.directories.append(
                    DirectoryStat(
                        prefix=metadata['Prefix']
                    )
                )

            yield page

            if not response['IsTruncated']:
                break

            if 'NextMarker' in response:
                marker = response['NextMarker']

            else:
                marker = response['Contents'][-1]['Key']

    def _singleCP(self, *, Key: str, callback: AbstractCallback, **kwargs):
        self._s3.copy_object(Key=Key, **kwargs)
        callback.added(Key)


    def _cp(
        self,
        source: Artefact,
        destination: str,
        *,
        callback: AbstractCallback,
        metadata: Optional[Dict[str, str]],
        content_type: Optional[str],
        storage_class: Optional[StorageClassInterface],
        worker_config: WorkerPoolConfig,
        **kwargs
        ) -> Artefact:

        # Convert the paths to s3 paths
        sourceBucket, sourcePath = self.manager(source).root, self._managerPath(source.path)
        destinationPath = self._managerPath(destination)
        amazon_storage_class = self.AmazonStorageClass.convert(storage_class or self.storage_class)

        copy_args = {}
        if metadata: copy_args['Metadata'] = metadata

        if isinstance(source, Directory):

            for results in self._list_objects(sourceBucket, sourcePath):

                callback.addTaskCount(len(results), isAdding=True)

                for fileMetadata in results.files:

                    # Copy the object from the source object to the relative location in the destination location
                    relpath = self.relpath(fileMetadata.key, sourcePath, separator='/')
                    if fileMetadata.key[-1] == '/':
                        # Resolve the relpath method removing trailing separators
                        relpath += '/'

                    subDestination = self.join(destinationPath, relpath, separator='/')

                    worker_config.submit(
                        self._singleCP,
                        CopySource={
                            "Bucket": sourceBucket,
                            "Key": fileMetadata.key
                        },
                        Bucket=self._bucketName,
                        # The Relative path to the source joined onto the destination path
                        Key=subDestination,
                        ContentType=(content_type or mimetypes.guess_type(destination)[0] or 'application/octet-stream'),
                        StorageClass=amazon_storage_class.value,
                        callback=callback,
                        **copy_args
                    )

        else:
            # Copy the object from the source object to the relative location in the destination location
            worker_config.submit(
                self._singleCP,
                CopySource={
                    "Bucket": sourceBucket,
                    "Key": sourcePath
                },
                Bucket=self._bucketName,
                # The Relative path to the source joined onto the destination path
                Key=destinationPath,
                ContentType=(content_type or mimetypes.guess_type(destination)[0] or 'application/octet-stream'),
                StorageClass=amazon_storage_class.value,
                callback=callback,
                **copy_args
            )

        return PartialArtefact(self, destination)

    def _mv(
        self,
        source: Artefact,
        destination: str,
        *,
        callback: AbstractCallback,
        **kwargs
        ):

        # Copy the source objects to the destination
        copied_artefact = self._cp(
            source,
            destination,
            callback=callback,
            **kwargs
        )

        # Delete the source objects now it has been entirely copied
        source._manager._rm(source, callback=callback)

        return copied_artefact

    def _ls(self, directory: str, recursive: bool = False):

        for results in self._list_objects(self._bucketName, self._managerPath(directory), delimiter='/'):

            for metadata in results.files:

                if metadata.key.endswith('/'):
                    continue

                # NOTE - metadata is not added as we don't know it all yet - Better to have it added by head object
                yield File(
                    self,
                    '/' + metadata.key,
                    modifiedTime=metadata.lastModified,
                    size=metadata.size,
                    storage_class=self.AmazonStorageClass(metadata.storageClass)
                )

            for metadata in results.directories:
                path = '/' + metadata.prefix[:-1]
                yield Directory(self, path)
                if recursive:
                    yield from self._ls(path, recursive=recursive)

    def _rm(self, artefact: Artefact, *, callback: AbstractCallback):

        key = self._managerPath(artefact.path)

        if isinstance(artefact, Directory):

            # Iterate through the list objects - and enqueue them to be deleted
            keys = []
            for results in self._list_objects(self._bucketName, key):
                if not results.files:
                    return

                callback.addTaskCount(len(results.files), isAdding=False)

                # Call the delete on the items -
                # NOTE max number of items is 1000 to delete (which is the max of the list_objects endpoint also)
                response = self._s3.delete_objects(
                    Bucket=self._bucketName,
                    Delete={
                        "Objects": [{"Key": file_metadata.key} for file_metadata in results.files],
                        "Quiet": True
                    }
                )

                if 'Errors' in response:
                    errors = []
                    for errored in response['Errors']:
                        msg = f"{errored['Code']}: {errored['Message']} - failed to delete {errored['Key']}"
                        log.error(msg)
                        errors.append(msg)

                    raise RuntimeWarning(f'Failed to delete items in s3: {errors}')

                callback.removed(len(results))

        else:
            self._s3.delete_object(Bucket=self._bucketName, Key=key)
            callback.removed(key)

    @classmethod
    def _signatureFromURL(cls, url: urllib.parse.ParseResult):

        # Extract the query data passed into
        queryData = urllib.parse.parse_qs(url.query)

        signature = {
            "bucket": url.netloc,
            "aws_access_key_id": queryData.get("aws_access_key_id", [None])[0],
            "aws_secret_access_key": queryData.get("aws_secret_access_key", [None])[0],
            "aws_session_token": queryData.get("aws_session_token", [None])[0],
            "region_name": queryData.get("region_name", [None])[0],
            "profile_name": queryData.get("profile", [None])[0]
        }

        return signature, (url.path or '/')

    @property
    def root(self):
        return self._bucketName

    def toConfig(self):
        return self._config

    class CommandLineConfig:

        def __init__(self, manager):
            self._manager = manager

        @staticmethod
        def arguments() -> typing.List[typing.Tuple]:
            return [
                (('-b', '--bucket'), {'help': '[REQUIRED] The bucket name'}),
                (('-k', '--access-key'), {'help': 'AWS access key id'}),
                (('-s', '--secret-key'), {'help': 'AWS secret access key'}),
                (('-t', '--token'), {'help': 'AWS session token'}),
                (('-r', '--region-name'), {'help': 'Region name'}),
                (('-p', '--profile'), {'help': 'Select aws profile credentials'}),
            ]

        def initialise(self, kwargs):

            session = boto3.Session(
                aws_access_key_id=kwargs['access_key'],
                aws_secret_access_key=kwargs['secret_key'],
                aws_session_token=kwargs['token'],
                region_name=kwargs['region_name'],
                profile_name=kwargs['profile']
            )

            if not kwargs.get('bucket'):
                s3 = session.client('s3')
                response = s3.list_buckets()

                # Output the bucket names
                print('Bucket (-b, --bucket) is required - Existing buckets:')
                print()
                print('Name'.ljust(80)+' Creation Date')
                for bucket in response['Buckets']:
                    print(f"{bucket['Name'].ljust(80)} {bucket['CreationDate'].isoformat()}")

                exit(1)

            return self._manager(bucket=kwargs['bucket'], aws_session=session)
