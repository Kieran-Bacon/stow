""" The implementation of Amazon's S3 storage manager for stow """

# pyright: reportTypedDictNotRequiredAccess=false

import os
import re
import io
import typing
from typing import Generator, List, Union, Optional, Tuple, Type, Dict, overload, Literal
from typing_extensions import Self
import urllib.parse
import hashlib
import mimetypes
import logging
import base64
import dataclasses
import concurrent
import concurrent.futures

import boto3
import botocore.config
import boto3.exceptions
from botocore.exceptions import ClientError, UnauthorizedSSOTokenError

from .. import utils as utils
from ..worker_config import WorkerPoolConfig
from ..types import HashingAlgorithm
from ..artefacts import Artefact, PartialArtefact, File, Directory, ArtefactType, Metadata, ArtefactOrPathLike, ArtefactOrStr
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
class DirStat:
    bucket: Optional[str]
    path: str
    keys: List[str] = dataclasses.field(default_factory=list)
    files: List[File] = dataclasses.field(default_factory=list)
    directory_keys: List[str] = dataclasses.field(default_factory=list)
    directories: List[Directory] = dataclasses.field(default_factory=list)
    next_marker: Optional[str] = None

    def __len__(self) -> int:
        return len(self.files) + len(self.directories)

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
    def fromGeneric(cls, generic: Optional[StorageClass]) -> "AmazonStorageClass":

        if generic is StorageClass.STANDARD:
            return cls.STANDARD
        elif generic is StorageClass.REDUCED_REDUNDANCY:
            return cls.REDUCED_REDUNDANCY
        elif generic is StorageClass.INFREQUENT_ACCESS:
            log.warning('Interpreting %s as %s - could have meant %s', generic, cls.STANDARD_IA, cls.ONEZONE_IA)
            return cls.STANDARD_IA
        else:
            raise NotImplementedError(f'{generic} cannot be mapped to specific storage class in S3 - Select storage class explicitly')


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

    SAFE_FILE_OVERWRITE = True
    SAFE_DIRECTORY_OVERWRITE = False  # The directory can have lingering artefacts during - must remove files beforehand
    SEPARATOR = '/'

    # Define regex for the object key
    _LINE_SEP = "/"
    _S3_OBJECT_KEY = re.compile(r"^[a-zA-Z0-9!_.*'()\- ]+(/[a-zA-Z0-9!_.*'()\- ]+)*$")
    _S3_DISALLOWED_CHARACTERS = "{}^%`]'`\"<>[~#|"

    _s3_max_keys = int(os.environ.get('STOW_AMAZON_MAX_KEYS', 1000))

    @overload
    def __init__(
        self,
        path: str = ...,
        *,
        storage_class: AmazonStorageClass = ...
        ):
        ...
    @overload
    def __init__(
        self,
        path: str = ...,
        *,
        aws_session: boto3.Session,
        storage_class: AmazonStorageClass = ...
        ):
        ...
    @overload
    def __init__(
        self,
        path: str = ...,
        *,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_session_token: Optional[str] = ...,
        default_region: Optional[str] = ...,
        storage_class: AmazonStorageClass = ...
        ):
        ...
    @overload
    def __init__(
        self,
        path: str = ...,
        *,
        profile_name: str,
        storage_class: AmazonStorageClass = ...
        ):
        ...
    def __init__(
        self,
        path: str = '',
        *,
        aws_session: Optional[boto3.Session] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        default_region: Optional[str] = None,
        profile_name: Optional[str] = None,
        storage_class: AmazonStorageClass = AmazonStorageClass.STANDARD
    ):

        self._path = self._validatePath(path)
        self._storageClass = AmazonStorageClass.convert(storage_class)

        # Handle the credentials for this manager
        if aws_session is None:
            aws_session = boto3.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=default_region,
                profile_name=profile_name
            )

        credentials = aws_session.get_credentials()
        if credentials is None:
            raise exceptions.OperationNotPermitted(
                f'No credentials found - either provide credentials or setup environment variables according to https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html'
            )

        self._config = {
            'aws_access_key': credentials.access_key,
            'aws_secret_key': credentials.secret_key,
            'default_region': aws_session.region_name,
        }
        if self._path: self._config['path'] = self._path
        if aws_session.profile_name != 'default': self._config['profile_name'] = aws_session.profile_name
        if credentials.token: self._config['aws_session_token'] = credentials.token
        if self._storageClass is not AmazonStorageClass.STANDARD: self._config['storage_class'] = self._storageClass.value


        # NOTE
        # The max pool connections default is 10 - so machines with a large number of threads may exceed this count and
        # experience a very slow connection plus urllib3 pool warnings.
        # Thread pools created by stow will be created with the system cpu count
        self._aws_session = aws_session
        self._s3 = self._aws_session.client(
            's3',
            config=botocore.config.Config(
                max_pool_connections=(os.cpu_count() or 5)*2,
            )
        )

        super().__init__()

    def __repr__(self):
        if self._path:
            return f'<Manager(S3): {self._path}>'
        else:
            return f'<Manager(S3)>'

    def _ensureBucket(self, bucket: str) -> bool:
        """ Check whether a bucket is accessible - creating it if it doesn't exists and raising errors if a permission
        issues exists

        Returns:
            bool: True if the bucket was created - False if it exists and we have permissions to it
        """
        try:
            self._s3.head_bucket(Bucket=bucket)

        except ClientError as e:
            status_code = e.response['ResponseMetadata']['HTTPStatusCode']

            if status_code == 404:
                # We can create the bucket as it doesn't exist currently
                log.warning('Creating s3 bucket=[%s] as it does not already exist and is available')
                self._s3.create_bucket(Bucket=bucket)
                return True

            elif status_code == 403:
                # Bucket already exists and cannot be written too
                raise exceptions.OperationNotPermitted('You do not have permissions to access s3 bucket [%s]') from e

            raise
        return False

    def _abspath(self, managerPath: str) -> str:

        bucket, path = self._pathComponents(managerPath)

        # NOTE abspath doesn't return

        return urllib.parse.ParseResult(
                's3',
                bucket,
                path,
                '',# urllib.parse.urlencode(self._config),
                '',
                ''
            ).geturl()

    def _managerPath(self, bucket: str, key: str) -> str:
        return '/' + ('/'.join((bucket, key))).removeprefix(self._path).lstrip('/')

    def _validatePath(self, managerPath: str) -> str:
        """ Difference between AWS and manager path is the removal of a leading '/'. As such remove
        the first character
        """
        abspath = managerPath.replace("\\", "/").lstrip("/")
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

    @overload
    def _pathComponents(self, artefact: File) -> Tuple[str, str]:
        ...
    @overload
    def _pathComponents(self, artefact: ArtefactOrStr, optional: Literal[False]) -> Tuple[str, str]:
        ...
    @overload
    def _pathComponents(self, artefact: ArtefactOrStr, optional: Literal[True] = True) -> Tuple[Optional[str], str]:
        ...
    def _pathComponents(self, artefact: ArtefactOrStr, optional: bool = True) -> Tuple[Optional[str], str]:

        artefactPath = artefact.path if isinstance(artefact, Artefact) else artefact

        path = self.join(self._path, self._validatePath(artefactPath), joinAbsolutes=True).lstrip('/')

        if not path:
            return None, ''

        components = path.split('/')

        if len(components) == 1:
            return path, ''
        else:
            return components[0], '/'.join(components[1:])


    @staticmethod
    def _getMetadataFromHead(head_object_data):

        metadata = head_object_data.get('Metadata', {})
        metadata['ETag'] = head_object_data['ETag']

        return metadata

    @overload
    def _identifyPath(self, managerPath: str, *, type: Type[File]) -> File:
        ...
    @overload
    def _identifyPath(self, managerPath: str, *, type: Optional[Type[ArtefactType]] = None) -> typing.Union[Artefact, None]:
        ...
    def _identifyPath(self, managerPath: str, *, type: Optional[Type[ArtefactType]] = None) -> typing.Union[Artefact, None]:

        bucket, key = self._pathComponents(managerPath)

        if bucket is None:
            # The identify directory
            return Directory(self, '/')

        elif not key:
            # The bucket directory
            try:
                bucketHead = self._s3.head_bucket(Bucket=bucket)
                return Directory(self, '/' + bucket)
            except:
                return None

        else:

            try:
                response = self._s3.head_object(
                    Bucket=bucket,
                    Key=key
                )

                return File(
                    self,
                    self._managerPath(bucket, key),
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
                            Bucket=bucket,
                            Prefix=key and key+'/',
                            Delimiter='/',
                            MaxKeys=1
                        )
                        if "Contents" in resp or "CommonPrefixes" in resp:
                            return Directory(self, self._managerPath(bucket, key))

                        return None

                raise

    def _exists(self, managerPath: str):
        return self._identifyPath(managerPath) is not None

    def _metadata(self, managerPath: str) -> typing.Dict[str, str]:
        bucket, key = self._pathComponents(managerPath)

        if bucket is None or key is None:
            return {}

        else:
            try:
                response = self._s3.head_object(
                    Bucket=bucket,
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

            bucket, key = self._pathComponents(file)

            attributes = self._s3.get_object_attributes(
                Bucket=bucket,
                Key=key,
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

    def _list_objects(
            self,
            bucket: Optional[str],
            key: str,
            delimiter: Literal['/', ''] = "",
            include_metadata: bool = False,
            paginate: bool = True,
            marker: str = "",
            include_directory_placeholder: bool = True
        ) -> Generator[DirStat, None, None]:

        if bucket is None:
            if delimiter:
                yield DirStat(
                    bucket=bucket,
                    path=key,
                    directories=[Directory(self, bucket['Name'], modifiedTime=bucket['CreationDate']) for bucket in self._s3.list_buckets()['Buckets']]
                )
            else:
                for bucket_name in self._s3.list_buckets():
                    yield from self._list_objects(bucket_name, key, delimiter=delimiter)

        else:

            if key:
                key += '/'

            while True:
                response = self._s3.list_objects(
                    Bucket=bucket,
                    Prefix=key,
                    Marker=marker,
                    Delimiter=delimiter,
                    MaxKeys=self._s3_max_keys
                )

                keys, files = [], []
                for objectdata in response.get('Contents', []):
                    objectKey = objectdata['Key']

                    if not include_directory_placeholder and objectKey[-1] == '/':
                        continue

                    keys.append(objectKey)

                    if include_metadata:
                        # Head the object to pull all the metadata
                        objectdata = self._s3.head_object(
                            Bucket=bucket,
                            Key=objectdata['Key']
                        )

                        files.append(
                            File(
                                self,
                                self._managerPath(bucket, objectKey),
                                modifiedTime=objectdata['LastModified'],
                                size=objectdata['ContentLength'],
                                metadata=self._getMetadataFromHead(objectdata),
                                content_type=objectdata['ContentType'],
                                storage_class=AmazonStorageClass(objectdata['StorageClass'])
                            )
                        )

                    else:
                        files.append(
                            File(
                                self,
                                self._managerPath(bucket, objectKey),
                                modifiedTime=objectdata['LastModified'],
                                size=objectdata['Size'],
                                storage_class=AmazonStorageClass(objectdata['StorageClass'])
                            )
                        )

                directory_keys, directories = [], []
                for metadata in response.get('CommonPrefixes', []):
                    directoryKey = metadata['Prefix'][:-1]

                    directory_keys.append(directoryKey)

                    directories.append(
                        Directory(
                                self,
                                self._managerPath(bucket, directoryKey)
                        )
                    )


                nextMarker = response.get('NextMarker')

                stat = DirStat(
                    bucket=bucket,
                    path=key,
                    keys=keys,
                    files=files,
                    directory_keys=directory_keys,
                    directories=directories,
                    next_marker=nextMarker
                )

                yield stat

                if paginate and nextMarker:
                    marker = response['NextMarker']
                else:
                    break

    def _download_file(
            self,
            s3File: File,
            destination: str,
            modified_time,
            accessed_time,
            callback
        ):

        transfer = callback.get_bytes_transfer(s3File.path, s3File.size)
        self._s3.download_file(*self._pathComponents(s3File), destination, Callback=transfer)
        utils.utime(destination, modified_time=modified_time, accessed_time=accessed_time)
        callback.written(destination)

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

        bucket, key = self._pathComponents(source)

        if bucket is None:

            worker_config = worker_config.detach()

            for bucket in self._s3.list_buckets()['Buckets']:
                worker_config.submit(
                    self._get,
                    self.join(source, bucket['Name']),
                    self.join(destination, bucket['Name']),
                    callback=callback,
                    modified_time=modified_time,
                    accessed_time=accessed_time,
                    worker_config=worker_config
                )

            worker_config.conclude()

        else:

            # If the source object is a directory
            if isinstance(source, Directory):

                # Loop through all objects under that prefix and create them locally
                for results in self._list_objects(bucket, key):
                    callback.writing(len(results))

                    for s3File in results.files:

                        # Create object absolute path locally
                        destinationPath = os.path.join(destination, source.relpath(s3File, separator='/'))

                        # Download the artefact to that location
                        if s3File.path[-1] != '/':

                            # Ensure the directory for that object
                            os.makedirs(os.path.dirname(destinationPath), exist_ok=True)

                            worker_config.submit(
                                self._download_file,
                                s3File,
                                destinationPath,
                                modified_time=modified_time or s3File.modifiedTime,
                                accessed_time=accessed_time or s3File.accessedTime,
                                callback=callback,
                            )

                        else:
                            os.makedirs(destinationPath, exist_ok=True)

            else:

                callback.writing(1)

                worker_config.submit(
                    self._download_file,
                    source,
                    destination,
                    modified_time=modified_time or source.modifiedTime.timestamp(),
                    accessed_time=accessed_time or source.accessedTime.timestamp(),
                    callback=callback,
                )

    def _getBytes(self, source: File, callback: AbstractCallback) -> bytes:

        bucket, key = self._pathComponents(source)

        # Get buffer to recieve bytes
        bytes_buffer = io.BytesIO()
        callback.writing(1)

        # Fetch the file bytes and write them to the buffer
        self._s3.download_fileobj(
            Bucket=bucket,
            Key=key,
            Fileobj=bytes_buffer,
            Callback=callback.get_bytes_transfer(source.path, source.size)
        )
        callback.written(source.path)

        # Return the bytes stored in the buffer
        return bytes_buffer.getvalue()

    def _put_object(self, callback: AbstractCallback, **kwargs):

        self._s3.put_object(
            **kwargs
        )

        callback.written(1)

    def _localise_put_file(
        self,
        source: File,
        bucket: str,
        key: str,
        extra_args: typing.Dict[str, str],
        callback: AbstractCallback,
        content_type: Optional[str],
        storage_class: AmazonStorageClass
        ):

        with source.localise() as abspath:
            try:
                self._s3.upload_file(
                    abspath,
                    bucket,
                    key,
                    ExtraArgs={
                        "StorageClass": storage_class.value,
                        "ContentType": (content_type or mimetypes.guess_type(key)[0] or 'application/octet-stream'),
                        **extra_args
                    },
                    Callback=callback.get_bytes_transfer(key, source.size)
                )

            except boto3.exceptions.S3UploadFailedError as e:

                if self._ensureBucket(bucket):
                    # The bucket was not present but it has now been created - we should be able to put item now

                    self._s3.upload_file(
                        abspath,
                        bucket,
                        key,
                        ExtraArgs={
                            "StorageClass": storage_class.value,
                            "ContentType": (content_type or mimetypes.guess_type(key)[0] or 'application/octet-stream'),
                            **extra_args
                        },
                        Callback=callback.get_bytes_transfer(key, source.size)
                    )

                else:
                    raise

        callback.written(key)

    def _put(
        self,
        source: ArtefactType,
        destination: str,
        *,
        callback: AbstractCallback,
        worker_config: WorkerPoolConfig,
        content_type: Optional[str],
        storage_class: Optional[StorageClass],
        metadata: Optional[Metadata] = None,
        **kwargs
        ):

        bucket, key = self._pathComponents(destination)

        if bucket is None:
            if isinstance(source, File):
                raise exceptions.OperationNotPermitted(
                    f'Attempting to overwrite... s3 [{destination}]. With a... file [{source}]'
                )

            for artefact in source.iterls():
                if isinstance(artefact, Directory):
                    self._ensureBucket(artefact.basename)

                    # Put the contents of the directory into the bucket
                    self._put(
                        artefact,
                        '/' + artefact.basename,
                        callback=callback,
                        worker_config=worker_config,
                        content_type=content_type,
                        metadata=metadata,
                        storage_class=storage_class
                    )

                else:
                    raise exceptions.OperationNotPermitted(
                        f'S3 bucket level cannot store files - {destination} is invalid for source {source}'
                    )

        else:
            callback.writing(1)

            # Setup metadata about the objects being put
            amazon_storage_class = AmazonStorageClass.convert(storage_class or self.storage_class)
            extra_args = {}

            if isinstance(source, Directory):

                directories = [source]

                while directories:
                    directory = directories.pop(0)

                    artefact = None
                    for artefact in directory.iterls():
                        callback.writing(1)

                        if isinstance(artefact, File):

                            file_destination = self.join(
                                key,
                                source.relpath(artefact, separator='/'),
                                separator='/'
                            )

                            if metadata is not None:
                                extra_args['Metadata'] = self._freezeMetadata(metadata, artefact)

                            worker_config.submit(
                                self._localise_put_file,
                                artefact,
                                bucket,
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

                        s3Bucket, s3DirectoryFilepath = self._pathComponents(
                            self.join(destination, source.relpath(directory, separator='/'), separator='/')
                        )

                        worker_config.submit(
                            self._put_object,
                            Body=b'',
                            Bucket=s3Bucket,
                            Key=s3DirectoryFilepath + '/',
                            callback=callback
                            # StorageClass=amazon_storage_class.value  # This is a dictionary file so I don't know if its needed
                        )

            else:

                if metadata is not None:
                    extra_args['Metadata'] = self._freezeMetadata(metadata, source)

                worker_config.submit(
                    self._localise_put_file,
                    source,
                    bucket,
                    key,
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
        metadata: Optional[Metadata] = None,
        content_type: Optional[str],
        storage_class: Optional[StorageClass],
        **kwargs
        ):
        callback.writing(1)

        bucket, key = self._pathComponents(destination)

        if bucket is None:
            raise exceptions.OperationNotPermitted('Cannot overwrite s3... with bytes')

        elif not key:
            raise exceptions.OperationNotPermitted('Cannot write bytes to overwrite s3 bucket [{destination}]')

        else:

            amazon_storage_class = AmazonStorageClass.convert(storage_class or self.storage_class)

            self._s3.upload_fileobj(
                io.BytesIO(fileBytes),
                bucket,
                key,
                ExtraArgs={
                    "StorageClass": amazon_storage_class.value,
                    "ContentType": (content_type or mimetypes.guess_type(destination)[0] or 'application/octet-stream'),
                    "Metadata": ({str(k): str(v) for k, v in metadata.items()} if metadata else {})
                },
                Callback=callback.get_bytes_transfer(destination, len(fileBytes))
            )
            callback.written(destination)

            return PartialArtefact(self, destination)

    def _ls(
        self,
        directory: str,
        recursive: bool = False,
        *,
        include_metadata: bool = False,
        worker_config: WorkerPoolConfig
        ) -> Generator[ArtefactType, None, None]:

        bucket, key = self._pathComponents(directory)

        if not recursive:
            for dir_stat in self._list_objects(bucket, key, '/', include_metadata=include_metadata, include_directory_placeholder=False):
                yield from dir_stat.directories
                yield from dir_stat.files
            return

        else:
            # Crack this nut with my sledge

            # Get a subsection of the worker_pool to parallelise the fetching of artefacts
            worker_config = worker_config.detach()

            # Submit the initial request to list the top level directory
            worker_config.submit(
                lambda *args,**kwargs: next(self._list_objects(*args, **kwargs)),
                bucket,
                key,
                '/',
                include_metadata=include_metadata,
                paginate=True,
                include_directory_placeholder=False
            )

            # Begin the loop of work of tasks fetching deeper and deeper layers of the filesystem
            while worker_config.futures:

                # Dequeue tasks we are going to wait to complete, reset the source for the next level of results
                enqueued, worker_config.futures = worker_config.futures, []

                # For the completed work
                for future in concurrent.futures.as_completed(enqueued):
                    try:
                        dir_stat: DirStat = future.result()
                    except StopIteration:
                        continue

                    # Iterate over the response and enqueue the next layer of recursion
                    for directoryKey, directoryObj in zip(dir_stat.directory_keys, dir_stat.directories):

                        worker_config.submit(
                            lambda *args,**kwargs: next(self._list_objects(*args, **kwargs)),
                            bucket,
                            directoryKey,
                            '/',
                            include_metadata=include_metadata,
                            paginate=True,
                            include_directory_placeholder=False
                        )
                        yield directoryObj

                    # Yield the currently collected information
                    yield from dir_stat.files

                    if dir_stat.next_marker:
                        worker_config.submit(
                            lambda *args,**kwargs: next(self._list_objects(*args, **kwargs)),
                            bucket,
                            dir_stat.path,
                            '/',
                            include_metadata=include_metadata,
                            pagination=True,
                            marker=dir_stat.next_marker,
                            include_directory_placeholder=False
                        )

    def _copy_object(
            self,
            *,
            Key: str,
            callback: AbstractCallback,
            delete: Optional[Tuple[str, str]] = None,
            **kwargs
        ):
        self._s3.copy_object(Key=Key, **kwargs)
        callback.written(Key)

        if delete is not None:
            sourceBucket, key = delete
            self._s3.delete_object(Bucket=sourceBucket, Key=key)

    def _cp(
        self,
        source: ArtefactType,
        destination: str,
        *,
        move: bool = False,
        callback: AbstractCallback,
        metadata: Optional[Metadata] = None,
        content_type: Optional[str],
        storage_class: Optional[StorageClassInterface],
        worker_config: WorkerPoolConfig,
        **kwargs
    ) -> ArtefactType:

        # Get the source manager - this should be s3 anyway
        sourceManager = self.manager(source)
        sourceBucket, sourceKey = sourceManager._pathComponents(source.path)

        # Parse the destination location
        destinationBucket, destinationKey = self._pathComponents(destination)

        if (sourceBucket is None and destinationBucket is None):
            operation = 'move' if move else 'copy'
            raise exceptions.OperationNotPermitted(f'Operations to {operation} all of s3 onto itself is not permitted')

        elif sourceBucket is None:
            # Backing up all buckets into a directory of s3
            for bucket in sourceManager._s3.list_buckets()['Buckets']:
                self._cp(
                    Directory(sourceManager, '/' + bucket['Name']),
                    '/'.join((destination, bucket['Name'])),
                    move=False,
                    callback=callback,
                    metadata=metadata,
                    content_type=content_type,
                    storage_class=storage_class,
                    worker_config=worker_config,
                )

        elif destinationBucket is None:
            # Bringing an artefact up a level
            if isinstance(source, File):
                raise exceptions.OperationNotPermitted(r'Cannot coppy {source} onto S3')

            else:
                for artefact in source.iterls():
                    if isinstance(artefact, Directory):
                        self._cp(
                            artefact,
                            '/' + artefact.basename,
                            move=False,
                            callback=callback,
                            metadata=metadata,
                            content_type=content_type,
                            storage_class=storage_class,
                            worker_config=worker_config,
                        )

                    else:
                        raise exceptions.OperationNotPermitted(
                            f'S3 does not support storing files at the bucket level - {artefact} cannot exist at s3://{artefact.basename}'
                        )

        else:

            # Setup defaults
            amazon_storage_class = AmazonStorageClass.convert(storage_class or self.storage_class)
            copy_args = {}

            if isinstance(source, Directory):

                for result in sourceManager._list_objects(sourceBucket, sourceKey):
                    callback.writing(len(result))

                    for sourceSubKey, sourceSubFile in zip(result.keys, result.files):

                        if metadata is not None:
                            copy_args['Metadata'] = self._freezeMetadata(metadata, sourceSubFile)

                        # Copy the object from the source object to the relative location in the destination location
                        relpath = self.relpath(sourceSubKey, sourceKey, separator='/')
                        if sourceSubKey[-1] == '/':
                            relpath += '/'

                        # Join the relative path to the destination location
                        subDestinationKey = self.join(destinationKey, relpath, separator='/')

                        worker_config.submit(
                            self._copy_object,
                            CopySource={
                                "Bucket": sourceBucket,
                                "Key": sourceSubKey
                            },
                            Bucket=destinationBucket,
                            Key=subDestinationKey,
                            ContentType=(content_type or mimetypes.guess_type(destination)[0] or 'application/octet-stream'),
                            StorageClass=amazon_storage_class.value,
                            callback=callback,
                            delete=(sourceBucket, sourceSubKey) if move else None,
                            **copy_args
                        )

            else:

                if metadata is not None:
                    copy_args['Metadata'] = self._freezeMetadata(metadata, source)

                # Copy the object from the source object to the relative location in the destination location
                worker_config.submit(
                    self._copy_object,
                    CopySource={
                        "Bucket": sourceBucket,
                        "Key": sourceKey
                    },
                    Bucket=destinationBucket,
                    # The Relative path to the source joined onto the destination path
                    Key=destinationKey,
                    ContentType=(content_type or mimetypes.guess_type(destination)[0] or 'application/octet-stream'),
                    StorageClass=amazon_storage_class.value,
                    callback=callback,
                    delete=(sourceBucket, sourceKey) if move else None,
                    **copy_args
                )

        return PartialArtefact(self, destination)

    def _mv(
        self,
        source: ArtefactType,
        destination: str,
        **kwargs
        ):

        # Copy the source objects to the destination
        copied_artefact = self._cp(
            source,
            destination,
            move=True,
            **kwargs
        )

        return copied_artefact

    def _rm(self, *artefacts: str, callback: AbstractCallback, **kwargs):
        callback.deleting(len(artefacts))

        for artefact in artefacts:

            bucket, key = self._pathComponents(artefact)

            if bucket is None:
                raise exceptions.OperationNotPermitted("Attempt to made to delete... s3. Operation is not allowed")

            if not key:
                # The bucket is also to be deleted
                self._s3.delete_bucket(Bucket=bucket)

            else:

                try:
                    # We are deleting a key directly
                    self._s3.head_object(Bucket=bucket, Key=key)
                    self._s3.delete_object(
                        Bucket=bucket,
                        Key=key
                    )
                    callback.deleted(artefact)
                except:

                    # The afteract is a directory
                    for stat in self._list_objects(bucket, key):

                        callback.deleting(len(stat.keys))

                        response = self._s3.delete_objects(
                            Bucket=bucket,
                            Delete={
                                "Objects": [{"Key": key} for key in stat.keys],
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

                        callback.deleted(len(stat.keys))

    @classmethod
    def _signatureFromURL(cls, url: urllib.parse.ParseResult):

        # Extract the query data passed into
        queryData = urllib.parse.parse_qs(url.query)

        signature = {
            k:v
            for k, v in {
                "aws_access_key_id": queryData.get("aws_access_key_id", [None])[0],
                "aws_secret_access_key": queryData.get("aws_secret_access_key", [None])[0],
                "aws_session_token": queryData.get("aws_session_token", [None])[0],
                "default_region": queryData.get("default_region", [None])[0],
                "profile_name": queryData.get("profile", [None])[0],
                "storage_class": queryData.get("storage_class", [None])[0]
            }.items()
            if v is not None
        }


        return signature, (url.netloc + url.path or '/')

    @property
    def protocol(self):
        return 's3'

    @property
    def root(self):
        return '/' + self._path

    @property
    def storage_class(self) -> AmazonStorageClass:
        return self._storageClass
    @storage_class.setter
    def storage_class(self, value: Union[StorageClass, str]):
        self._storageClass = AmazonStorageClass.convert(value)

    @property
    def s3_max_keys(self) -> int:
        return self._s3_max_keys
    @s3_max_keys.setter
    def s3_max_keys(self, value: int):
        self._s3_max_keys = value

    @property
    def config(self):
        return self._config

    class CommandLineConfig:

        def __init__(self, manager: Type["Amazon"]):
            self._manager = manager

        @staticmethod
        def arguments() -> typing.List[typing.Tuple]:
            return [
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

            return self._manager(aws_session=session)
