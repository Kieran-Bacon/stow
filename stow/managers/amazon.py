""" The implementation of Amazon's S3 storage manager for stow """

import os
import re
import io
import typing
from typing import Union, Optional, Tuple
import urllib.parse
import hashlib
import enum
import mimetypes
import datetime
import threading
import concurrent.futures as futures
import logging
import base64
import functools

from tqdm import tqdm
import boto3
from botocore.exceptions import ClientError

from ..artefacts import Artefact, PartialArtefact, File, Directory, HashingAlgorithm
from ..manager import RemoteManager
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

    return digests[0] == digests[1]

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
    _S3_MAX_KEYS = os.environ.get('STOW_AMAZON_MAX_KEYS', 100)

    class StorageClass(enum.Enum):
        STANDARD = 'STANDARD'
        REDUCED_REDUNDANCY = 'REDUCED_REDUNDANCY'
        STANDARD_IA = 'STANDARD_IA'
        ONEZONE_IA = 'ONEZONE_IA'
        INTELLIGENT_TIERING = 'INTELLIGENT_TIERING'
        GLACIER = 'GLACIER'
        DEEP_ARCHIVE = 'DEEP_ARCHIVE'
        OUTPOSTS = 'OUTPOSTS'

    def __init__(
        self,
        bucket: str,
        *,
        aws_session: boto3.Session = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        aws_session_token: str = None,
        region_name: str = None,
        profile_name: str = None,
        storage_class: str = None,
        max_keys: int = None
    ):

        self._bucketName = bucket

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

        self._config = {
            'manager': 'AWS',
            'bucket': self._bucketName,
            'aws_access_key': credentials.access_key,
            'aws_secret_key': credentials.secret_key,
            'aws_session_token': credentials.token,
            'region_name': aws_session.region_name,
            'profile_name': aws_session.profile_name,
        }

        self._aws_session = aws_session
        self._s3 = self._aws_session.client('s3')

        if storage_class:
            self._storageClass = self.StorageClass(storage_class)
            self._config['storage_class'] = storage_class

        else:
            self._storageClass = self.StorageClass.STANDARD

        if max_keys is not None:
            self._S3_MAX_KEYS = max_keys

        super().__init__()

    def __repr__(self):
        return f'<Manager(S3): {self._bucketName}>'

    def _abspath(self, managerPath: str) -> str:

        return urllib.parse.ParseResult(
                's3',
                self._bucketName,
                managerPath,
                {},
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
        source: Artefact,
        destination: str,
        *,
        callback = None,
        modified_time: float = None,
        accessed_time: float = None
        ):

        # Convert manager path to s3
        keyName = self._managerPath(source.path)

        # If the source object is a directory
        if isinstance(source, Directory):

            with futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:

                future_collection = []

                # Loop through all objects under that prefix and create them locally
                for results in self._list_objects(self._bucketName, keyName):
                    callback.addTaskCount(len(results), isAdding=True)

                    for metadata, _ in results:

                        # Get the artefact manager path
                        artefactPath = metadata['Key']

                        # Get the objects relative path to the directory
                        relativePath = self.relpath(artefactPath, keyName)

                        # Create object absolute path locally
                        path = os.path.join(destination, relativePath)

                        # Download the artefact to that location
                        if artefactPath[-1] != '/':

                            # Ensure the directory for that object
                            os.makedirs(os.path.dirname(path), exist_ok=True)
                            times = (accessed_time or metadata['LastModified'].timestamp(), modified_time or metadata['LastModified'].timestamp())
                            transfer = callback.get_bytes_transfer(path, metadata['Size'])

                            future = executor.submit(
                                # self._s3.download_file,
                                self._download_file,
                                artefactPath,
                                path,
                                times,
                                callback=callback,
                                transfer=transfer
                            )
                            future_collection.append(future)

                        else:
                            os.makedirs(path, exist_ok=True)

                for future in futures.as_completed(future_collection):
                    exception = future.exception()
                    if exception:
                        raise exception

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

    def _getBytes(self, source: Artefact, callback = None) -> bytes:

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
        extra_args: typing.Dict,
        callback = None
        ):

        with source.localise() as abspath:
            self._s3.upload_file(
                abspath,
                self._bucketName,
                self._managerPath(destination),
                ExtraArgs={
                    "StorageClass": self._storageClass.value,
                    "ContentType": (mimetypes.guess_type(destination)[0] or 'application/octet-stream'),
                    **extra_args
                },
                Callback=callback.get_bytes_transfer(destination, source.size)
            )

        callback.added(destination)

    def _put(self, source: Artefact, destination: str, *, metadata = None, callback = None, **kwargs):

        # Setup metadata about the objects being put
        extra_args = {}
        if metadata is not None:
            extra_args['Metadata'] = {str(k): str(v) for k, v in metadata.items()}

        if isinstance(source, Directory):

            # Create a thread pool to upload multiple files in parallel
            with futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                future_collection = []

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

                            future_collection.append(
                                executor.submit(
                                    self._localise_put_file,
                                    artefact,
                                    file_destination,
                                    extra_args=extra_args,
                                    callback=callback
                                )
                            )

                        else:
                            directories.append(artefact)

                    if artefact is None:

                        file_destination = self.join(
                            destination,
                            source.relpath(directory),
                            separator='/'
                        ) + '/'


                        future_collection.append(
                            executor.submit(
                                self._s3.put_object,
                                Body=b'',
                                Bucket=self._bucketName,
                                Key=self._managerPath(file_destination)
                            )
                        )

                for future in futures.as_completed(future_collection):
                    exception = future.exception()
                    if exception is not None:
                        raise exception

        else:

            self._localise_put_file(
                source,
                destination,
                extra_args=extra_args,
                callback=callback
            )

        return PartialArtefact(self, destination)

    def _putBytes(
        self,
        fileBytes: bytes,
        destination: str,
        *,
        callback,
        metadata: typing.Dict[str, str] = None,
        **kwargs
        ):


        self._s3.upload_fileobj(
            io.BytesIO(fileBytes),
            self._bucketName,
            self._managerPath(destination),
            ExtraArgs={
                "StorageClass": self._storageClass.value,
                "ContentType": (mimetypes.guess_type(destination)[0] or 'application/octet-stream'),
                "Metadata": ({str(k): str(v) for k, v in metadata.items()} if metadata else {})
            },
            Callback=callback.get_bytes_transfer(destination, len(fileBytes))
        )
        callback.added(destination)

        return PartialArtefact(self, destination)

    def _list_objects(self, bucket: str, key: str, delimiter: str = "") -> typing.List[typing.Tuple[typing.Dict, bool]]:

        if key:
            key += '/'

        marker = ""
        while True:
            response = self._s3.list_objects(
                Bucket=bucket,
                Prefix=key,
                Marker=marker,
                Delimiter=delimiter,
                MaxKeys=self._S3_MAX_KEYS
            )

            results = [(metadata, True) for metadata in response.get('Contents', [])]
            results.extend([(prefix, False) for prefix in response.get('CommonPrefixes', [])])

            yield results

            if not response['IsTruncated']:
                break

            if 'NextMarker' in response:
                marker = response['NextMarker']

            else:
                marker = response['Contents'][-1]['Key']


    def _cp(self, source: Artefact, destination: str, *, callback) -> Artefact:

        # Convert the paths to s3 paths
        sourceBucket, sourcePath = self.manager(source).root, self._managerPath(source.path)
        destinationPath = self._managerPath(destination)

        if isinstance(source, Directory):

            for results in self._list_objects(sourceBucket, sourcePath):

                callback.addTaskCount(len(results), isAdding=True)

                for fileMetadata, _ in results:

                    # Copy the object from the source object to the relative location in the destination location
                    relpath = self.relpath(fileMetadata['Key'], sourcePath, separator='/')
                    if fileMetadata['Key'][-1] == '/':
                        # Resolve the relpath method removing trailing separators
                        relpath += '/'

                    subDestination = self.join(destinationPath, relpath, separator='/')

                    self._s3.copy_object(
                        CopySource={
                            "Bucket": sourceBucket,
                            "Key": fileMetadata['Key']
                        },
                        Bucket=self._bucketName,
                        # The Relative path to the source joined onto the destination path
                        Key=subDestination
                    )
                    callback.added(subDestination)

        else:
            # Copy the object from the source object to the relative location in the destination location
            self._s3.copy_object(
                CopySource={
                    "Bucket": sourceBucket,
                    "Key": sourcePath
                },
                Bucket=self._bucketName,
                # The Relative path to the source joined onto the destination path
                Key=destinationPath
            )
            callback.added(destinationPath)

        return PartialArtefact(self, destination)

    def _mv(self, source: Artefact, destination: str, *, callback):

        # Copy the source objects to the destination
        copied_artefact = self._cp(source, destination, callback=callback)

        # Delete the source objects now it has been entirely copied
        source.manager._rm(source, callback=callback)

        return copied_artefact

    def _ls(self, directory: str, recursive: bool = False):

        for results in self._list_objects(self._bucketName, self._managerPath(directory), delimiter='/'):

            for metadata, is_file in results:

                if is_file:
                    if metadata['Key'].endswith('/'):
                        continue

                    yield File(
                        self,
                        '/' + metadata['Key'],
                        modifiedTime=metadata['LastModified'],
                        size=metadata['Size']
                    )

                else:
                    path = '/' + metadata['Prefix'][:-1]
                    yield Directory(self, '/' + metadata['Prefix'][:-1])
                    if recursive:
                        yield from self._ls(path, recursive=recursive)

    def _rm(self, artefact: Artefact, *, callback):

        key = self._managerPath(artefact.path)

        if isinstance(artefact, Directory):

            # Iterate through the list objects - and enqueue them to be deleted
            keys = []
            for results in self._list_objects(self._bucketName, key):
                callback.addTaskCount(len(results), isAdding=False)
                for file_metadata, _ in results:
                    keys.append({"Key": file_metadata['Key']})

        else:
            keys = [{"Key": key}]

        # Delete the enqueued objects
        response = self._s3.delete_objects(
            Bucket=self._bucketName,
            Delete={
                "Objects": keys,
                "Quiet": True
            }
        )

        for deleted in response['Deleted']:
            callback.removed(deleted['Key'])

        if 'Errors' in response:
            errors = []
            for errored in response['Errors']:
                msg = f"{errored['code']}: {errored['message']} - failed to delete {errored['Key']}"
                log.error(msg)
                errors.append(msg)

            raise RuntimeWarning(f'Failed to delete items in s3: {errors}')

    @classmethod
    def _signatureFromURL(cls, url: urllib.parse.ParseResult):

        # Extract the query data passed into
        queryData = urllib.parse.parse_qs(url.query)

        signature = {
            "bucket": url.netloc,
            "aws_access_key_id": queryData.get("aws_access_key_id", [None])[0],
            "aws_secret_access_key": queryData.get("aws_secret_access_key", [None])[0],
            "region_name": queryData.get("region_name", [None])[0],
            "storage_class": queryData.get("storage_class", ['STANDARD'])[0],
        }

        return signature, (url.path or '/')

    @property
    def root(self):
        return self._bucketName

    def toConfig(self):
        return self._config
