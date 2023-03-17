""" The implementation of Amazon's S3 storage manager for stow """

import os
import re
import io
import typing
import urllib.parse
import enum
import mimetypes
import datetime
import concurrent.futures as futures
import logging

from tqdm import tqdm
import boto3
from botocore.exceptions import ClientError

from ..artefacts import Artefact, PartialArtefact, File, Directory, HashingAlgorithm
from ..manager import RemoteManager
from .. import exceptions

log = logging.getLogger(__name__)

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

    def _identifyPath(self, managerPath: str) -> typing.Union[Artefact, None]:

        key = self._managerPath(managerPath)
        if not key:
            return Directory(self, '/')

        try:
            response = self._s3.head_object(
                Bucket=self._bucketName,
                Key=key
            )

            log.debug('Head %s: %s', key, response)

            # TODO
            # The ETag of multipart uploads is not MD5 - each part has a md5 calculated and then
            # they are concaticated before they are MD5'd for the final etag. It is then followed
            # by a -xxxx that stored the number of parts that made up the upload.

            return File(
                self,
                '/' + key,
                modifiedTime=response['LastModified'],
                size=response['ContentLength'],
                metadata=response['Metadata'],
                content_type=response['ContentType'],
                digest={
                    HashingAlgorithm.MD5: response['ETag'].strip('"')
                }
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

            return response.get('Metadata', {})

        except ClientError as e:
            if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
                raise exceptions.ArtefactNoLongerExists(
                    f'Failed to fetch metadata information for {key}'
                )

            raise

    def _digest(self, file: File, algorithm: HashingAlgorithm):

        if algorithm is HashingAlgorithm.MD5:
            # The MD5 is downloaded and set at File creation - the object will have it set to return
            return file.digest(algorithm=algorithm)

        else:

            checksums = self._s3.get_object_attributes(
                Bucket=self._bucketName,
                Key=self._managerPath(file.path),
                ObjectAttributes=['Checksum']
            )

            log.debug(f'Checksums collected for {file.path}: {checksums}')

            if algorithm is HashingAlgorithm.CRC32:
                return checksums['ChecksumCRC32']
            elif algorithm is HashingAlgorithm.CRC32C:
                return checksums['ChecksumCRC32C']
            elif algorithm is HashingAlgorithm.SHA1:
                return checksums['ChecksumSHA1']
            elif algorithm is HashingAlgorithm.SHA256:
                return checksums['ChecksumSHA256']
            else:
                raise NotImplementedError(f'Amazon does not provide {algorithm} hashing')

    def _isLink(self, _: str):
        return False

    def _isMount(self, _: str):
        return False

    def _get(self, source: Artefact, destination: str, *, Callback = None):

        # Convert manager path to s3
        keyName = self._managerPath(source.path)

        # If the source object is a directory
        if isinstance(source, Directory):

            with futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:

                future_collection = []

                # Loop through all objects under that prefix and create them locally
                for artefact in self._ls(keyName, recursive=True):

                    # Get the artefact manager path
                    artefactPath = self._managerPath(artefact.path)

                    # Get the objects relative path to the directory
                    relativePath = self.relpath(artefactPath, keyName)

                    # Create object absolute path locally
                    path = os.path.join(destination, relativePath)

                    # Download the artefact to that location
                    if isinstance(artefact, File):

                        # Ensure the directory for that object
                        os.makedirs(os.path.dirname(path), exist_ok=True)

                        future = executor.submit(
                            self._s3.download_file,
                            self._bucketName,
                            artefactPath,
                            path,
                            Callback=Callback and Callback(artefact, is_downloading=True)
                        )
                        future_collection.append(future)

                    else:
                        os.makedirs(path, exist_ok=True)

                for future in futures.as_completed(future_collection):
                    exception = future.exception()
                    if exception:
                        raise exception

        else:
            self._s3.download_file(
                self._bucketName,
                keyName,
                destination,
                Callback=Callback and Callback(source, is_downloading=True)
            )

    def _getBytes(self, source: Artefact, Callback = None) -> bytes:

        # Get buffer to recieve bytes
        bytes_buffer = io.BytesIO()

        # Fetch the file bytes and write them to the buffer
        self._s3.download_fileobj(
            Bucket=self._bucketName,
            Key=self._managerPath(source.path),
            Fileobj=bytes_buffer,
            Callback=Callback and Callback(source, is_downloading=True)
        )

        # Return the bytes stored in the buffer
        return bytes_buffer.getvalue()

    def _localise_put_file(
        self,
        source: File,
        destination: str,
        extra_args: typing.Dict,
        Callback = None
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
                Callback=Callback and Callback(source, is_downloading=False)
            )

    def _put(self, source: Artefact, destination: str, *, metadata = None, Callback = None):

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
                                    Callback=Callback
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
                Callback=Callback
            )

    def _putBytes(
        self,
        fileBytes: bytes,
        destination: str,
        *,
        metadata: typing.Dict[str, str] = None,
        Callback = None
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
            Callback=Callback and Callback(File(self, destination, len(fileBytes), datetime.datetime.now(tz=datetime.timezone.utc)), is_downloading=False)
        )

        return PartialArtefact(self, destination)

    def _list_objects(self, bucket: str, key: str, delimiter: str = "") -> typing.Tuple[typing.Dict, bool]:

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

            for fileMetadata in response.get('Contents', []):
                yield fileMetadata, True

            for prefix in response.get('CommonPrefixes', []):
                yield prefix, False

            if not response['IsTruncated']:
                break

            marker = response['NextMarker']

    def _cp(self, source: Artefact, destination: str) -> Artefact:

        # Convert the paths to s3 paths
        sourceBucket, sourcePath = self.manager(source).root, self._managerPath(source.path)
        destinationPath = self._managerPath(destination)

        if isinstance(source, Directory):
            for fileMetadata, _ in tqdm(self._list_objects(sourceBucket, sourcePath), desc=f"Copying {sourcePath} -> {destinationPath}"):
                # Copy the object from the source object to the relative location in the destination location
                self._s3.copy_object(
                    CopySource={
                        "Bucket": sourceBucket,
                        "Key": fileMetadata['Key']
                    },
                    Bucket=self._bucketName,
                    # The Relative path to the source joined onto the destination path
                    Key=self.join(destinationPath, self.relpath(fileMetadata['Key'], sourcePath), separator='/')
                )

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

        return PartialArtefact(self, destination)

    def _mv(self, source: Artefact, destination: str):

        # Copy the source objects to the destination
        copied_artefact = self._cp(source, destination)

        # Delete the source objects now it has been entirely copied
        self._rm(source)

        return copied_artefact

    def _ls(self, directory: str, recursive: bool = False):

        for metadata, is_file in self._list_objects(
            self._bucketName,
            self._managerPath(directory),
            delimiter='/'
            ):

            if is_file:
                if metadata['Key'].endswith('/'):
                    continue

                yield File(
                    self,
                    '/' + metadata['Key'],
                    modifiedTime=metadata['LastModified'],
                    size=metadata['Size'],
                    digest={
                        HashingAlgorithm.MD5: metadata['ETag'].strip('"')
                    }
                )

            else:
                path = '/' + metadata['Prefix'][:-1]
                yield Directory(self, '/' + metadata['Prefix'][:-1])
                if recursive:
                    yield from self._ls(path, recursive=recursive)

    def _rm(self, artefact: Artefact):

        key = self._managerPath(artefact.path)

        if isinstance(artefact, Directory):

            # Iterate through the list objects - and enqueue them to be deleted
            keys = []
            for file_metadata, _ in self._list_objects(self._bucketName, key):
                keys.append({"Key": file_metadata['Key']})

        else:
            keys = [{"Key": key}]

        # Delete the enqueued objects
        self._s3.delete_objects(
            Bucket=self._bucketName,
            Delete={
                "Objects": keys,
                "Quiet": True
            }
        )

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
