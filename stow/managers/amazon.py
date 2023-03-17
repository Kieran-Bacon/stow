""" The implementation of Amazon's S3 storage manager for stow """

import os
import re
import io
import typing
import urllib.parse
import hashlib
import enum
import mimetypes
import datetime
import concurrent.futures as futures

from tqdm import tqdm
import boto3
from botocore.exceptions import ClientError

from ..artefacts import Artefact, File, Directory
from ..manager import RemoteManager
from .. import exceptions

def getS3ETag(bytes_readable):

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
                digests.append(getS3ETag(handle))

    return digests[0] == digests[1]

class Amazon(RemoteManager):
    """ Connect to an amazon s3 bucket using an IAM user credentials or environment variables

    Params:
        bucket: The s3 bucket name
        aws_access_key_id (None): The access key for a IAM user that has permissions to the bucket
        aws_secret_access_key (None): The secret key for a IAM user that has permissions to the bucket
        region_name (None): The region of the user/bucket
        storage_class (STANDARD): The storage class type name e.g. STANDARD, REDUCED_REDUDANCY

    """

    # Define regex for the object key
    _LINE_SEP = "/"
    _S3_OBJECT_KEY = re.compile(r"^[a-zA-Z0-9!_.*'()\- ]+(/[a-zA-Z0-9!_.*'()\- ]+)*$")
    _S3_DISALLOWED_CHARACTERS = "{}^%`]'`\"<>[~#|"

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
        storage_class: str = 'STANDARD'
    ):

        self._bucketName = bucket

        if aws_session is None:
            aws_session = boto3.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=region_name,
                profile_name=profile_name
            )

        self._aws_session = aws_session
        self._s3 = self._aws_session.client('s3')

        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_session_token = aws_session_token
        self._region_name = region_name
        self._profile_name = profile_name
        self._storageClass = self.StorageClass(storage_class)

        super().__init__()

    def __repr__(self): return '<Manager(S3): {}>'.format(self._bucketName)

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

        try:
            response = self._s3.head_object(
                Bucket=self._bucketName,
                Key=key
            )



            return File(
                self,
                key,
                modifiedTime=response['LastModified'],
                size=response['ContentLength'],
                metadata=self._getMetadataFromHead(response),
                content_type=response['ContentType'],
                # digest=
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
                    return Directory(self, key)

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

                    # Get the objects relative path to the directory
                    relativePath = self.relpath(artefact.path, keyName)

                    # Create object absolute path locally
                    path = os.path.join(destination, relativePath)

                    # Ensure the directory for that object
                    os.makedirs(os.path.dirname(path), exist_ok=True)

                    # Download the artefact to that location
                    if isinstance(artefact, File):

                        future = executor.submit(
                            self._s3.download_file,
                            self._bucketName,
                            artefact.path,
                            path,
                            Callback=Callback and Callback(artefact, is_downloading=True)
                        )
                        future_collection.append(future)

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

                for artefact in source.ls(recursive=True):
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

    def _list_objects(self, bucket: str, key: str, delimiter: str = ""):

        if key:
            key += '/'

        marker = ""
        while True:
            response = self._s3.list_objects(
                Bucket=bucket,
                Prefix=key,
                Marker=marker,
                Delimiter=delimiter
            )

            for fileMetadata in response.get('Contents', []):
                yield fileMetadata

            if not response['IsTruncated']:
                break

            marker = response['NextMarker']

    def _cp(self, source: Artefact, destination: str):

        # Convert the paths to s3 paths
        sourceBucket, sourcePath = source.manager.root, self._managerPath(source.path)
        destinationPath = self._managerPath(destination)

        if isinstance(source, Directory):
            for fileMetadata in tqdm(self._list_objects(sourceBucket, sourcePath), desc=f"Copying {sourcePath} -> {destinationPath}"):

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

    def _mv(self, source: Artefact, destination: str):

        # Convert the paths to s3 paths
        sourceBucket, sourcePath = source.manager.root, self._managerPath(source.path)
        destinationPath = self._managerPath(destination)

        if isinstance(source, Directory):
            for fileMetadata in tqdm(self._list_objects(sourceBucket, sourcePath), desc=f"Moving {sourcePath} -> {destinationPath}"):

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

                # Delete the original object
                self._s3.delete_object(
                    Bucket=sourceBucket,
                    Key=fileMetadata['Key']
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

            # Delete the original object
            self._s3.delete_object(
                Bucket=sourceBucket,
                Key=sourcePath
            )

    def _ls(self, directory: str, recursive: bool = False):

        key = self._managerPath(directory)
        if key:
            key += '/'

        marker = ""
        while True:

            response = self._s3.list_objects(
                Bucket=self._bucketName,
                Prefix=key,
                Delimiter='/',
                MaxKeys=100,
                Marker=marker
            )

            for fileMetadata in response.get('Contents', []):
                if fileMetadata['Key'] == '':
                    continue

                yield File(
                    self,
                    fileMetadata['Key'],
                    modifiedTime=fileMetadata['LastModified'],
                    size=fileMetadata['Size'],
                    digest=fileMetadata['ETag'].strip('"')
                )

            for prefix in response.get('CommonPrefixes', []):
                yield Directory(self, prefix['Prefix'])

                if recursive:
                    yield from self._ls(prefix['Prefix'], True)

            if not response['IsTruncated']:
                return

            marker = response['NextMarker']

    def _rm(self, artefact: Artefact):

        key = self._managerPath(artefact.path)

        if isinstance(artefact, Directory):
            keys = []

            marker = ""
            while True:
                response = self._s3.list_objects(
                    Bucket=self._bucketName,
                    Prefix=key + '/',
                    Marker=marker
                )

                for fileMetadata in response.get('Contents', []):
                    keys.append({"Key": fileMetadata['Key']})

                if not response.get('IsTruncated', False):
                    break
                marker = response['NextMarker']

        else:
            keys = [{"Key": key}]

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
        return {
            'manager': 'AWS',
            'bucket': self._bucketName,
            'aws_access_key_id': self._aws_access_key_id,
            'aws_secret_access_key': self._aws_secret_access_key,
            'region_name': self._region_name,
            'storage_class': self._storageClass.value
        }
