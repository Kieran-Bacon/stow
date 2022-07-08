import boto3
from botocore.exceptions import ClientError

import os
import re
import io
import typing
import urllib.parse
import enum
import mimetypes
from tqdm import tqdm

from ..artefacts import Artefact, File, Directory
from ..manager import RemoteManager
from .. import exceptions

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
        storage_class: str = 'STANDARD'
    ):

        self._bucketName = bucket

        if aws_session is None:
            self._aws_session = boto3.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=region_name,
            )

        else:
            self._aws_session = aws_session

        self._s3 = self._aws_session.client('s3')

        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_session_token = aws_session_token
        self._region_name = region_name
        self._storageClass = self.StorageClass(storage_class)

        super().__init__()

    def __repr__(self): return '<Manager(S3): {}>'.format(self._bucketName)

    def _abspath(self, managerPath: str) -> str:

        params = {}
        if self._aws_access_key_id:
            params = {
                "aws_access_key_id": self._aws_access_key_id,
                "aws_secret_access_key": self._aws_secret_access_key,
                "aws_session_token": self._aws_session_token,
                "region_name": self._region_name,
            }

        return urllib.parse.ParseResult(
                's3',
                self._bucketName,
                managerPath,
                params,
                '',
                ''
            ).geturl()

    def _managerPath(self, managerPath: str) -> str:
        """ Difference between AWS and manager path is the removal of a leading '/'. As such remove the first character
        """
        abspath = managerPath.strip("/")
        assert not abspath or self._S3_OBJECT_KEY.match(abspath) is not None, "artefact name isn't accepted by S3: {}".format(abspath)
        return abspath

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
                metadata=response['Metadata'],
                content_type=response['ContentType']
            )

        except:
            resp = self._s3.list_objects(Bucket=self._bucketName, Prefix=key+'/', Delimiter='/',MaxKeys=1)
            if "Contents" in resp or "CommonPrefixes" in resp:
                return Directory(self, key)

            return None

    def _exists(self, abspath: str):
        return self._identifyPath(abspath) is not None

    def _metadata(self, managerPath: str) -> typing.Dict[str, str]:
        key = self._managerPath(managerPath)

        try:
            response = self._s3.head_object(
                Bucket=self._bucketName,
                Key=key
            )

            return response.get('Metadata', {})

        except:
            raise exceptions.ArtefactNotAvailable(f'Failed to fetch metadata information for {key}')

    def _isLink(self, abspath: str):
        return False

    def _isMount(self, abspath: str):
        return False

    def _list_objects(self, bucket: str, key: str, delimiter: str = ""):
        marker = ""
        while True:
            response = self._s3.list_objects(
                Bucket=bucket,
                Prefix=key + '/',
                Marker=marker,
                Delimiter=delimiter
            )

            for fileMetadata in response.get('Contents', []):
                yield fileMetadata

            if not response['IsTruncated']:
                break

            marker = response['NextMarker']

    def _get(self, source: Artefact, destination: str):

        # Convert manager path to s3
        keyName = self._managerPath(source.path)

        # If the source object is a directory
        if isinstance(source, Directory):

            # Loop through all objects under that prefix and create them locally
            for artefact in self._ls(keyName):

                # Get the objects relative path to the directory
                relativePath = self.relpath(artefact.path, keyName)

                # Create object absolute path locally
                path = os.path.join(destination, relativePath)

                # Ensure the directory for that object
                os.makedirs(os.path.dirname(path), exist_ok=True)

                # Download the artefact to that location
                if isinstance(artefact, File):
                    self._s3.download_file(
                        self._bucketName,
                        artefact.path,
                        path
                    )

                else:
                    # Recursively get the child directory
                    self._get(artefact, path)

        else:
            self._s3.download_file(
                self._bucketName,
                keyName,
                destination
            )
            # self._bucket.download_file(keyName, destination)

    def _getBytes(self, source: Artefact) -> bytes:

        # Get buffer to recieve bytes
        bytes_buffer = io.BytesIO()

        # Fetch the file bytes and write them to the buffer
        self._s3.download_fileobj(Bucket=self._bucketName, Key=self._managerPath(source.path), Fileobj=bytes_buffer)

        # Return the bytes stored in the buffer
        return bytes_buffer.getvalue()

    def _upload_file(self, path: str, key: str):
        """ Wrapper for the boto3 upload_file method to load the file content types """

        self._bucket.upload_file(
            path,
            key,
            ExtraArgs = {
                'StorageClass': self._storageClass.value,
                'ContentType': (mimetypes.guess_type(path)[0] or 'application/octet-stream')
            }
        )


    def _put(self, source: str, destination: str):

        destination = self._abspath(destination)

        if os.path.isdir(source):
            # A directory of items is to be uploaded - walk local directory and uploaded each file

            sourcePathLength = len(source) + 1

            for root, dirs, files in os.walk(source):

                dRoot = self.join(destination, root[sourcePathLength:], separator='/')

                if not (dirs or files):
                    # There are no sub-directories or files to be uploaded
                    placeholder_path = self.join(dRoot, self._PLACEHOLDER, separator='/')
                    self._bucket.put_object(Key=placeholder_path, Body=b'', StorageClass=self._storageClass.value)
                    continue

                # For each file at this point - construct their local absolute path and their relative remote path
                for file in files:
                    self._upload_file(os.path.join(root, file), self.join(dRoot, file, separator='/'))

        else:
            # Putting a file
            self._upload_file(source, destination)

    def _putBytes(self, fileBytes: bytes, destination: str, *, metadata: typing.Dict[str, str] = None):

        self._s3.put_object(
            Bucket=self._bucketName,
            Key=self._managerPath(destination),
            Body=fileBytes,
            StorageClass=self._storageClass.value,
            ContentType=(mimetypes.guess_type(destination)[0] or 'application/octet-stream'),
            Metadata=({str(k): str(v) for k, v in metadata.items()} if metadata else {})
        )

    def _cpFile(self, source, destination):
        self._bucket.Object(destination).copy_from(
            CopySource={'Bucket': self._bucketName, 'Key': source},
            ContentType=(mimetypes.guess_type(destination)[0] or 'application/octet-stream')
        )

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

    def _ls(self, directory: str):

        key = self._managerPath(directory) + '/'

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
                    size=fileMetadata['Size']
                )

            for prefix in response.get('CommonPrefixes', []):
                yield Directory(self, prefix['Prefix'])

            if not response['IsTruncated']:
                return

            marker = response['NextMarker']

        if directory != "/":
            key = self._abspath(directory) + "/"

        else:
            key = ""

        # Create a pagination that looks specifically at the manager path given
        pages = self._clientPaginator.paginate(
            Bucket=self._bucketName,
            Prefix=key,
            Delimiter="/"
        )

        # Iterate over the pages
        for page in pages:
            if page is None:
                break

            # Check To see if there are any files with the given name
            if "Contents" in page:
                # There are files that lead with the manager path given
                for file in page["Contents"]:

                    if self.basename(file["Key"]) == self._PLACEHOLDER:
                        # Don't list placeholders
                        continue

                    self._addArtefact(
                        File(
                            self,
                            "/" + file["Key"],
                            modifiedTime=file["LastModified"],
                            size=file["Size"]
                        )
                    )

            if "CommonPrefixes" in page:
                for directory in page["CommonPrefixes"]:
                    self._addArtefact(Directory(self, "/" + directory["Prefix"][:-1]))

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
