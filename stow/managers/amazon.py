import boto3
from botocore.exceptions import ClientError

import os
import re
import io
import typing
import urllib.parse
import enum

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
    _S3_OBJECT_KEY = re.compile(r"^[a-zA-Z0-9!_.*'()-]+(/[a-zA-Z0-9!_.*'()-]+)*$")

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
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        region_name: str = None,
        storage_class: str = 'STANDARD'
    ):

        self._bucketName = bucket
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._region_name = region_name
        self._storageClass = self.StorageClass(storage_class)

        self._s3Client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        self._s3Resource = boto3.resource(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

        # Create a paginator object for iterating through remote objects
        self._clientPaginator = self._s3Client.get_paginator('list_objects')

        # Create a reference to the AWS bucket - create a Directory to represent it
        self._bucket = self._s3Resource.Bucket(name=bucket) # pylint: disable=no-member

        super().__init__()

    def __repr__(self): return '<Manager(S3): {}>'.format(self._bucketName)

    def _abspath(self, managerPath: str) -> str:
        """ Difference between AWS and manager path is the removal of a leading '/'. As such remove the first character
        """
        abspath = managerPath.strip("/")
        assert not abspath or self._S3_OBJECT_KEY.match(abspath) is not None, "artefact name isn't accepted by S3: {}".format(abspath)
        return abspath

    def _identifyPath(self, managerPath: str) -> typing.Union[Artefact, None]:

        # Extract the key path
        key = self._abspath(managerPath)

        # Create a pagination that looks specifically at the manager path given
        pages = self._clientPaginator.paginate(
            Bucket=self._bucketName,
            Prefix=key,
            Delimiter="/"
        )

        # Iterate over the pages -
        for page in pages:
            if page is None:
                break

            # Check To see if there are any files with the given name
            if "Contents" in page:
                # There are files that lead with the manager path given
                for file in page["Contents"]:
                    if file["Key"] == key:
                        # We've found the file artefact that directly matches - create a file and return
                        return File(
                            self,
                            managerPath,
                            modifiedTime=file["LastModified"],
                            size=file["Size"]
                        )

            if "CommonPrefixes" in page:

                # Directories end in "/"
                key += "/"

                for directory in page["CommonPrefixes"]:
                    if directory["Prefix"] == key:
                        # The path is to the directory
                        return Directory(self, managerPath)

        return None

    def _loadArtefact(self, managerPath: str) -> Artefact:

        if managerPath in self._paths:
            # We may have already pulled the manager
            return super()._loadArtefact(managerPath)

        # Ensure the owning directory and fetch the directory object
        try:
            directory = self._ensureDirectory(self.dirname(managerPath))

        except (exceptions.ArtefactNotFound, exceptions.ArtefactTypeError) as e:
            raise exceptions.ArtefactNotFound("Cannot locate artefact {}".format(managerPath)) from e

        # Add all artefacts of the directory into the manager - adding the target artefact at the same time
        self._ls(directory.path)
        directory._collected = True

        # Check now that the directory has been downloaded and added that the original target exists
        if managerPath in self._paths:
            return self._paths[managerPath]

        else:
            raise exceptions.ArtefactNotFound("Cannot locate artefact {}".format(managerPath))

    def _get(self, source: Artefact, destination: str):

        # Convert manager path to s3
        keyName = self._abspath(source.path)

        # If the source object is a directory
        if isinstance(source, Directory):

            # Loop through all objects in the bucket and create them locally
            for object in self._bucket.objects.filter(Prefix=keyName):

                # Get the objects relative path to the directory
                relativePath = self.relpath(object.key, keyName)

                # Create object absolute path
                path = os.path.join(destination, relativePath)

                # Ensure the directory for that object
                os.makedirs(os.path.dirname(path), exist_ok=True)

                # Download the file to that location
                self._bucket.download_file(object.key, path)

        else:
            self._bucket.download_file(keyName, destination)

    def _getBytes(self, source: Artefact) -> bytes:

        # Get buffer to recieve bytes
        bytes_buffer = io.BytesIO()

        # Fetch the file bytes and write them to the buffer
        self._s3Client.download_fileobj(Bucket=self._bucketName, Key=self._abspath(source.path), Fileobj=bytes_buffer)

        # Return the bytes stored in the buffer
        return bytes_buffer.getvalue()

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
                    self._bucket.upload_file(
                        os.path.join(root, file),
                        self.join(dRoot, file, separator='/'),
                        ExtraArgs = {'StorageClass': self._storageClass.value}
                    )

        else:
            # Putting a file
            self._bucket.upload_file(source, destination, ExtraArgs = {'StorageClass': self._storageClass.value})

    def _putBytes(self, fileBytes: bytes, destination: str):
        self._bucket.put_object(Key=self._abspath(destination), Body=fileBytes, StorageClass=self._storageClass.value)

    def _cpFile(self, source, destination):
        self._bucket.Object(destination).copy_from(CopySource={'Bucket': self._bucketName, 'Key': source})

    def _cp(self, source: Artefact, destination: str):

        # Convert the paths to s3 paths
        sourcePath, destinationPath = self._abspath(source.path), self._abspath(destination)

        # Detemine how to handle the source
        if isinstance(source, Directory):
            # Source is director - loop through and copy each file object
            for obj in self._bucket.objects.filter(Prefix=sourcePath):
                self._cpFile(obj.key, self.join(destinationPath, self.relpath(obj.key, sourcePath), separator='/'))

        else:
            # Source is a file - copy directly to location
            self._cpFile(sourcePath, destinationPath)

    def _mv(self, source: Artefact, destination: str):

        # Convert the paths to s3 paths
        sourcePath, destinationPath = self._abspath(source.path), self._abspath(destination)

        # Detemine how to handle the source
        if isinstance(source, Directory):
            # Source is director - loop through and copy each file object
            for obj in self._bucket.objects.filter(Prefix=sourcePath):
                self._cpFile(obj.key, self.join(destinationPath, self.relpath(obj.key, sourcePath), separator='/'))
                obj.delete()

        else:
            # Source is a file - copy directly to location
            self._cpFile(sourcePath, destinationPath)
            self._bucket.Object(sourcePath).delete()

    def _ls(self, directory: str):

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

        key = self._abspath(artefact.path)

        if isinstance(artefact, Directory):
            for obj in self._bucket.objects.filter(Prefix=key):
                obj.delete()

        else:
            self._bucket.Object(key).delete()

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

    def toConfig(self):
        return {
            'manager': 'AWS',
            'bucket': self._bucketName,
            'aws_access_key_id': self._aws_access_key_id,
            'aws_secret_access_key': self._aws_secret_access_key,
            'region_name': self._region_name,
            'storage_class': self._storageClass.value
        }
