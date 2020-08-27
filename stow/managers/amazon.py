import os
import boto3
from botocore.exceptions import ClientError
import tempfile
import re

from ..artefacts import Artefact, File, Directory
from ..manager import RemoteManager
from .. import exceptions

class Amazon(RemoteManager):

    # Define regex for the object key
    _S3_OBJECT_KEY = re.compile(r"^[a-zA-Z0-9!_.*'()-]+(/[a-zA-Z0-9!_.*'()-]+)*$")

    def __init__(
        self,
        bucket: str,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        region_name: str = None
    ):

        self._bucketName = bucket
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._region_name = region_name

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

    def isabs(self, path: str) -> bool:
        return self._S3_OBJECT_KEY.match(path) is not None

    def abspath(self, relpath: str):
        """ Difference between AWS and manager path is the removal of a leading '/'. As such remove the first character
        """
        return self.relpath(relpath)[1:]

    def _isdir(self, relpath: str):

        # Get manager specific path
        abspath = self.abspath(relpath)
        if not abspath:
            # Referencing bucket
            return True

        try:
            # Try and load the object outright - if successful then file not dir
            self._bucket.Object(abspath).load()
            return False

        except ClientError as e:
            # No object existed at location - check if a directory exists
            if e.response["Error"]["Code"] != "404": raise

            # Get all directories for that level
            dirs = self._clientPaginator.paginate(
                Bucket=self._bucketName,
                Prefix=self.abspath(self.dirname(relpath)) + "/",
                Delimiter='/'
            )

            # Loop through the returned directires and if any match return True
            for dirObj in dirs.search("CommonPrefixes"):
                if dirObj is None: continue
                if relpath == self.relpath(dirObj.get("Prefix")): return True

            raise exceptions.ArtefactNotFound("Couldn't find artefact with relpath: {}".format(relpath))

    def _makefile(self, remotePath: str):
        try:
            awsObject = self._bucket.Object(self.abspath(remotePath))
        except Exception as e:
            raise exceptions.ArtefactNotFound(
                "Couldn't create file at {} as it doesn't exist".format(remotePath)
            ) from e

        return File(self, remotePath, awsObject.last_modified, awsObject.content_length)

    def _get(self, src_remote: Artefact, dest_local: str):

        if isinstance(src_remote, Directory):

            # Identify the prefix for the directory
            prefix = self.abspath(src_remote.path)

            for object in self._bucket.objects.filter(Prefix=prefix):
                # Collect the objects in that directory - downlad each one

                # Write the relative path
                relative_path = object.key[len(prefix) + 1:]

                # Create object absolute path
                path = os.path.abspath(os.path.join(dest_local, relative_path))

                # Ensure the directory for that object
                os.makedirs(os.path.dirname(path), exist_ok=True)

                # Download the file to that location
                self._bucket.download_file(object.key, path)

        else:
            self._bucket.download_file(self.abspath(src_remote.path), dest_local)

    def _put(self, src_local, dest_remote, merge: bool = False):

        if os.path.isdir(src_local):
            # A directory of items is to be uploaded - walk local directory and uploaded each file

            for root, dirs, files in os.walk(src_local):
                dRoot = self.join(dest_remote, root[len(src_local):])

                if not (dirs or files):
                    # There are no sub-directories or files to be uploaded

                    if merge and dRoot in self and not self[dRoot].isEmpty():
                        # We are merging a directory that has contents so no activity to take
                        continue

                    placeholder_path = self.abspath(self.join(dRoot, self._PLACEHOLDER))
                    self._bucket.put_object(Key=placeholder_path, Body=b'')
                    continue

                # For each file at this point - construct their local absolute path and their relative remote path
                for file in files:
                    self._bucket.upload_file(
                        os.path.join(root, file),
                        self.abspath(self.join(dRoot, file))
                    )

        else:
            # Putting a file
            self._bucket.upload_file(src_local, dest_remote)

    def _putBytes(self, source, destinationAbsPath):
        self._bucket.put_object(Key=destinationAbsPath, Body=source)

    def _rm(self, artefact: Artefact):

        key = self.abspath(artefact.path)
        if isinstance(artefact, Directory):
            for obj in self._bucket.objects.filter(Prefix=key):
                obj.delete()

        else:
            self._bucket.Object(key).delete()

    def _cpFile(self, source, destination):
        self._bucket.Object(destination).copy_from(CopySource={'Bucket': self._bucketName, 'Key': source})

    def _cp(self, srcObj: Artefact, destPath: str):
        """ Move the object to the desintation """

        source_path, dest_path = self.abspath(srcObj.path), self.abspath(destPath)
        if isinstance(srcObj, Directory):
            for obj in self._bucket.objects.filter(Prefix=source_path):
                relative = obj.key[len(source_path):]
                self._cpFile(obj.key, self.abspath(self.join(dest_path, relative)))

        else:
            self._cpFile(source_path, dest_path)

    def _mvFile(self, source, destination):
        self._cpFile(source, destination)
        self._bucket.Object(source).delete()

    def _mv(self, srcObj: Artefact, destPath: str):
        """ Move the object to the desintation """

        source_path, dest_path = self.abspath(srcObj.path), self.abspath(destPath)
        if isinstance(srcObj, Directory):
            for obj in self._bucket.objects.filter(Prefix=source_path):
                relative = obj.key[len(source_path):]
                self._mvFile(obj.key, self.abspath(self.join(dest_path, relative)))

        else:
            self._mvFile(source_path, dest_path)

    def _collectDirectoryContents(self, directory: Directory):

        abspath = self.abspath(directory.path)
        if abspath: abspath += "/"

        # Extract the relevent objects from s3
        dirs = self._clientPaginator.paginate(Bucket=self._bucketName, Prefix=abspath, Delimiter='/')
        for dirRelpath in (self.relpath(p.get("Prefix")) for p in dirs.search("CommonPrefixes") if p is not None):
            self._backfillHierarchy(dirRelpath)

        # Iterate over the files
        files = iter(self._bucket.objects.filter(Prefix=abspath, Delimiter='/'))
        for obj in files:
            key = obj.key
            relpath = self.relpath(key)

            if (
                relpath in self._paths or
                key == abspath or
                key.endswith(self._PLACEHOLDER)
                ):
                continue

            self._add(
                File(self, relpath, obj.last_modified, obj.size)
            )

        directory._collected = True

    def _listdir(self, relpath: str):

        abspath = self.abspath(relpath)
        if abspath: abspath += "/"

        # Extract the relevent objects from s3
        dirs = self._clientPaginator.paginate(Bucket=self._bucketName, Prefix=abspath, Delimiter='/')
        files = self._bucket.objects.filter(Prefix=abspath, Delimiter='/')

        # Expand and convert s3 objects
        dirs = {self.relpath(p.get("Prefix")) for p in dirs.search("CommonPrefixes") if p is not None}
        files = {self.relpath(obj.key) for obj in files if obj.key != abspath and obj.key.split('/')[-1] != self._PLACEHOLDER}

        return dirs, files

    def mkdir(self, path):

        with tempfile.TemporaryDirectory() as directory:
            fp = os.path.join(directory, self._PLACEHOLDER)
            open(fp, 'w').close()
            self._bucket.upload_file(Filename=fp, Key=self.abspath(self.join(path, self._PLACEHOLDER)))

        # Identify the owning directory
        owning_directory = self._backfillHierarchy(self.dirname(path))
        art = Directory(self, path)

        # Save the new artefact
        owning_directory._add(art)
        self._paths[path] = art
        return art

    def toConfig(self):
        return {
            'manager': 'AWS',
            'bucket': self._bucketName,
            'aws_access_key_id': self._aws_access_key_id,
            'aws_secret_access_key': self._aws_secret_access_key,
            'region_name': self._region_name
        }
