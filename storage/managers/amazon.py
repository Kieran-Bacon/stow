import os
import boto3
import tempfile
import re

from ..artefacts import Artefact, File, Directory
from ..manager import RemoteManager
from .. import exceptions

class Amazon(RemoteManager):

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

        self._s3 = boto3.resource(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

        # Create a reference to the AWS bucket - create a Directory to represent it
        self._bucket = self._s3.Bucket(name=bucket) # pylint: disable=no-member

        super().__init__()

    def _abspath(self, artefact: str):
        """ Difference between AWS and manager path is the removal of a leading '/'. As such remove the first character
        """
        _, path = self._artefactFormStandardise(artefact)
        return self._relpath(path)[1:]

    def _basename(self, artefact):
        _, path = self._artefactFormStandardise(artefact)
        return os.path.basename(self._relpath(path))

    def _dirname(self, path):
        _, path = self._artefactFormStandardise(path)
        return "/".join(self._relpath(path).split('/')[:-1]) or '/'

    def _walkOrigin(self, prefix = None):

        if prefix is None:
            objs = self._bucket.objects.all()
        else:
            objs = self._bucket.objects.filter(Prefix=self._abspath(prefix))

        return [self._relpath(obj.key) for obj in objs]

    def _makefile(self, remotePath: str):
        awsObject = self._bucket.Object(self._abspath(remotePath))
        return File(self, remotePath, awsObject.last_modified, awsObject.content_length)

    def _get(self, src_remote, dest_local):

        if isinstance(src_remote, Directory):

            for object in self._bucket.objects.filter(Prefix=self._abspath(src_remote)):
                # Collect the objects in that directory - downlad each one

                # Create object absolute path
                path = os.path.abspath(os.path.join(dest_local, object.key))

                # Ensure the directory for that object
                os.makedirs(os.path.dirname(path), exist_ok=True)

                # Download the file to that location
                self._bucket.download_file(object.key, path)

        else:
            self._bucket.download_file(self._abspath(src_remote), dest_local)

    def _put(self, src_local, dest_remote):

        if os.path.isdir(src_local):
            # A directory of items is to be uploaded - walk local directory and uploade each file

            for path, _, files in os.walk(src_local):

                # For each file at this point - construct their local absolute path and their relative remote path
                for file in files:
                    self._bucket.upload_file(
                        os.path.join(path, file),
                        self._abspath(self._join(dest_remote, path[len(src_local):], file))
                    )

        else:
            # Putting a file
            self._bucket.upload_file(src_local, dest_remote)

    def _rm(self, artefact: Artefact, artefactPath: str):

        key = self._abspath(artefactPath)
        if isinstance(artefact, Directory):
            for obj in self._bucket.objects.filter(Prefix=key):
                obj.delete()

        else:
            self._bucket.Object(key).delete()

    def _mvFile(self, source, destination):

        self._bucket.Object(destination).copy_from(CopySource={'Bucket': self._bucketName, 'Key': source})
        self._bucket.Object(source).delete()

    def _mv(self, srcObj: Artefact, destPath: str):
        """ Move the object to the desintation """

        source_path, dest_path = self._abspath(srcObj.path), self._abspath(destPath)
        if isinstance(srcObj, Directory):
            for obj in self._bucket.objects.filter(Prefix=source_path):
                relative = obj.key[len(source_path):]
                self._mvFile(obj.key, self._abspath(self._join(dest_path, relative)))

        else:
            self._mvFile(source_path, dest_path)

    def mkdir(self, path):

        with tempfile.TemporaryDirectory() as directory:
            fp = os.path.join(directory, self._PLACEHOLDER)
            open(fp, 'w').close()
            self._bucket.upload_file(Filename=fp, Key=self._abspath(self._join(path, self._PLACEHOLDER)))

        # Identify the owning directory
        owning_directory = self._backfillHierarchy(self._dirname(path))
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
