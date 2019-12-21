import boto3

from ..interfaces import Manager
from ..artefacts import File, Directory

toAWSPath = lambda x: x.strip('/')
fromAWSPath = lambda x: '/' + x.strip('/')

class Amazon(Manager):

    def __init__(self, bucket: str, aws_access_key: str, aws_secret: str, region: str):

        self._s3 = boto3.resource(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret,
            region_name=region
        )

        self._bucket = self._s3.Bucket(name=bucket)

        self.refresh()

    def _getHierarchy(self, path) -> Directory:

        if path in self._paths:
            # The path points to an already established directory

            directory = self._paths[path]

            if isinstance(directory, File):
                raise ValueError("Invalid path given. Path points to a file.")

            return directory

        # Create the directory at this location
        art = Directory(self, path)

        # Fetch the owning directory and add this diretory into it
        self._getHierarchy(fromAWSPath('/'.join(path.split('/')[:-1]))).add(art)

        self._paths[path] = art
        return art


    def refresh(self):

        for obj in self._bucket.objects.all():

            # Extract the path of the artefact from the remote
            remote_path = fromAWSPath(obj.key)
            if remote_path in self._paths:
                # An atefact already exists for this object - do nothing
                continue

            # Identify what the item is
            key_components = obj.key.split('/')

            # Split object key to obtain it's name and its relative location
            name = key_components[-1]
            directory_path = fromAWSPath('/'.join(key_components[:-1]))

            # Get the owning directory for the object
            owning_directory = self._getHierarchy(directory_path)

            if name:
                # The object is a file
                art = File(self, remote_path, obj.last_modified, obj.content_length)

            else:
                # The object is a directory
                art = Directory(self, remote_path)

            # Store the newly created artefact in the manager store
            self._paths[remote_path] = art
            owning_directory.add(art)

    def get(self, src_remote, dest_local):

        # Call the generic get function which ensures the src_remote path
        remote_path = toAWSPath(super.get(src_remote, dest_local))

        self._bucket.download_file(remote_path, dest_local)

    def put(self, src_local, dest_remote) -> File:

        # Resolve the destination location - get relative path for the manager and the absolute path to the files
        relativePath = super().put(src_local, dest_remote)
        abspath = os.path.join(self._path, relativePath.strip(os.path.sep))

        self._bucket.upload_file(Filename=abspath, Key=relativePath.strip(os.path.sep))

import os, better

path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'etc', 'aws_credentials.ini')
a = Amazon(**better.ConfigParser().read(path))