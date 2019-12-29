#TODO Ensure that when objects are removed from a directory (such that its now empty) that the directory itself isn't removed
#TODO maybe clear up the placeholder object in the event that the directory has items added to it

import os
import boto3
import tempfile

from ..interfaces import Artefact
from ..artefacts import File, Directory
from ..manager import Manager

PLACEHOLDER = 'placeholder.ignore'
toAWSPath = lambda x: x.strip('/')
fromAWSPath = lambda x: '/' + x.strip('/')
dirpath = lambda x: '/'.join(x.split('/')[:-1]) or '/'

class Amazon(Manager):

    def __init__(
        self,
        name: str,
        bucket: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: str
    ):
        super().__init__(name)

        self._s3 = boto3.resource(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

        # Create a reference to the AWS bucket - create a Directory to represent it
        self._bucket = self._s3.Bucket(name=bucket)
        self._paths['/'] = Directory(self, '/')

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


    def refresh(self, prefix=None):

        # TODO Ensure that when refreshing, that artefacts that are believed to exist that no longer to are removed
        #? Thinking that a set of the expected names could be created
        #? When a file is found its path is removed from the set
        #? Any left names must be removed from the manager => self.rm(name)
        #? Must build in precausion for ^ sub prefixing

        # Create iterable for AWS objects
        iterable = self._bucket.objects.all() if prefix is None else self._bucket.objects.filter(Prefix=toAWSPath(prefix))

        for obj in iterable:

            # Extract the path of the artefact from the remote
            remote_path = fromAWSPath(obj.key)
            if remote_path in self._paths:
                # An atefact already exists for this object

                # Get artefact
                art = self._paths[remote_path]

                if isinstance(art, File):
                    # Update the meta of the file with the objectSummary
                    art._update(obj.last_modified, obj.size)

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
                if name == PLACEHOLDER: continue

                art = File(self, remote_path, obj.last_modified, obj.size)

            else:
                # The object is a directory
                art = Directory(self, remote_path)

            # Store the newly created artefact in the manager store
            self._paths[remote_path] = art
            owning_directory.add(art)

    def get(self, src_remote, dest_local):

        # Call the generic get function which ensures the src_remote path
        path = super().get(src_remote, dest_local)
        remote_path = toAWSPath(path)

        # Get the local artefact that is being downloaded
        if path in self._paths:
            art = self._paths[path]
        else:
            raise FileNotFoundError("Couldn't find find artefact with path {}".format(path))

        if isinstance(art, Directory):

            for object in self._bucket.objects.filter(Prefix=remote_path):
                # Collect the objects in that directory - downlad each one

                # Create object absolute path
                path = os.path.abspath(os.path.join(dest_local, object.key))

                # Ensure the directory for that object
                os.makedirs(os.path.dirname(path), exist_ok=True)

                # Download the file to that location
                self._bucket.download_file(object.key, path)

        else:

            self._bucket.download_file(remote_path, dest_local)

    def put(self, src_local, dest_remote) -> File:

        with super().put(src_local, dest_remote) as (source_path, destination_path):

            if os.path.isdir(source_path):

                if destination_path in self._paths:
                    self.rm(destination_path, True)

                # Walk the directory and add each object to AWS
                for path, _, files in os.walk(source_path):

                    # Remove the original source
                    relative_path = path.replace(source_path, '')

                    # Need to attach the dest_local
                    remote_path = toAWSPath(destination_path + relative_path)

                    for file in files:
                        self._bucket.upload_file(os.path.join(path, file), remote_path + '/' + file)

                # Refresh the internal objects of the manager from the new directory
                self.refresh(destination_path)
                return self._paths[destination_path]

            else:
                key = toAWSPath(destination_path)

                # Upload the the file to the location and collect an AWS object for that file
                self._bucket.upload_file(Filename=source_path, Key=key)
                awsFile = self._bucket.Object(key)

                if destination_path in self._paths:
                    # The file has been updated and the artefact for that file needs to be updated
                    file = self._paths[destination_path]
                    file._update(awsFile.last_modified, awsFile.content_length)

                else:
                    # The file is a new file and needs to be generated
                    file = File(self, destination_path, awsFile.last_modified, awsFile.content_length)

                    self._paths[destination_path] = file
                    directory = self._getHierarchy(dirpath(destination_path))
                    directory.add(file)

                return file

    def rm(self, artefact: Artefact, recursive: bool = False):

        # Ensure/Resolve the passed object
        path = super().rm(artefact, recursive=recursive)

        # Get the artefact that is being deleted
        art = self._paths[path]

        # Get the owner for the object and remove it
        owner = self._getHierarchy(dirpath(art.path))
        owner.remove(art)

        if isinstance(art, Directory):

            # Delete all contents at that location - to be here with items recursive must have been true
            for obj in self._bucket.objects.filter(Prefix=toAWSPath(art.path)):
                obj.delete()

            # run the deletion method for self + all children
            toDelete = art.ls()
            while toDelete:
                # NOTE as the parent is being detached and so are the parents - the GC can clean up
                # No need to remove each artefact from their parent.
                a = toDelete.pop()

                if isinstance(a, Directory): toDelete += a.ls()

                # Trigger the artefact to no longer exist
                a._exists = False

                # Remove the artefact from the internal store
                del self._paths[a.path]

        else:
            # The item to delete is a file

            # Get the object from AWS and delete it
            awsObj = self._bucket.Object(toAWSPath(art.path))
            awsObj.delete()

        # Set the artefact to no longer exist and remove from the internal store
        art._exists = False
        del self._paths[art.path]

    def mkdir(self, path):
        with tempfile.TemporaryDirectory() as directory:

            fp = os.path.join(directory, PLACEHOLDER)

            open(fp, 'w').close()

            self._bucket.upload_file(Filename=fp, Key=toAWSPath(path + '/' + PLACEHOLDER))

        self.refresh(path)

