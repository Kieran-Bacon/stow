import os
import tempfile
from tqdm import tqdm
import better
import datetime

from . import sep
from .interfaces import Manager
from .artefacts import File
from .utils import connect

class Conflict:
    """ Record of conflict between local and remote artefacts. Holds state about how the conflict has arisen to be
    able to resolve at a later point

    Params:
        filepath (str): Relative filepath for the artefact
        local (int): State of the local file - Created/Updated/Deleted
        remote (int): State of the remote file - Created/Updated/Deleted
    """

    def __init__(self, filepath: str, local: int, remote: int):
        self.file = filepath
        self.local = local
        self.remote = remote

    def __str__(self): return "File '{}' - local = {}, remote = {} ".format(self.file, self.local, self.remote)

class Sync:
    CREATED = "CREATED"
    UPDATED = "UPDATED"
    DELETED = "DELETED"

    CONFLICT = 'CONFLICT'
    TRUST_LOCAL = 'LOCAL'
    TRUST_REMOTE = 'REMOTE'
    STOP_EXECUTION = 'STOP'

    def __init__(self,
        local: Manager,
        remote: Manager,
        *,
        tracked_timestamp = 0000000000,
        tracked = set(),
        filters: [str] = [],
        conflict_policy: str = CONFLICT
        ):

        # Record the managers
        self._local = local
        self._remote = remote
        self._trackedTime = datetime.datetime.fromtimestamp(tracked_timestamp)
        self._tracked = tracked
        self._conflictPolicy = conflict_policy

        self._filters = filters

    def upload(self, filenames: [str]):
        """ Upload a file to the remote manager """

        for filename in tqdm(filenames):
            # Getting the file object from local, put the file object on remote at the filename location
            self._remote.put(self._local[filename], filename)


    def download(self, filenames: [str]):
        """ Replace local files/download files from the remote """

        for filename in tqdm(filenames):

            self._local.put(self._remote[filename], filename)

    def delete(self, filenames: [str]):
        for filename in filenames:
            if filename in self._local: self._local.rm(filename)
            else: self._remote.rm(filename)

    def conflict(self, conflicts: [Conflict]):
        if self._conflictPolicy == self.STOP_EXECUTION:
            raise RuntimeError("Conflicts occurred on files: \n\t{}".format('\n\t'.format([str(x) for x in conflicts])))

        upload, download, delete = set(), set(), set()

        for conflict in conflicts:

            policy = self._conflictPolicy

            if self._conflictPolicy == self.CONFLICT:

                while True:

                    # Render the conflict and await for a resolution to be given
                    print(str(conflict))

                    # Ask the user to chose a resolution
                    choice = input('Choice local or remote (local, remote): ').strip().lower()

                    # Check that the choice is valid
                    if choice not in ['local', 'remote']:
                        print("Not a valid selection")
                        continue

                    policy = self.TRUST_LOCAL if choice == 'local' else self.TRUST_REMOTE
                    break


            if policy == self.TRUST_LOCAL:

                if conflict.local == self.UPDATED:
                    upload.add(conflict.file)

                else:
                    delete.add(conflict.file)

            else:

                if conflicts.remote == self.UPDATED:
                    download.add(conflicts.file)

                else:
                    delete.add(conflict.file)

        if upload:self.upload(upload)
        if download: self.download(download)
        if delete: self.delete(delete)

    def sync(self):

        # Extract the filenames for the files to be synced
        local = set(self._local.paths(File).keys())
        remote = set(self._remote.paths(File).keys())
        expected = set(self._tracked)

        # Define containers to hold the ultimate decisions for the files
        uploadable, downloadable, deletable, conflicts = set(), set(), set(), set()

        # 1 New files made locally
        for file in local.difference(expected).difference(remote):
            uploadable.add(file)

        # 6 Files on the remote, not to download
        for file in remote.difference(expected).difference(local):
            downloadable.add(file)

        # 4 - drop none expected things - hopefully this just happens - As the file is no longer on the local or remote

        # 7 The file is new locally and a version exists on the remote - conflict
        for file in local.intersection(remote).difference(expected):
            conflicts.add(Conflict(file, local=self.CREATED, remote=self.CREATED))

        # 2 The file was expected to be on the remote but it wasn't
        for file in local.intersection(expected).difference(remote):

            localFile = self._local[file]

            if localFile.modifiedTime <= self._trackedTime:
                # The local file is the same as the expected - absent from the remote means it was deleted by another
                deletable.add(file)

            else:
                # The local file has been editted and deleted on the remote - a conflict has occurred since last syncing
                conflicts.add(Conflict(file, local=self.UPDATED, remote=self.DELETED))

        # 5
        for file in remote.intersection(expected).difference(local):
            # Exists on remote but deleted locally

            remoteFile = self._remote[file]
            if remoteFile.modifiedTime <= self._trackedTime:
                # The file was deleted locally and not changed since last sync on the remote - delete the remote
                deletable.add(file)

            else:
                # Updated on the remote before the deleted file was pushed
                conflicts.add(Conflict(file, local=self.DELETED, remote=self.UPDATED))

        # 3
        for file in local.intersection(expected).intersection(remote):
            # When the file exists everywhere

            localFile = self._local[file]
            remoteFile = self._remote[file]

            if localFile.modifiedTime <= self._trackedTime and remoteFile.modifiedTime <= self._trackedTime:
                # No change has occurred
                continue

            elif localFile.modifiedTime <= self._trackedTime:
                # The local file has not been changed
                downloadable.add(file)

            elif remoteFile.modifiedTime <= self._trackedTime:
                # The remote has not changed, local changes to be pushed up
                uploadable.add(file)

            else:
                # Both the local and remote has changed - conflict!
                conflicts.add(Conflict(file, local=self.UPDATED, remote=self.UPDATED))

        if uploadable: self.upload(uploadable)
        if downloadable: self.download(downloadable)
        if deletable: self.delete(deletable)
        if conflicts: self.conflict(conflicts)

        self._tracked = uploadable.union(downloadable).union(expected).difference(deletable)
        self._trackedTime = datetime.datetime.now()

    def toConfig(self):
        return {
            "container1": self._local.toConfig(),
            "container2": self._remote.toConfig(),
            "tracked": {
                "tracked_timestamp": self._trackedTime.timestamp(),
                "tracked": self._tracked
            },

            "options": {
                "conflict_policy": self._conflictPolicy
            }
        }
