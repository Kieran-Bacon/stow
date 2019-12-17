import collections
import datetime
import tempfile
import json
import uuid

from .interfaces import Manager

import logging
log = logging.getLogger(__name__)

class Change:
    """ Representation of a single change """

    pass

class StagedChanges:
    """ Changes that are going would be made between to managers to synchronise them """

    def __init__(self):
        pass

class SyncMeta:

    def __init__(self, machineId: str, groupId, synctime: datetime.datetime, expectations: {str: {str}}):

        self.machineId = machineId
        self.groupId = groupId
        self.synctime = synctime
        self.expectations = expectations

    @classmethod
    def emptyMeta(cls):
        return cls(uuid.uuid4(), None, datetime.datetime.fromtimestamp(0000000000), {})

    @classmethod
    def read(cls, handle):
        data = json.load(handle)
        return cls(
            data['mid'],
            data['gid'],
            datetime.datetime.fromtimestamp(data['time']),

            data['exp']
        )

    def write(self, handle):
        json.dump({
            'mid': self.machineId,
            'gid': self.groupId,
            'time': self.synctime.timestamp(),
            'exp': self.expectations
        }, handle)

    def compare(self, other):
        """ Compare/make the the same the myself with the other meta """

        # Ensure that the two meta classes are apart of the same network and ensure that the meta has expectations for
        # the other meta

        if self.machineId not in other.expectations: other.expectations[self.machineId] = set()
        if other.machineId not in self.expectations: self.expectations[other.machineId] = set()

        if self.groupId is None and other.groupId is None:
            self.groupId = other.groupId = uuid.uuid4()
        elif self.groupId is None:
            self.groupId = other.groupId
        elif other.groupId is None:
            other.groupId = self.groupId
        else:
            if self.groupId != other.groupId:
                raise RuntimeError(
                    "Managers have inconsistent metadata. The meta data has either been editted or the managers "
                    "are apart of other syncing processes which this action would break. If this is an intended "
                    "action, pass 'force=True'"
                )

class Sync:

    _META_PATH = '/.__syncdata__'

    def __init__(self, manager1: Manager, manager2: Manager, force: bool = False):

        self._manager1 = manager1
        self._manager2 = manager2

        # Load the meta for the first manager
        if self._META_PATH in manager1:
            with manager1[self._META_PATH].open('r') as handle:
                meta1 = SyncMeta.read(handle)

        else:
            meta1 = SyncMeta.emptyMeta()


        if self._META_PATH in manager2:
            with manager2[self._META_PATH].open('r') as handle:
                meta2 = SyncMeta.read(handle)

        else:
            meta2 = SyncMeta.emptyMeta()

        # Connect/ensure the meta data for the two managers
        try:
            meta1.compare(meta2)
        except:
            if not force: raise

            log.warning("Managers being forced to sync - they were apart of different networks")
            meta1.groupId = meta2.groupId = uuid.uuid4()
            meta1.synctime = meta2.synctime = datetime.datetime.fromtimestamp(0000000000)

    def sync(self):

        changes = StagedChanges(self._manager1, self._manager2)

        for filename in

        # Relation tabs -
        self._syncTo(self._manager1, self._manager2)
        self._syncTo(self._manager2, self._manager1)


    def _syncTo(self, mFrom, mTo):

        origin, expected, destination = set(), set(), set()

        push, pull, delete, conflict = set(), set(), set(), set()

        for filename in origin.difference(expected).difference(destination):
            push.add(filename)

        for filename in local




        # Perform the synchronisation
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


