import collections
import datetime
import tempfile
import json
import uuid

from .interfaces import Manager
from .artefacts import File

import logging
log = logging.getLogger(__name__)

class Change:
    """ Representation of a single change """

    DELETE = 'DELETE'
    PULL = "PULL"

    def __init__(self, target: Manager, filename: str, change_type: str):
        """ A representation of a single change """
        self.target = target
        self.filename = filename
        self.change_type = change_type

class Conflict:

    NEW = 'NEW'
    UPDATED = 'UPDATED'
    DELETED = 'DELETED'

    def __init__(self, filename: str, manager1_type: str, manager2_type: str):
        pass

class StagedChanges:
    """ Changes that are going would be made between to managers to synchronise them """

    def __init__(self, manager1: Manager, manager2: Manager):
        self.manager1 = manager1
        self.manager2 = manager2

        self._changes = []
        self._conflicts = []

    def __iter__(self): return iter(self._changes)

    def addChange(self, change: Change):
        """ """
        self._changes.append(change)

    def addConflict(self, conflict: Conflict):
        """ Record a conflict """
        self._conflicts.append(conflict)

    def hasConflicts(self): return bool(len(self._conflicts))


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

            self._meta = {
                self._manager1: meta1,
                self._manager2: meta2
            }

            self._expectations = meta1.expectations[meta2.machineId].union(meta2.expectations[meta1.machineId])

        except:
            if not force: raise

            log.warning("Managers being forced to sync - they were apart of different networks")
            meta1.groupId = meta2.groupId = uuid.uuid4()
            meta1.synctime = meta2.synctime = datetime.datetime.fromtimestamp(0000000000)

    def calculateChanges(self):

        # Unpack the manager files and the expected files
        m1 = set(self._manager1.paths(File).keys())
        m2 = set(self._manager2.paths(File).keys())
        expected = self._expectations

        changes = StagedChanges(self._manager1, self._manager2)

        # Identify the newly added files on either machine (won't exist in expected or on the other machine)
        for a, b, bm in [(m1, m2, self._manager2), (m2, m1, self._manager1)]:
            for filename in a.difference(expected).difference(b):
                changes.addChange(Change(bm, filename, Change.PULL))

        # File added to both
        for filename in m1.intersection(m2).difference(expected):
            changes.addConflict(Conflict(filename, Conflict.NEW, Conflict.NEW))

        # Identify the files that need to be deleted/conflict as one has updated and one has been deleted
        for a, am, b in [(m1, self._manager1, m2), (m2, self._manager2, m1)]:
            for filename in a.intersection(expected).difference(b):

                if am[filename].motifiedTime <= self._meta[am].synctime:
                    changes.addChange(Change(am, filename, Change.DELETE))

                else:
                    changes.addConflict(Conflict(filename, Conflict.DELETED, Conflict.DELETED))

        # Resolve differences when the files exists on both
        for filename in m1.intersection(m2).intersection(expected):

            # Unpack the files from the managers
            af, bf  =  self._manager1[filename], self._manager2[filename]

            t1 = af.modifiedTime <= self._meta[self._manager1].synctime
            t2 = bf.modifiedTime <= self._meta[self._manager2].synctime

            if (not t1) and (not t2):
                # Both have been updated
                changes.addConflict(Conflict(filename, Conflict.UPDATED, Conflict.UPDATED))

            elif t1:
                # Manager 1 hasn't been changed
                changes.addChange(Change(self._manager1, filename, Change.PULL))

            elif t2:
                # Manager 2 hasn't been changed
                changes.addChange(Change(self._manager2, filename, Change.PULL))

        # Return the stages changes
        return changes

    def applyChanges(self, changes: StagedChanges):

        if changes.hasConflicts():
            raise RuntimeError('Cannot perform sync when there are conflicts on files.')

        for change in changes:

            # Identify the opposite manager
            oppositeManager  = self._manager2 if change.target == self._manager1 else self._manager1

            if change.change_type == change.PULL:
                change.target.put(oppositeManager[change.filename], change.filename)

            elif change.change_type == change.DELETE:
                change.target.rm(change.filename)

    def sync(self):
        changes = self.calculateChanges()

        # Apply any conflict policy

        self.applyChanges(changes)