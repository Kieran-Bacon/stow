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

        self.filename = filename
        self.manager1_type = manager1_type
        self.manager2_type = manager2_type

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

    def conflicts(self): return self._conflicts

    def removeConflict(self, conflict: Conflict):
        self._conflicts.remove(conflict)

    def hasConflicts(self): return bool(len(self._conflicts))


class SyncMeta:

    def __init__(self, machineId: str, groupId, synctime: datetime.datetime, expectations: {str: {str}}):

        self.machineId = machineId
        self.groupId = groupId
        self.synctime = synctime
        self.expectations = expectations

    @classmethod
    def emptyMeta(cls):
        return cls(str(uuid.uuid4()), None, datetime.datetime.fromtimestamp(0000000000), {})

    @classmethod
    def read(cls, handle):
        data = json.load(handle)
        return cls(
            data['mid'],
            data['gid'],
            datetime.datetime.fromtimestamp(data['time']),

            {k: set(v) for k, v in data['exp']}
        )

    def write(self, handle):
        json.dump({
            'mid': self.machineId,
            'gid': self.groupId,
            'time': self.synctime.timestamp(),
            'exp': {k: list(v) for k, v in self.expectations.items()}
        }, handle)

    def compare(self, other):
        """ Compare/make the the same the myself with the other meta """

        # Ensure that the two meta classes are apart of the same network and ensure that the meta has expectations for
        # the other meta

        if self.machineId not in other.expectations: other.expectations[self.machineId] = set()
        if other.machineId not in self.expectations: self.expectations[other.machineId] = set()

        if self.groupId is None and other.groupId is None:
            self.groupId = other.groupId = str(uuid.uuid4())
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

    CONFLICT = 'CONFLICT'
    ACCEPT_1 = 'ACCEPT_1'
    ACCEPT_2 = 'ACCEPT_2'
    STOP_EXECUTION = 'STOP'

    def __init__(self, manager1: Manager, manager2: Manager, *, force: bool = False, conflictPolicy: str = 'CONFLICT'):

        self._manager1 = manager1
        self._manager2 = manager2
        self._conflictPolicy = conflictPolicy

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

            self._meta1 = meta1
            self._meta2 = meta2
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

        # Remove the meta path from consideration
        m1.discard(self._META_PATH)
        m2.discard(self._META_PATH)

        # Create the stages changes container
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

                if am[filename].modifiedTime <= self._meta[am].synctime:
                    changes.addChange(Change(am, filename, Change.DELETE))

                else:
                    changes.addConflict(Conflict(filename, Conflict.DELETED, Conflict.DELETED))

        # Resolve differences when the files exists on both
        for filename in m1.intersection(m2).intersection(expected):

            # Unpack the files from the managers
            af, bf  =  self._manager1[filename], self._manager2[filename]

            t1 = af.modifiedTime <= self._meta1.synctime
            t2 = bf.modifiedTime <= self._meta2.synctime

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

        # Calculate the new expectations of the two
        synctime = datetime.datetime.now()
        self._expectations = set(self._manager1.paths(File).keys()).union(self._manager2.paths(File).keys()).union(self._expectations)

        self._meta1.synctime = synctime
        self._meta1.expectations[self._meta2.machineId] = self._expectations
        self._meta2.synctime = synctime
        self._meta2.expectations[self._meta1.machineId] = self._expectations

        # Create a new meta files on the managers and write the meta data back to the manager
        metapath1 = self._manager1.touch(self._META_PATH)
        with metapath1.open('w') as fh:
            self._meta1.write(fh)

        metapath2 = self._manager2.touch(self._META_PATH)
        with metapath2.open('w') as fh:
            self._meta2.write(fh)

    def sync(self):
        changes = self.calculateChanges()

        # Resolve the conflicts in the staged changes
        for conflict in changes.conflicts():

            if self._conflictPolicy == self.STOP_EXECUTION:
                raise ValueError("Sync has conflicts - policy requires execution to stop")

            policy = self._conflictPolicy

            if self._conflictPolicy == self.CONFLICT:

                while True:

                    # Render the conflict and await for a resolution to be given
                    print(str(conflict))

                    # Ask the user to chose a resolution
                    choice = input('which change should be accepted? (1, 2): ').strip().lower()

                    # Check that the choice is valid
                    if choice not in ['1', '2']:
                        print("Not a valid selection")
                        continue

                    policy = self.ACCEPT_1 if choice == '1' else self.ACCEPT_2
                    break

            if policy == self.ACCEPT_1:
                target, methodType = self._manager2, conflict.manager1_type
            
            else:
                target, methodType = self._manager1, conflict.manager2_type

            # Select the method of the conflict
            method = Change.PULL if methodType in ['NEW', 'UPDATED'] else Change.DELETE

            # Create the change for the conflict
            changes.addChange(Change(target, conflict.filename, method))

            # Remove the conflict from the stages changes
            changes.removeConflict(conflict)

        # Apply any conflict policy
        self.applyChanges(changes)
