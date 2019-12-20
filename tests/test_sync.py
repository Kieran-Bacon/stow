import unittest
import pytest

import os
import shutil
import tempfile

import storage

class Test_Syncing(unittest.TestCase):

    def setUp(self):

        # Create a large container for all the directories to exist in to check syncing
        self._container = tempfile.mkdtemp()

        # Define the starting directories
        self._localContainer = os.path.join(self._container, 'local')

        # Define the remote directories
        self._remoteContainer = os.path.join(self._container, 'remote')

        os.mkdir(self._localContainer)
        os.mkdir(self._remoteContainer)

        self.local = storage.connect('local', manager='FS', path=self._localContainer)
        self.remote = storage.connect('remote', manager='FS', path=self._remoteContainer)

    def tearDown(self):
        shutil.rmtree(self._container)

    def test_uploadOnly(self):

        # Create two files - one nested another surface level
        file1 = self.local.touch('/directory1/file1')
        file2 = self.local.touch('/file2')
        file3 = self.local.touch('/directory1/d2/d3/f3')

        # Write content to the files to check later
        with file1.open('w') as handle:
            handle.write('Content here')
        with file2.open('w') as handle:
            handle.write('Content there')
        with file3.open('w') as handle:
            handle.write('Content in the deep')

        # Setup a sync and synchronies
        storage.Sync(self.local, self.remote).sync()

        # Check that the files have been pushed to the remote container
        self.assertEqual(open(os.path.join(self._remoteContainer, 'directory1', 'file1'), 'r').read(), 'Content here')
        self.assertEqual(open(os.path.join(self._remoteContainer, 'file2'), 'r').read(), 'Content there')
        self.assertEqual(open(os.path.join(self._remoteContainer, 'directory1', 'd2','d3', 'f3'), 'r').read(), 'Content in the deep')

    def test_updatingLocallyandPushing(self):

        file1 = self.local.touch('/file1')
        file2 = self.local.touch('/directory1/file2')

        with file1.open('w') as fh: fh.write('Hello')
        with file2.open('w') as fh: fh.write('Maybe')

        s = storage.Sync(self.local, self.remote)
        s.sync()

        self.assertEqual(open(os.path.join(self._remoteContainer, 'file1'), 'r').read(), 'Hello')
        self.assertEqual(open(os.path.join(self._remoteContainer, 'directory1', 'file2'), 'r').read(), 'Maybe')

        with file1.open('a') as fh: fh.write(' there')
        with file2.open('a') as fh: fh.write(' it\'ll work')

        s.sync()

        self.assertEqual(open(os.path.join(self._remoteContainer, 'file1'), 'r').read(), 'Hello there')
        self.assertEqual(open(os.path.join(self._remoteContainer, 'directory1', 'file2'), 'r').read(), 'Maybe it\'ll work')

    def test_updatingLocallyRemotelyandPushing(self):

        file1 = self.local.touch('/file1')
        file2 = self.remote.touch('/file2')

        with file1.open('w') as fh: fh.write('Hello')
        with file2.open('w') as fh: fh.write('Maybe')

        s = storage.Sync(self.local, self.remote)
        s.sync()

        self.assertEqual(open(os.path.join(self._remoteContainer, 'file1'), 'r').read(), 'Hello')
        self.assertEqual(open(os.path.join(self._localContainer, 'file2'), 'r').read(), 'Maybe')

        with file1.open('a') as fh: fh.write(' there')
        with file2.open('a') as fh: fh.write(' it\'ll work')

        s.sync()

        self.assertEqual(open(os.path.join(self._remoteContainer, 'file1'), 'r').read(), 'Hello there')
        self.assertEqual(open(os.path.join(self._localContainer, 'file2'), 'r').read(), 'Maybe it\'ll work')

    def test_deleteingLocallyRemotelyandPushing(self):

        file1 = self.local.touch('/file1')
        file2 = self.remote.touch('/file2')

        with file1.open('w') as fh: fh.write('Hello')
        with file2.open('w') as fh: fh.write('Maybe')

        s = storage.Sync(self.local, self.remote)
        s.sync()

        self.assertEqual(open(os.path.join(self._remoteContainer, 'file1'), 'r').read(), 'Hello')
        self.assertEqual(open(os.path.join(self._localContainer, 'file2'), 'r').read(), 'Maybe')

        self.local.rm(file1)
        self.remote.rm(file2)

        s.sync()

        self.assertFalse(os.path.exists(os.path.join(self._remoteContainer, 'file1')))
        self.assertFalse(os.path.exists(os.path.join(self._localContainer, 'file2')))

    def test_conflictAccept1(self):

        # Enumerate files
        for i, m in enumerate([self.local, self.remote]):
            f = m.touch('/file1')
            with f.open('w') as fh: fh.write('Testing ' + str(i))

        # Create the syncing method
        s = storage.Sync(self.local, self.remote, conflictPolicy=storage.Sync.ACCEPT_1)
        s.sync()

        # Get the files
        f = self.remote['/file1']

        # Read the contents of the file to see that the conflict was resolved positively
        with f.open('r') as fh: self.assertEqual(fh.read(), 'Testing 0')

    def test_conflictAccept2(self):

        # Enumerate files
        for i, m in enumerate([self.local, self.remote]):
            f = m.touch('/file1')
            with f.open('w') as fh: fh.write('Testing ' + str(i))

        # Create the syncing method
        s = storage.Sync(self.local, self.remote, conflictPolicy=storage.Sync.ACCEPT_2)
        s.sync()

        # Get the files
        f = self.local['/file1']

        # Read the contents of the file to see that the conflict was resolved positively
        with f.open('r') as fh: self.assertEqual(fh.read(), 'Testing 1')

    def test_conflictStopExecution(self):

        # Enumerate files
        for i, m in enumerate([self.local, self.remote]):
            f = m.touch('/file1')
            with f.open('w') as fh: fh.write('Testing ' + str(i))

        # Create the syncing method
        s = storage.Sync(self.local, self.remote, conflictPolicy=storage.Sync.STOP_EXECUTION)

        # Check that the syncing process raises the correct error
        with pytest.raises(ValueError):
            s.sync()