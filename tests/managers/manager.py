import unittest
import pytest

import os
import tempfile
import shutil

import storage

class ManagerTests:

    def setUp(self):
        self.manager = storage.interfaces.Manager

    def test_put_and_get(self):

        with tempfile.TemporaryDirectory() as directory:

            localInFP = os.path.join(directory, 'in.txt')
            localOutFP = os.path.join(directory, 'out.txt')

            content = 'here are some lines'

            # Create a file to be put into the manager
            with open(localInFP, 'w') as fh:
                fh.write(content)

            # Put the file onto the server
            file = self.manager.put(localInFP, '/test1.txt')

            # Assert that the pushed item is a file
            self.assertIsInstance(file, storage.artefacts.File)

            # Pull the file down again
            self.manager.get('/test1.txt', localOutFP)

            with open(localOutFP, 'r') as fh:
                self.assertEqual(fh.read(), content)

    def test_put_and_get_with_artefacts(self):

        with tempfile.TemporaryDirectory() as directory:

            localInFP = os.path.join(directory, 'in.txt')
            localOutFP = os.path.join(directory, 'out.txt')

            content = 'here are some lines'

            # Create a file to be put into the manager
            with open(localInFP, 'w') as fh:
                fh.write(content)

            # Create a file on the manager
            file = self.manager.touch('/test1.txt')

            # Put the local file onto, using the file object
            file_b = self.manager.put(localInFP, file)

            # Assert its a file and that its the same file object as before
            self.assertIsInstance(file_b, storage.artefacts.File)
            self.assertIs(file, file_b)

            # Pull the file down again - using the file object
            self.manager.get(file, localOutFP)

            with open(localOutFP, 'r') as fh:
                self.assertEqual(fh.read(), content)

    def test_put_and_get_with_directories(self):

        with tempfile.TemporaryDirectory() as directory:

            # Make a directory of files and sub-files
            d = os.path.join(directory, 'testdir')

            os.mkdir(d)

            with open(os.path.join(d, 'test1.txt'), 'w') as fh:
                fh.write('1')

            # Sub directory
            dSub = os.path.join(d, 'subdir')

            os.mkdir(dSub)

            with open(os.path.join(dSub, 'test2.txt'), 'w') as fh:
                fh.write('2')

            art = self.manager.put(d, '/testdir')

            self.assertIsInstance(art, storage.artefacts.Directory)

    def test_ls(self):
        pass

    def test_mv(self):
        pass

    def test_rm(self):

        with tempfile.TemporaryDirectory() as directory:

            # Delete a file
            # Delete a directory
            # Fail to delete a directory with contents
            # Delete an full directory

            # Create a file on the manager
            self.manager.touch('/file1.txt')

            # Demonstrate that the file can be collected/played with
            file = self.manager['/file1.txt']
            self.assertTrue(file._exists)
            self.manager.get('/file1.txt', os.path.join(directory, 'temp.txt'))
            os.stat(os.path.join(directory, 'temp.txt'))

            # Delete the file
            self.manager.rm('/file1.txt')

            # Demonstrate that the file has been removed from the manager
            with pytest.raises(KeyError):
                self.manager['/file1.txt']

            self.assertFalse(file._exists)

            with pytest.raises(FileNotFoundError):
                self.manager.get('/file1.txt', os.path.join(directory, 'temp.txt'))
                os.stat(os.path.join(directory, 'temp.txt'))

            self.manager.mkdir('/directory')
            self.manager.mkdir('/directory2')
            self.manager.touch('/directory2/file1.txt')



