import unittest
import pytest

import os
import tempfile
import shutil

import storage

class ManagerTests:

    def setUp(self):
        self.manager = storage.interfaces.Manager

    def test_mkdir(self):

        self.manager.mkdir('/directory')

        self.assertEqual(len(self.manager.ls(recursive=True)), 1)

        directory = self.manager['/directory']
        self.assertIsInstance(directory, storage.Directory)
        self.assertTrue(len(directory) == 0)

    def test_touch(self):

        self.manager.touch('/file1.txt')
        self.manager.touch('/directory/file2.txt')

        self.manager.mkdir('/otherdir')
        self.manager.touch('/otherdir/file3.txt')

        self.assertEqual(len(self.manager.ls(recursive=True)), 5)
        self.assertIn(self.manager['/directory/file2.txt'], self.manager['/directory'])
        self.assertIn(self.manager['/otherdir/file3.txt'], self.manager['/otherdir'])

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

    def test_putting_directories_overwrites(self):

        with tempfile.TemporaryDirectory() as directory:

            # Create a directory and a file on the manager
            self.manager.touch('/directory/file1.txt')

            # Create a local directory and similar file
            path = os.path.join(directory, 'directory')
            os.mkdir(path)
            open(os.path.join(path, 'file2.txt'), 'w').close()

            # Put the local directory into the machine, ensure that its overwritten
            self.manager.put(path, '/directory')

            folder = self.manager['/directory']

            self.assertEqual(len(folder), 1)

            with pytest.raises(KeyError):
                self.manager['/directory/file1.txt']

            self.manager['/directory/file2.txt']

    def test_ls(self):
        """ Create a hierarchy of files and show that listing the

        A
        B c.txt C
        d.txt e.txt |

        """

        # Create the filesystem
        self.manager.touch('/A/c.txt')
        self.manager.touch('/A/B/d.txt')
        self.manager.touch('/A/B/e.txt')
        self.manager.mkdir('/A/C')

        # Assert top level
        self.assertEqual(self.manager.ls(), {self.manager['/A']})
        self.assertEqual(self.manager.ls(), self.manager['/'].ls())

        # Assert Next level
        self.assertEqual(self.manager['/A'].ls(), {self.manager[x] for x in ['/A/c.txt', '/A/B', '/A/C']})

        # Assert Next level
        self.assertEqual(self.manager['/A/B'].ls(), {self.manager[x] for x in ['/A/B/d.txt', '/A/B/e.txt']})
        self.assertEqual(self.manager['/A/C'].ls(), set())

        # Assert the recursive function
        objects = set(self.manager.paths().values()).difference({self.manager['/']})
        self.assertEqual(self.manager.ls(recursive=True), objects)
        self.assertEqual(self.manager['/'].ls(recursive=True), objects)

    def test_mv(self):

        content = 'Here is some file content to be verified'

        with tempfile.TemporaryDirectory() as directory:
            # Create a file on the manager
            file_one = self.manager.touch('/file1.txt')
            file_two = self.manager.touch('/file2.txt')

            with file_one.open('w') as fh: fh.write(content)
            with file_two.open('w') as fh: fh.write(content)

            # Assert that the file exists
            self.assertTrue(self.manager['/file1.txt'])
            self.assertTrue(self.manager['/file2.txt'])

            with pytest.raises(KeyError):
                self.manager['/file3.txt']

            with pytest.raises(KeyError):
                self.manager['/file4.txt']

            # Move the file
            self.manager.mv('/file1.txt', '/file3.txt')
            self.manager.mv(file_two, '/file4.txt')

            # Assert that the file exists
            self.assertTrue(self.manager['/file3.txt'])
            self.assertTrue(self.manager['/file4.txt'])
            with pytest.raises(KeyError): self.manager['/file1.txt']
            with pytest.raises(KeyError): self.manager['/file2.txt']

            # Open the new file and assert that its content matches
            path = os.path.join(directory, 'file.txt')
            self.manager.get('/file3.txt', path)
            with open(path, 'r') as handle:
                self.assertEqual(handle.read(), content)

            self.manager.get('/file4.txt', path)
            with open(path, 'r') as handle:
                self.assertEqual(handle.read(), content)


    def test_rm_file(self):

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


    def test_rm_empty_directory(self):

        # Make an empty directory to delete
        self.manager.mkdir('/directory')

        tempDir = self.manager['/directory']
        self.assertTrue(tempfile._exists)

        # Delete the directory
        self.manager.rm('/directory')

        self.assertFalse(tempDir._exists)


    def test_rm_non_empty_directory(self):

        with tempfile.TemporaryDirectory() as directory:

            # Make a directory and some content
            self.manager.mkdir('/directory')
            self.manager.touch('/directory/file1.txt')

            # Get the two items
            folder = self.manager['/directory']
            file = self.manager['/directory/file1.txt']

            # Ensure that they exist
            for i, (art, method) in enumerate([(folder, os.path.isdir), (file, os.path.isfile)]):
                self.assertTrue(art._exists)

                local_path = os.path.join(directory, str(i))

                self.manager.get(art, local_path)

                self.assertTrue(os.path.exists(local_path))
                self.assertTrue(method(local_path))

            # Ensure that one cannot delete the directory while it still has contents
            with pytest.raises(storage.interfaces.Exceptions.OperationNotPermitted):
                self.manager.rm(folder)

            # Remove recursively
            self.manager.rm(folder, True)

            # Assert that the items are not removed
            # Ensure that they exist
            for art in [folder, file]:
                self.assertFalse(art._exists)

                with pytest.raises(KeyError):
                    self.manager[art.__dict__['path']]