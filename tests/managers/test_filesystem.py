import unittest

import os
import tempfile
import shutil

from .manager import ManagerTests

import storage

class Test_Filesystem(unittest.TestCase, ManagerTests):

    def setUp(self):
        # Make the managers local space to store files
        self.directory = tempfile.mkdtemp()

        # Define the manager
        self.manager = storage.connect(manager='FS', path=self.directory)

    def tearDown(self):

        # Delete the directory and all it's contents
        shutil.rmtree(self.directory)

    def test_abspath(self):

        # Make a file
        with self.manager.open('/directory/file.txt', 'w') as handle:
            handle.write('content')

        file = self.manager['/directory/file.txt']

        # Assert that it's full path
        self.assertEqual(
            os.path.join(self.directory, "directory", "file.txt"),
            self.manager._abspath(file)
        )

        self.assertEqual(open(os.path.join(self.directory, "directory", "file.txt"), "r").read(), "content")

    def test_dirname(self):
        pass

    def test_basename(self):
        pass

    def test_relname(self):
        pass




class Test_Locals(unittest.TestCase):

    def setUp(self):
        # Make the managers local space to store files
        self.directory = tempfile.mkdtemp()

        directories = []
        for name in ['dir1', 'dir2']:
            path = os.path.join(self.directory, name)
            os.mkdir(path)
            directories.append(path)

        # Define the manager
        self.manager = storage.connect(manager='Locals', directories=directories)

    def tearDown(self):

        # Delete the directory and all it's contents
        shutil.rmtree(self.directory)

    def test_put_get(self):

        with tempfile.TemporaryDirectory() as directory:
            file = self.manager.touch('/dir1/file1.txt')

            with file.open('w') as handle:
                handle.write('some content')

            local_path = os.path.join(directory, 'temp')
            self.manager.get('/dir1/file1.txt', local_path)

            with open(local_path, 'r') as handle:
                self.assertEqual(handle.read(), 'some content')

    def test_ls(self):

        pass

