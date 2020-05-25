import unittest

import os
import tempfile
import shutil

import warehouse
from warehouse.managers import FS

from .manager import ManagerTests, SubManagerTests

class Test_Filesystem(unittest.TestCase, ManagerTests, SubManagerTests):

    def setUp(self):
        # Make the managers local space to store files
        self.directory = tempfile.mkdtemp()

        # Define the manager
        self.manager = FS(path=self.directory)

    def setUpWithFiles(self):
        # Make the managers local space to store files
        self.directory = tempfile.mkdtemp()

        with open(os.path.join(self.directory, "initial_file1.txt"), "w") as handle:
            handle.write("Content")

        os.mkdir(os.path.join(self.directory, "initial_directory"))
        with open(os.path.join(self.directory, "initial_directory", "initial_file2.txt"), "w") as handle:
            handle.write("Content")

        os.mkdir(os.path.join(self.directory, "directory-stack"))
        os.mkdir(os.path.join(self.directory, "directory-stack", "directory-stack"))
        with open(os.path.join(self.directory, "directory-stack", "directory-stack", "initial_file3.txt"), "w") as handle:
            handle.write("Content")

        # Define the manager
        self.manager = FS(path=self.directory)

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


    def test_abspath(self):

        paths = [
            ('/hello/kieran', os.path.join(self.directory, 'hello/kieran')),
            ('/hello/kieran/', os.path.join(self.directory, 'hello/kieran')),
        ]


        for i, o in paths:
            self.assertEqual(self.manager.abspath(i), o)
    def test_relPath(self):

        paths = [
            ('/hello/kieran', '/hello/kieran'),
            (os.path.join(self.directory, 'hello/kieran'), '/hello/kieran'),
            ('/hello/kieran/', '/hello/kieran'),
            (r'\what\the\hell', '/what/the/hell'),
            (r'C:\\what\\the\\hell', '/what/the/hell'),
            ('s3://path/like/this', '/path/like/this')
        ]

        for i, o in paths:
            self.assertEqual(self.manager.relpath(i), o)
