import unittest

import os
import tempfile
import shutil

from .manager import ManagerTests

import warehouse

class Test_SubManager(unittest.TestCase, ManagerTests):

    def setUp(self):
        # Make the managers local space to store files
        self.directory = tempfile.mkdtemp()

        # Define the manager
        self.mainManager = warehouse.connect(manager='FS', path=self.directory)
        self.mainManager.mkdir("/demo")
        self.manager = self.mainManager.submanager("/demo")

    def setUpWithFiles(self):
        # Make the managers local space to store files
        self.directory = tempfile.mkdtemp()

        os.mkdir(os.path.join(self.directory, "demo"))

        with open(os.path.join(self.directory, "demo", "initial_file1.txt"), "w") as handle:
            handle.write("Content")

        os.mkdir(os.path.join(self.directory, "demo", "initial_directory"))
        with open(os.path.join(self.directory, "demo", "initial_directory", "initial_file2.txt"), "w") as handle:
            handle.write("Content")

        os.mkdir(os.path.join(self.directory, "demo", "directory-stack"))
        os.mkdir(os.path.join(self.directory, "demo", "directory-stack", "directory-stack"))
        with open(os.path.join(self.directory, "demo", "directory-stack", "directory-stack", "initial_file3.txt"), "w") as handle:
            handle.write("Content")

        # Define the manager
        self.mainManager = warehouse.connect(manager='FS', path=self.directory)
        self.manager = self.mainManager.submanager("/demo")

    def tearDown(self):

        # Delete the directory and all it's contents
        shutil.rmtree(self.directory)

    def test_put_file(self):

        with tempfile.TemporaryDirectory() as directory:

            tempfilename = os.path.join(directory, "temp.txt")
            with open(tempfilename, "w") as handle:
                handle.write("content")

            subFile = self.manager.put(tempfilename, "/file1.txt")
            mainFile = self.mainManager['/demo/file1.txt']

            self.assertTrue(mainFile)
            self.assertEqual(mainFile.modifiedTime, subFile.modifiedTime)
            self.assertEqual(mainFile.size, subFile.size)

    def test_put_directory(self):

        with tempfile.TemporaryDirectory() as directory:

            for path in [("dir1", "file1.txt"), ("dir1", "dir2", "file2.txt")]:
                fp = os.path.join(directory, *path)
                os.makedirs(os.path.dirname(fp))
                with open(fp, "w") as handle:
                    handle.write("some content")


            self.manager.put(os.path.join(directory, "dir1"), "/dir1")

            # Test all of the items
            for filename in ["/dir1/file1.txt", "/dir1/dir2/file2.txt"]:
                sub = self.manager[filename]
                main = self.mainManager["/demo"+filename]

                self.assertEqual(main.modifiedTime, sub.modifiedTime)
                self.assertEqual(main.size, sub.size)
