import unittest

import os
import tempfile
import shutil
import random
import string
import time

import stow
from stow.managers import FS

from .manager import ManagerTests

class Test_Filesystem(unittest.TestCase, ManagerTests):

    def setUp(self):
        # Make the managers local space to store files
        self.directory = os.path.splitdrive(tempfile.mkdtemp())
        self.directory = self.directory[0].lower() + self.directory[1]

        # Define the manager
        self.manager = FS(path=self.directory)

    def setUpWithFiles(self):
        # Make the managers local space to store files
        self.directory = os.path.splitdrive(tempfile.mkdtemp())
        self.directory = self.directory[0].lower() + self.directory[1]


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

    def test_splitArtefactTypeError(self):
        with self.assertRaises(TypeError):
            self.manager.mklink(10, 'path')

    def test_relativePath(self):

        filename = "test-filename.txt"

        while os.path.exists(filename):
            filename = "".join([random.choice(string.ascii_letters) for _ in range(10)])

        #
        try:
            with open(filename, "w") as handle:
                handle.write("Content")

            file = stow.artefact(filename)

            self.assertEqual(file.content.decode(), "Content")

        finally:
            os.remove(filename)


    def test_config(self):

        config = self.manager.toConfig()

        if os.name == 'nt':
            self.assertEqual(config, {"manager": "FS", "path": self.directory, 'drive': 'c'})
        else:
            self.assertEqual(config, {"manager": "FS", "path": self.directory})

    # def test_otherDriveLetters(self):

    #     stow.rm("G:\\My Drive\\Pictures\\Cards\\18th Birthday from Linford and Bethan.pdf")
