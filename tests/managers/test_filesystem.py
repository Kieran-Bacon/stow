import unittest

import os
import tempfile
import shutil
import random
import string
import time

import stow
from stow.managers import FS

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

        self.assertEqual(self.manager.toConfig(), {"manager": "FS", "path": self.directory})

    def test_speed(self):

        with tempfile.TemporaryDirectory() as directory:

            targets = ['{}.txt'.format(i) for i in range(100000)]

            for t in targets:
                open(os.path.join(directory, t), 'w').close()



            start = time.time()
            for t in targets:
                os.path.exists(os.path.join(directory, t))
            rawTotal = time.time() - start

            directoryArtefact = stow.artefact(directory)

            start = time.time()
            for t in targets:
                t in directoryArtefact

            stowTotal = time.time() - start

            self.assertAlmostEqual(rawTotal, stowTotal)


