import unittest

import tempfile
import shutil

from .manager import ManagerTests

import storage

class Test_Filesystem(unittest.TestCase, ManagerTests):

    def setUp(self):
        # Make the managers local space to store files
        self.directory = tempfile.mkdtemp()

        # Define the manager
        self.manager = storage.connect('test', manager='FS', path=self.directory)

    def tearDown(self):

        # Delete the directory and all it's contents
        shutil.rmtree(self.directory)