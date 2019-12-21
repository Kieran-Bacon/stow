import unittest

import tempfile
import shutil

from .manager import ManagerTests

import storage

class Test_Filesystem(unittest.TestCase, ManagerTests):

    def setUp(self):

        # Create a temporary directory for the testing to commence in
        self.directory = tempfile.mkdtemp()

        # Define the manager
        self.manager = storage.connect('test', manager='FS', path=self.directory)


    def tearDown(self):

        # Delete the directory and all it's contents
        shutil.rmtree(self.directory)