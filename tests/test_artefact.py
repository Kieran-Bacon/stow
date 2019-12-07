
import unittest
from unittest.mock import patch, MagicMock

import os
import tempfile
import datetime
import shutil

import storage

class Test_Files(unittest.TestCase):

    def setUp(self):

        self.directory = tempfile.mkdtemp()

        self.filepath = os.path.join(self.directory, 'file1')
        self.filetext = 'Another one bits the dust'
        with open(self.filepath, 'w') as handle:
            handle.write(self.filetext)

        self.manager = storage.connect('name', manager='FS', path=self.directory)

    def tearDown(self):
        shutil.rmtree(self.directory)

    def test_opening(self):

        file = self.manager.ls()[0]

        with file.open('r') as fh:
            self.assertEqual(fh.read(), self.filetext)

        with file.open('a') as fh:
            pass

        with open(self.filepath, 'r') as fh:
            self.assertEqual(fh.read(), self.filetext)

        text = "Something else"
        with file.open('w') as fh:
            fh.write(text)

        with open(self.filepath, 'r') as fh:
            self.assertEqual(fh.read(), text)

class Test_Directories(unittest.TestCase):

    def setUp(self):

        self.directory = tempfile.mkdtemp()
        os.mkdir('dir1')

        self.filepath = os.path.join(self.directory, 'dir1', 'file1')
        self.filetext = 'Another one bits the dust'
        with open(self.filepath, 'w') as handle:
            handle.write(self.filetext)

        self.manager = storage.connect('name', manager='FS', path=self.directory)

    def tearDown(self):
        shutil.rmtree(self.directory)

    def test_removal(self):

        directory = self.manager.ls()[0]

        self.assertIsInstance(directory, storage.Directory)

        directory.rm('/file1')