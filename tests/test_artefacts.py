
import unittest
from unittest.mock import patch, MagicMock
import pytest

import os
import tempfile
import datetime
import shutil

import storage

class BasicSetup:

    def setUp(self):

        self.directory = tempfile.mkdtemp()

        # Create a file
        self.filepath = os.path.join(self.directory, 'file1')
        self.filetext = 'Another one bits the dust'
        with open(self.filepath, 'w') as handle:
            handle.write(self.filetext)

        # Make a directory
        os.mkdir(os.path.join(self.directory, 'directory1'))

        self.manager = storage.connect(manager='FS', path=self.directory)

    def tearDown(self):
        shutil.rmtree(self.directory)


class Test_Artefacts(BasicSetup, unittest.TestCase):

    def test_path(self):

        file = self.manager['/file1']
        self.assertEqual(file.path, "/file1")

        file.path = '/directory1/file1'

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {"/directory1/file1", "/directory1"}
        )

        file.path = '/another_directory/file1'

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {"/another_directory/file1", "/directory1", '/another_directory'}
        )

        file.path = '/directory1/file1'
        self.manager['/directory1'].path = "/another_directory/directory2"

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {"/another_directory", "/another_directory/directory2", '/another_directory/directory2/file1'}
        )

    def test_manager(self):

        file = self.manager['/file1']
        directory = self.manager['/directory1']

        self.assertEqual(file.manager, self.manager)
        self.assertEqual(directory.manager, self.manager)

        with pytest.raises(AttributeError):
            file.manager = None


class Test_Files(BasicSetup, unittest.TestCase):

    def test_content(self):
        self.assertEqual(self.manager['/file1'].content.decode(), self.filetext)

    def test_size(self):

        file = self.manager['/file1']
        self.assertEqual(file.size, len(self.filetext))

    def test_modifiedTime(self):

        file = self.manager['/file1']
        self.assertTrue(
            (datetime.datetime.now() - datetime.timedelta(seconds=2)) < file.modifiedTime and
            (datetime.datetime.now()) > file.modifiedTime
        )

    def test_opening(self):
        file = self.manager['/file1']

        with file.open('r') as fh:
            self.assertEqual(fh.read(), self.filetext)

        with file.open('a') as fh:
            fh.write("added")

        with open(self.filepath, 'r') as fh:
            self.assertEqual(fh.read(), self.filetext + "added")

        text = "Something else"
        with file.open('w') as fh:
            fh.write(text)

        with open(self.filepath, 'r') as fh:
            self.assertEqual(fh.read(), text)

    def test_update(self):

        file = self.manager['/file1']

        newTime = (datetime.datetime.now() - datetime.timedelta(seconds=2))
        newSize = 4000
        newFile = storage.artefacts.File(self.manager, '/file1', newTime, newSize)

        file._update(newFile)

        self.assertEqual(file.modifiedTime, newTime)
        self.assertEqual(len(file), newSize)

class Test_Directories(unittest.TestCase):

    def setUp(self):

        self.directory = tempfile.mkdtemp()
        os.mkdir(os.path.join(self.directory, 'dir1'))

        self.filepath = os.path.join(self.directory, 'dir1', 'file1')
        self.filetext = 'Another one bits the dust'
        with open(self.filepath, 'w') as handle:
            handle.write(self.filetext)

        self.manager = storage.connect(manager='FS', path=self.directory)

    def tearDown(self):
        shutil.rmtree(self.directory)

    def test_removal(self):

        directory = self.manager.ls().pop()

        self.assertIsInstance(directory, storage.Directory)

        directory.rm('/file1')