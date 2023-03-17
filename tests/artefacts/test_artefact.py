""" Test the base artefact object behaviour """

import os
import shutil
import unittest
import pytest
import tempfile

import stow
from stow.managers import FS

class Test_Artefacts(unittest.TestCase):

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()

        self.manager = FS(self.directory)

    def tearDown(self) -> None:
        shutil.rmtree(self.directory)

    def test_existence(self):

        # Test that when a file is deleted that the object no longer works
        file = self.manager["/file1"]

        # Assert that we can get the length of the file
        self.assertTrue(len(file))

        # Detele the file from the manager
        self.manager.rm("/file1")

        with pytest.raises(stow.exceptions.ArtefactNoLongerExists):
            self.assertTrue(len(file))

    def test_abspath(self):

        file = self.manager['/file1']
        self.assertEqual(file.abspath, os.path.join(self.directory, 'file1'))

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

    def test_directory(self):

        file = self.manager['/file1']

        self.assertIsInstance(file.directory, stow.Directory)
        self.assertEqual(file.directory.path, '/')

    def test_basename(self):

        file = self.manager["/file1"]
        self.assertEqual(file.basename, "file1")

        file.basename = 'file1.txt'

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {"/directory1", "/file1.txt"}
        )


        # Directory changes
        self.manager.touch("/another_directory/file1.txt")
        directory = self.manager["/another_directory"]

        self.assertEqual(directory.basename, "another_directory")

        directory.basename = "something_else"

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {"/directory1", "/something_else/file1.txt", '/something_else', '/file1.txt'}
        )

    def test_name(self):
        file = self.manager["/file1"]
        self.assertEqual(file.name, "file1")

        file.basename = 'file1.txt'

        file.name = 'file1-changed'

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {"/directory1", "/file1-changed.txt"}
        )

        # Directory changes
        directory = self.manager.mkdir("/another_directory")

        self.assertEqual(directory.basename, "another_directory")

        directory.name = "something_else"

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {"/directory1", "/file1-changed.txt", '/something_else'}
        )

        directory.name = "something_else_again/with_level"

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {"/directory1", "/file1-changed.txt", '/with_level'}
        )

    def test_manager(self):

        file = self.manager['/file1']
        directory = self.manager['/directory1']

        self.assertEqual(file.manager, self.manager)
        self.assertEqual(directory.manager, self.manager)

        with pytest.raises(AttributeError):
            file.manager = None