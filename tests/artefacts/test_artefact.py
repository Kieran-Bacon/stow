""" Test the base artefact object behaviour """

import unittest
import pytest

import stow

from . import BasicSetup

class Test_Artefacts(BasicSetup, unittest.TestCase):

    def test_existence(self):

        # Test that when a file is deleted that the object no longer works
        file = self.manager["/file1"]

        # Assert that we can get the length of the file
        self.assertTrue(len(file))

        # Detele the file from the manager
        self.manager.rm("/file1")

        print(self.assertTrue(len(file)))
        with pytest.raises(stow.exceptions.ArtefactNoLongerExists):
            self.assertTrue(len(file))

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

    def test_basename(self):

        file = self.manager["/file1"]
        self.assertEqual(file.basename, "file1")

        file.basename = 'file1.txt'

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {"/directory1", "/file1.txt"}
        )

        file.basename = '/another_directory/file1.txt'

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {"/directory1", "/another_directory/file1.txt", '/another_directory'}
        )

        # Directory changes

        directory = self.manager["/another_directory"]

        self.assertEqual(directory.basename, "another_directory")

        directory.basename = "something_else"

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {"/directory1", "/something_else/file1.txt", '/something_else'}
        )

        directory.basename = "something_else_again/with_level"

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {"/directory1", "/something_else_again/with_level/file1.txt", '/something_else_again', "/something_else_again/with_level"}
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
            {"/directory1", "/file1-changed.txt", '/something_else_again', "/something_else_again/with_level"}
        )

    def test_manager(self):

        file = self.manager['/file1']
        directory = self.manager['/directory1']

        self.assertEqual(file.manager, self.manager)
        self.assertEqual(directory.manager, self.manager)

        with pytest.raises(AttributeError):
            file.manager = None
