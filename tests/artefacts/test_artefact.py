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
        self._directory = tempfile.TemporaryDirectory()
        self.directory = os.path.normcase(self._directory.name)

        self.file1 = os.path.join(self.directory, 'file1')
        with open(self.file1, 'w') as handle:
            handle.write('Content')

        os.mkdir(os.path.join(self.directory, 'directory1'))

        self.manager = FS(self.directory)

    def tearDown(self) -> None:
        shutil.rmtree(self.directory)

    def test_length(self):

        # Test that when a file is deleted that the object no longer works
        file = self.manager["/file1"]

        # Assert that we can get the length of the file
        self.assertEqual(len(file), 7)

    def test_abspath(self):

        file = self.manager['/file1']
        self.assertEqual(file.abspath, self.file1)

    def test_path(self):

        file = self.manager.artefact('/file1', type=stow.File)
        self.assertEqual(file.path, f"{os.sep}file1")

        file.path = '/directory1/file1'

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {f"{os.sep}directory1{os.sep}file1", f"{os.sep}directory1"}
        )

        file.path = '/another_directory/file1'

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {f"{os.sep}another_directory{os.sep}file1", f"{os.sep}directory1", f'{os.sep}another_directory'}
        )

        file.path = '/directory1/file1'
        self.manager[f'{os.sep}directory1'].path = f"{os.sep}another_directory{os.sep}directory2"

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {f"{os.sep}another_directory", f"{os.sep}another_directory{os.sep}directory2", f'{os.sep}another_directory{os.sep}directory2{os.sep}file1'}
        )

    def test_directory(self):

        file = self.manager['/file1']

        self.assertIsInstance(file.directory, stow.Directory)
        self.assertEqual(file.directory.path, os.sep)

    def test_basename(self):

        file = self.manager["/file1"]
        self.assertEqual(file.basename, "file1")

        file.basename = 'file1.txt'

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {f"{os.sep}directory1", f"{os.sep}file1.txt"}
        )

        # Directory changes
        self.manager.touch("/another_directory/file1.txt")
        directory = self.manager["/another_directory"]

        self.assertEqual(directory.basename, "another_directory")

        directory.basename = "something_else"

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {f"{os.sep}directory1", f"{os.sep}something_else{os.sep}file1.txt", f'{os.sep}something_else', f'{os.sep}file1.txt'}
        )

    def test_name(self):

        file = self.manager["/file1"]
        self.assertEqual(file.name, "file1")

        file.basename = 'file1.txt'

        file.name = 'file1-changed'

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {f"{os.sep}directory1", f"{os.sep}file1-changed.txt"}
        )

        # Directory changes
        directory = self.manager.mkdir("/another_directory")

        self.assertEqual(directory.basename, "another_directory")

        directory.name = "something_else"

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {f"{os.sep}directory1", f"{os.sep}file1-changed.txt", f'{os.sep}something_else'}
        )

        directory.name = "something_else_again/with_level"

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {f"{os.sep}directory1", f"{os.sep}file1-changed.txt", f'{os.sep}with_level'}
        )