
import unittest
from unittest.mock import patch, MagicMock
import pytest

import os
import tempfile
import datetime
import time
import shutil

import stow

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

        self.manager = stow.connect(manager='FS', path=self.directory)

    def tearDown(self):
        shutil.rmtree(self.directory)


class Test_Artefacts(BasicSetup, unittest.TestCase):

    def test_existence(self):

        # Test that when a file is deleted that the object no longer works
        file = self.manager["/file1"]

        # Assert that we can get the length of the file
        self.assertTrue(len(file))

        # Detele the file from the manager
        self.manager.rm("/file1")

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


class Test_Files(BasicSetup, unittest.TestCase):

    def test_extension(self):
        file = self.manager["/file1"]
        self.assertEqual(file.extension, "")

        file.basename = "file1.txt"

        self.assertEqual(file.extension, "txt")

        file.basename = "file1.tar.gz"

        self.assertEqual(file.extension, "gz")


    def test_content(self):

        file = self.manager['/file1']

        self.assertEqual(file.content.decode(), self.filetext)

        newContent = "this is new content for the file"

        file.content = bytes(newContent, encoding="utf-8")

        self.assertEqual(file.content.decode(), newContent)

    def test_size(self):

        file = self.manager['/file1']
        self.assertEqual(file.size, len(self.filetext))

    def test_modifiedTime(self):

        file = self.manager['/file1']
        self.assertTrue(
            (datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc) - datetime.timedelta(seconds=2)) < file.modifiedTime
        )
        self.assertTrue(
            (datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)) > file.modifiedTime
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
        newFile = stow.artefacts.File(self.manager, '/file1', newSize, newTime)

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

        self.manager = stow.connect(manager='FS', path=self.directory)

    def tearDown(self):
        shutil.rmtree(self.directory)

    def test_removal(self):

        directory = self.manager.ls().pop()

        self.assertIsInstance(directory, stow.Directory)

        directory.rm('/file1')

    def test_mkdir(self):

        self.manager["/dir1"].mkdir("/subdir1")
        self.manager["/dir1"].mkdir("subdir2")

        self.assertTrue(self.manager["/dir1/subdir1"] is not None)
        self.assertTrue(self.manager["/dir1/subdir2"] is not None)

        self.assertEqual(len(self.manager.ls(recursive=True)), 4)

    def test_touch(self):

        self.manager["/dir1"].touch("/file1.txt")
        self.manager["/dir1"].touch("file2.txt")

        self.manager["/dir1"].touch("subdir1/file1.txt")

        self.assertEqual(len(self.manager.ls(recursive=True)), 6)

    def test_open(self):

        with self.manager["/dir1"].open("/file1.txt", "w") as handle:
            handle.write("Some content")

        self.assertTrue("/dir1/file1.txt" in self.manager)
        self.assertEqual(len(self.manager["/dir1/file1.txt"]), 12)

        with pytest.raises(FileNotFoundError):
            with self.manager["/dir1"].open("/file2.txt") as handle:
                handle.read()

    def test_membership(self):

        self.assertTrue("file1" in self.manager["/dir1"])
        self.assertTrue(self.manager["/dir1/file1"] in self.manager["/dir1"])
        f2 = self.manager.touch("/dir1/file2.txt")
        self.assertTrue(f2 in self.manager["/dir1"])

        self.assertFalse("file3.txt" in self.manager["/dir1"])
        f3 = self.manager.touch("/file3.txt")
        self.assertFalse(f3 in self.manager["/dir1"])

    def test_isEmpty(self):

        # Assert on directory that it does have contents
        self.assertFalse(self.manager["/dir1"].isEmpty())

        _dir = self.manager.mkdir("/empty_dir")

        self.assertTrue(_dir.isEmpty())

class Test_Subdirectories(Test_Directories):

    def setUp(self):

        self.ori = tempfile.mkdtemp()
        self.directory = os.path.join(self.ori, "demo")
        os.mkdir(self.directory)

        os.mkdir(os.path.join(self.directory, 'dir1'))

        self.filepath = os.path.join(self.directory, 'dir1', 'file1')
        self.filetext = 'Another one bits the dust'
        with open(self.filepath, 'w') as handle:
            handle.write(self.filetext)

        self.ori = stow.connect(manager='FS', path=self.ori)
        self.manager = self.ori.submanager("/demo")
