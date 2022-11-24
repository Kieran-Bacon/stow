
import unittest
from unittest.mock import patch, MagicMock
import pytest

import os
import tempfile
import datetime
import time
import shutil
import pickle

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


class Test_Directories(unittest.TestCase):

    def setUp(self):

        self.directory = tempfile.mkdtemp()
        os.mkdir(os.path.join(self.directory, 'dir1'))

        self.subdirectory = os.path.join(self.directory, "dir1")
        self.filepath = os.path.join(self.subdirectory, 'file1')
        self.filetext = 'Another one bits the dust'
        with open(self.filepath, 'w') as handle:
            handle.write(self.filetext)

        self.manager = stow.connect(manager='FS', path=self.directory)

    def tearDown(self):
        shutil.rmtree(self.directory)

    def test_name(self):

        directory = self.manager.ls().pop()

        self.assertEqual(directory.name, "dir1")

    def test_time(self):

        directory = self.manager["/dir1"]

        for time in directory.createdTime, directory.modifiedTime, directory.accessedTime:

            self.assertTrue(
                (datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc) - datetime.timedelta(seconds=2)) < time
            )
            self.assertTrue(
                (datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)) > time
            )

        timelessDirectory = stow.Directory(self.manager, "path")

        self.assertEqual(timelessDirectory.createdTime, None)
        self.assertEqual(timelessDirectory.modifiedTime, None)
        self.assertEqual(timelessDirectory.accessedTime, None)

        created = (datetime.datetime.now() - datetime.timedelta(seconds=10))
        modified = (datetime.datetime.now() - datetime.timedelta(seconds=5))
        accessed = (datetime.datetime.now() - datetime.timedelta(seconds=0))


        file1 = stow.File(self.manager, "/example", 0, modifiedTime=modified, createdTime=modified, accessedTime=accessed)
        file2 = stow.File(self.manager, "/example", 0, modifiedTime=modified, createdTime=created, accessedTime=modified)

        timelessDirectory._add(file1)
        timelessDirectory._add(file2)

        self.assertEqual(timelessDirectory.createdTime, created)
        self.assertEqual(timelessDirectory.modifiedTime, modified)
        self.assertEqual(timelessDirectory.accessedTime, accessed)

    def test_modificationTimeUpdate(self):

        file = self.manager["dir1/file1"]

        self.assertEqual(file.createdTime, file.modifiedTime)

        time.sleep(1.0)

        file.content = b"Updated contents"

        stats = os.stat(self.filepath)

        self.assertEqual(datetime.datetime.utcfromtimestamp(stats.st_ctime), file.createdTime.replace(tzinfo=None))
        self.assertEqual(datetime.datetime.utcfromtimestamp(stats.st_mtime), file.modifiedTime.replace(tzinfo=None))
        self.assertEqual(datetime.datetime.utcfromtimestamp(stats.st_atime), file.accessedTime.replace(tzinfo=None))

    def test_relpath(self):

        directory = self.manager["/dir1"]
        directory: stow.Directory

        self.assertEqual(directory.relpath("/dir1/file1.txt"), "file1.txt")
        self.assertEqual(directory.relpath(self.manager["/dir1/file1"]), "file1")

        with self.assertRaises(stow.exceptions.ArtefactNotMember):
            directory.relpath("/somethingelse/here")

    def test_privateLS(self):

        dir1 = self.manager["/dir1"]

        # Because the file inside it has not yet been initialised so ti doesn't exist yet
        self.assertEqual(dir1._ls(), set())

        file = self.manager["/dir1/file1"]

        # Not that is has been loaded the file is referenced inside the directory
        self.assertEqual(dir1._ls(), {file})

        del file

        # It will continue to exist inside the directory as the manager is holding a reference
        self.assertEqual(len(dir1._ls()), 1)

        os.mkdir(os.path.join(self.directory, "dir1", "dir2"))
        open(os.path.join(self.directory, "dir1", "dir2", "file2"), "w").close()

        self.assertEqual(len(dir1._ls(recursive=True)), 1)

        file = self.manager["/dir1/dir2/file2"]

        # Now it has a directory and a new file in it
        self.assertEqual(len(dir1._ls(recursive=True)), 3)

    def test_contentUpdateModifiedTime(self):

        with tempfile.TemporaryDirectory() as directory:

            filepath = stow.join(directory, "filename.txt")

            file = stow.touch(filepath)

            modifiedTime = file.modifiedTime

            time.sleep(1)

            file.content = b"file content"

            self.assertNotEqual(modifiedTime, file.modifiedTime)
            self.assertTrue(modifiedTime < file.modifiedTime)


    def test_save(self):

        with tempfile.TemporaryDirectory() as directory:
            localPath = os.path.join(directory, "direct")

            directoryObj = self.manager["dir1"]

            directoryObj.save(localPath)

            self.assertTrue(os.path.isdir(localPath))

    def test_delete(self):

        directory = self.manager["dir1"]

        with self.assertRaises(stow.exceptions.OperationNotPermitted):
            directory.delete()

        directory.delete(force=True)

        with self.assertRaises(stow.exceptions.ArtefactNotFound):
            self.manager["dir1"]

        self.assertFalse(os.path.exists(self.subdirectory))

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

    def test_empty(self):

        self.manager.touch('/dir1/subdir/file1.txt')

        self.assertEqual(len(self.manager.ls('/dir1')), 2)

        self.manager['/dir1'].empty()

        self.assertEqual(len(self.manager.ls('/dir1')), 0)


    def test_update(self):

        directory = self.manager["/dir1"]

        created = (datetime.datetime.now() - datetime.timedelta(seconds=10))

        directory._update(stow.Directory(self.manager, directory.path, createdTime=created))

        self.assertEqual(directory.createdTime, created)

    def test_serialised(self):

        directory = self.manager['/dir1']

        hydrated = pickle.loads(pickle.dumps(directory))

        self.assertEqual(hydrated._manager, self.manager)
        self.assertEqual(directory, hydrated)

class Test_Subdirectories(Test_Directories):

    def setUp(self):

        self.ori = tempfile.mkdtemp()
        self.directory = os.path.join(self.ori, "demo")
        os.mkdir(self.directory)

        self.subdirectory = os.path.join(self.directory, 'dir1')
        os.mkdir(self.subdirectory)

        self.filepath = os.path.join(self.directory, 'dir1', 'file1')
        self.filetext = 'Another one bits the dust'
        with open(self.filepath, 'w') as handle:
            handle.write(self.filetext)

        self.ori = stow.connect(manager='FS', path=self.ori)
        self.manager = self.ori.submanager("/demo")

    def test_update(self):

        directory = self.manager["/dir1"]

        created = (datetime.datetime.now() - datetime.timedelta(seconds=10))

        directory._update(stow.SubDirectory(self.manager, "/example", stow.Directory(self.ori, directory.path, createdTime=created)))

        self.assertEqual(directory.createdTime, created)