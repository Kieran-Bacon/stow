
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

    def test_names(self):

        file = self.manager.touch("file.txt")

        self.assertEqual(file.basename, "file.txt")
        self.assertEqual(file.name, "file")
        self.assertEqual(file.extension, "txt")

        file.basename = "filename"

        self.assertEqual(file.basename, "filename")
        self.assertEqual(file.name, "filename")
        self.assertEqual(file.extension, "")

    def test_settingFileName(self):

        file = self.manager.touch("file.txt")

        file.name = "hello"

        self.assertEqual(file.basename, "hello.txt")
        self.assertEqual(file.name, "hello")
        self.assertEqual(file.extension, "txt")

        file.basename = "hello"

        file.name = "something_else"

        self.assertEqual(file.basename, "something_else")
        self.assertEqual(file.name, "something_else")
        self.assertEqual(file.extension, "")

    def test_settingExtension(self):

        file = self.manager.touch("file.txt")
        file.extension = "ini"

        self.assertEqual(file.basename, "file.ini")
        self.assertEqual(file.name, "file")
        self.assertEqual(file.extension, "ini")

        file.basename = "hello"
        file.extension = "ini"

        self.assertEqual(file.basename, "hello.ini")
        self.assertEqual(file.name, "hello")
        self.assertEqual(file.extension, "ini")

    def test_save(self):

        with tempfile.TemporaryDirectory() as directory:
            localPath = os.path.join(directory, "hello.txt")

            file = self.manager["file1"]

            file.save(localPath)

            with open(localPath) as handle:
                self.assertEqual(handle.read(), self.filetext)

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

        with self.assertRaises(ValueError):
            file.content = newContent

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

    def test_createdTime(self):

        file = self.manager["/file1"]

        self.assertEqual(file.createdTime, file.modifiedTime)

        file = stow.File(self.manager, "/example", 0, file.modifiedTime)

        self.assertEqual(file.createdTime, file.modifiedTime)

        file = stow.File(self.manager, "/example", 0, file.modifiedTime, createdTime= file.modifiedTime - datetime.timedelta(seconds=2))

        self.assertNotEqual(file.createdTime, file.modifiedTime)
        self.assertTrue(file.createdTime < file.modifiedTime)

    def test_accessedTime(self):

        file = self.manager["/file1"]

        self.assertEqual(file.accessedTime, file.modifiedTime)

        file = stow.File(self.manager, "/example", 0, file.modifiedTime)

        self.assertEqual(file.accessedTime, file.modifiedTime)

        file = stow.File(self.manager, "/example", 0, file.modifiedTime, accessedTime=file.modifiedTime + datetime.timedelta(seconds=2))

        self.assertNotEqual(file.accessedTime, file.modifiedTime)
        self.assertTrue(file.accessedTime > file.modifiedTime)

    def test_localise(self):

        file = self.manager["/file1"]

        with file.localise() as abspath:
            with open(abspath) as handle:
                self.assertEqual(handle.read(), self.filetext)

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

        created = (datetime.datetime.now() - datetime.timedelta(seconds=10))
        modified = (datetime.datetime.now() - datetime.timedelta(seconds=5))
        accessed = (datetime.datetime.now() - datetime.timedelta(seconds=0))

        newFile = stow.File(self.manager, "/file1", newSize, modifiedTime=modified, createdTime=created, accessedTime=accessed)

        file._update(newFile)

        self.assertEqual(file.modifiedTime, modified)
        self.assertEqual(file.createdTime, created)
        self.assertEqual(file.accessedTime, accessed)

class Test_SubFiles(Test_Files):

    def setUp(self):

        self.ori = tempfile.mkdtemp()

        self.directory = os.path.join(self.ori, "demo")
        os.mkdir(self.directory)

        manager = stow.connect(manager="FS", path=self.ori)
        self.manager = manager.submanager("/demo")

        # Create a file
        self.filepath = os.path.join(self.directory, 'file1')
        self.filetext = 'Another one bits the dust'
        with open(self.filepath, 'w') as handle:
            handle.write(self.filetext)

        # Make a directory
        os.mkdir(os.path.join(self.directory, 'directory1'))

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
        file2 = stow.File(self.manager, "/example", 0,modifiedTime=modified, createdTime=created, accessedTime=modified)

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

    def test_update(self):

        directory = self.manager["/dir1"]

        created = (datetime.datetime.now() - datetime.timedelta(seconds=10))

        directory._update(stow.Directory(self.manager, directory.path, createdTime=created))

        self.assertEqual(directory.createdTime, created)

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