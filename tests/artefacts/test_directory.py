import unittest
import pytest

import os
import time
import tempfile
import shutil
import pickle

import stow

class Test_Directories(unittest.TestCase):

    def setUp(self):

        self.directory = os.path.splitdrive(tempfile.mkdtemp())
        self.directory = self.directory[0].lower() + self.directory[1]

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

    def test_relpath(self):

        directory = self.manager["/dir1"]
        directory: stow.Directory

        self.assertEqual(directory.relpath("/dir1/file1.txt"), "file1.txt")
        self.assertEqual(directory.relpath(self.manager["/dir1/file1"]), "file1")

        # with self.assertRaises(stow.exceptions.ArtefactNotMember):
        #     directory.relpath("/somethingelse/here")

    def test_contentUpdateModifiedTime(self):

        with tempfile.TemporaryDirectory() as directory:

            filepath = stow.join(directory, "filename.txt")

            file = stow.touch(filepath)

            modifiedTime = file.modifiedTime

            time.sleep(0.1)
            file.content = b"file content"

            modifiedFile = stow.artefact(filepath)

            self.assertNotEqual(modifiedFile.modifiedTime, file.modifiedTime)
            self.assertTrue(modifiedFile.modifiedTime > file.modifiedTime)


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

        if isinstance(directory, stow.File):
            raise ValueError('Incorrect type')

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

    def test_serialised(self):

        directory = self.manager['/dir1']

        hydrated = pickle.loads(pickle.dumps(directory))

        self.assertEqual(hydrated._manager, self.manager)
        self.assertEqual(directory, hydrated)

    def test_invalidContains(self):

        with pytest.raises(TypeError):
            10 in self.manager['/dir1']

    def test_isMount(self):
        directory = stow.artefact('G:/')

        self.assertTrue(directory.isMount())

    def test_disappearingDirectory(self):

        directory = self.manager.mkdir('/directory1')
        self.manager.rm('/directory1')

        with pytest.raises(stow.exceptions.ArtefactNoLongerExists):
            directory.ls()

