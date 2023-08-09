""" Test the file objects in stow """

import pytest
import unittest

import os
import pickle
import tempfile
import datetime
import time

import stow
from stow.managers import FS

from . import BasicSetup


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

    def test_setContentType(self):
        """ Local FS doesn't support the idea of content type """

        file: stow.File = self.manager['/file1']
        with pytest.raises(NotImplementedError):
            file.content_type = 'video/mp4'

    def test_modifiedTime(self):

        file = self.manager['/file1']
        self.assertAlmostEqual(time.time(), file.modifiedTime.timestamp(), places=1)


    def test_createdTime(self):

        file = self.manager["/file1"]

        self.assertAlmostEqual(file.createdTime.timestamp(), file.modifiedTime.timestamp(), places=1)

        file = stow.File(self.manager, "/example", 0, file.modifiedTime)

        self.assertTrue(file.createdTime <= file.modifiedTime)

        file = stow.File(self.manager, "/example", 0, file.modifiedTime, createdTime= file.modifiedTime - datetime.timedelta(seconds=2))

        self.assertNotEqual(file.createdTime, file.modifiedTime)
        self.assertTrue(file.createdTime < file.modifiedTime)

    def test_accessedTime(self):

        file = self.manager["/file1"]

        self.assertAlmostEqual(file.accessedTime, file.modifiedTime, delta=datetime.timedelta(seconds=2e-2))

        file = stow.File(self.manager, "/example", 0, file.modifiedTime)

        self.assertEqual(file.accessedTime, file.modifiedTime)

        file = stow.File(self.manager, "/example", 0, file.modifiedTime, accessedTime=file.modifiedTime + datetime.timedelta(seconds=2))

        self.assertNotEqual(file.accessedTime, file.modifiedTime)
        self.assertTrue(file.accessedTime > file.modifiedTime)

    def test_settingAccessTime(self):

        file: stow.File = self.manager['/file1']

        accesstime = datetime.datetime.now().timestamp()

        file.accessedTime = accesstime

        stat = os.stat(file.abspath)

        self.assertEqual(stat.st_atime, accesstime)

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

    def test_isLink(self):

        file: stow.File = self.manager['/file1']
        self.assertFalse(file.isLink())

        linked_file = self.manager.mklink(file, '/file-linked')
        self.assertTrue(linked_file.isLink())

    def test_serialised(self):

        file = self.manager['/file1']

        hydrated = pickle.loads(pickle.dumps(file))

        self.assertEqual(hydrated._manager, self.manager)
        self.assertEqual(file, hydrated)

    def test_content_type(self):

        with open(os.path.join(self.directory, 'video.mp4'), 'w') as handle:
            handle.write('data')

        manager = FS(self.directory)
        self.assertEqual('video/mp4', manager['/video.mp4'].content_type)

    def test_update_modified_time(self):

        # Write the file data
        with open(os.path.join(self.directory, 'file.txt'), 'w') as handle:
            handle.write('data')

        # Fetch the file
        manager = FS(self.directory)
        file = manager['/file.txt']

        new_modified_time = datetime.datetime(2022, 10, 10).timestamp()

        self.assertNotEqual(file.modifiedTime, new_modified_time)

        file.modifiedTime = new_modified_time

        self.assertEqual(datetime.datetime.fromtimestamp(new_modified_time, tz=datetime.timezone.utc), file.modifiedTime)

