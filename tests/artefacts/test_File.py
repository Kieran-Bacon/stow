import unittest
import os
import tempfile
import datetime
import pickle

import stow

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

    def test_serialised(self):

        file = self.manager['/file1']

        hydrated = pickle.loads(pickle.dumps(file))

        self.assertEqual(hydrated._manager, self.manager)
        self.assertEqual(file, hydrated)


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