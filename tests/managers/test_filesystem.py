import unittest
import pytest

import os
import tempfile
import shutil
import random
import string
import time
import datetime
import pickle
import multiprocessing

import stow
from stow.managers import FS

def mpManagerLSFunc(manager):
    return {x.name for x in manager.ls()}

@unittest.skipIf(os.name != 'nt', 'Not windows os')
class Test_WindowsFilesystemQwerks(unittest.TestCase):

    def test_windows_file_length_issue(self):

        with tempfile.TemporaryDirectory() as directory:

            short_file_name = 'a'*(258 - len(directory))
            long_file_name = 'b'*(260 - len(directory))

            short_abspath = os.path.join(directory, short_file_name)
            with open(short_abspath, 'w') as handle:
                handle.write('hello')

            long_abspath = os.path.join(directory, long_file_name)
            with self.assertRaises(FileNotFoundError):
                with open(long_abspath, 'w') as handle:
                    handle.write('hello')

            with stow.open(long_abspath, 'w') as handle:
                handle.write('hello')

            with self.assertRaises(FileNotFoundError):
                with open(long_abspath) as handle:
                    pass

            with stow.open(long_abspath) as handle:
                self.assertEqual(handle.read(), 'hello')

            with self.assertRaises(FileNotFoundError):
                os.remove(long_abspath)

            stow.rm(long_abspath)

class Test_Filesystem(unittest.TestCase):

    def setUp(self):
        # Make the managers local space to store files
        directoryParts = os.path.splitdrive(tempfile.mkdtemp())
        self.directory = directoryParts[0].lower() + directoryParts[1]

        # Define the manager
        self.manager = FS(path=self.directory)

    def setUpWithFiles(self):

        with open(os.path.join(self.directory, "initial_file1.txt"), "w") as handle:
            handle.write("Content")

        os.mkdir(os.path.join(self.directory, "initial_directory"))
        with open(os.path.join(self.directory, "initial_directory", "initial_file2.txt"), "w") as handle:
            handle.write("Content")

        os.mkdir(os.path.join(self.directory, "directory-stack"))
        os.mkdir(os.path.join(self.directory, "directory-stack", "directory-stack"))
        with open(os.path.join(self.directory, "directory-stack", "directory-stack", "initial_file3.txt"), "w") as handle:
            handle.write("Content")

        # Define the manager
        self.manager = FS(path=self.directory)

    def tearDown(self):

        # Delete the directory and all it's contents
        shutil.rmtree(self.directory)

    def test_splitArtefactTypeError(self):
        with self.assertRaises(TypeError):
            self.manager.mklink(10, 'path')

    def test_relativePath(self):

        stow.artefact('README.md', type=stow.File)

    def test_config(self):

        config = self.manager.config

        self.assertDictEqual(
            config,
            {'path': self.directory}
        )

    def test_artefact(self):

        self.setUpWithFiles()

        self.manager.artefact("/initial_file1.txt")

    def test_initial_files(self):

        self.setUpWithFiles()

        self.assertTrue("/initial_file1.txt" in self.manager)
        self.assertTrue("/initial_directory/initial_file2.txt" in self.manager)

    def test_mkdir(self):

        self.manager.mkdir('/directory')

        self.assertEqual(len(self.manager.ls(recursive=True)), 1)

        directory = self.manager['/directory']
        self.assertIsInstance(directory, stow.Directory)
        self.assertTrue(len(directory) == 0)

    def test_mkdir_ignore_exists(self):

        self.manager.mkdir("/directory")

        # Does not through error
        self.manager.mkdir("/directory")

        self.manager.touch("/directory/file1.txt")

        # Again doesn't through error
        self.manager.mkdir("/directory")

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {f"{os.sep}directory", f"{os.sep}directory{os.sep}file1.txt"}
        )

        with pytest.raises(stow.exceptions.OperationNotPermitted):
            self.manager.mkdir("/directory", ignore_exists=False)

    def test_mkdir_overwrite(self):


        self.manager.touch("/directory/file1.txt")

        # Again doesn't through error
        self.manager.mkdir("/directory", overwrite=True)

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {f"{os.sep}directory"}
        )

    def test_touch(self):

        self.manager.touch('/file1.txt')
        self.manager.touch('/directory/file2.txt')

        self.manager.mkdir('/otherdir')
        self.manager.touch('/otherdir/file3.txt')

        self.assertEqual(len(self.manager.ls(recursive=True)), 5)
        self.assertIn(self.manager['/directory/file2.txt'], self.manager['/directory'])
        self.assertIn(self.manager['/otherdir/file3.txt'], self.manager['/otherdir'])


    def test_contains(self):

        file1 = self.manager.touch('/file1.txt')

        # Check that the file exists in the manager
        self.assertTrue('/file1.txt' in self.manager)
        self.assertTrue(file1 in self.manager)

        # Esure that non existent files doesn't exist
        self.assertFalse('/file-non-existent.txt' in self.manager)
        self.assertFalse(stow.File(self.manager, '/file-non-existent.txt', 10, datetime.datetime.utcnow()) in self.manager)

    def test_getEnsuresDirectories(self):

        with tempfile.TemporaryDirectory() as directory:

            file = self.manager.touch("/file1.txt")
            contentbytes = b"here is some content"
            file.content(contentbytes)

            self.assertEqual(file.content(), contentbytes)

            # Get the file
            filepath = os.path.join(directory, "some_dir", "another dir", "file.1.txt")
            self.manager.get("/file1.txt", filepath)

            with open(filepath, "rb") as handle:
                self.assertEqual(handle.read(), contentbytes)

    def test_getWontOverwriteDirectory(self):

        with tempfile.TemporaryDirectory() as directory:

            filepath = os.path.join(directory, "some_dir", "another dir", "file.1.txt")
            os.makedirs(os.path.dirname(filepath))

            file = self.manager.touch("/file1.txt")
            contentbytes = b"here is some content"
            file.content(contentbytes)

            with pytest.raises(stow.exceptions.OperationNotPermitted):
                self.manager.get("/file1.txt", os.path.join(directory, "some_dir"))

            self.manager.get("/file1.txt", os.path.join(directory, "some_dir"), overwrite=True)

            with open(os.path.join(directory, "some_dir"), "rb") as handle:
                self.assertEqual(handle.read(), contentbytes)

    def test_put_and_get(self):

        with tempfile.TemporaryDirectory() as directory:

            localInFP = os.path.join(directory, 'in.txt')
            localOutFP = os.path.join(directory, 'out.txt')

            content = 'here are some lines'

            # Create a file to be put into the manager
            with open(localInFP, 'w') as fh:
                fh.write(content)

            # Put the file onto the server
            file = self.manager.put(localInFP, '/test1.txt')

            # Assert that the pushed item is a file
            self.assertIsInstance(file, stow.artefacts.File)

            # Pull the file down again
            self.manager.get('/test1.txt', localOutFP)

            with open(localOutFP, 'r') as fh:
                self.assertEqual(fh.read(), content)

    def test_puttingAndPullingDirectories(self):

        with tempfile.TemporaryDirectory() as directory:

            inputdir = os.path.join(directory, "input-dir")
            outputdir = os.path.join(directory, "output-dir")

            os.mkdir(inputdir)
            with open(os.path.join(inputdir, "file1.txt"), "w") as handle:
                handle.write("here is some lovely content")

            self.manager.put(inputdir, "/directory")
            self.manager.get("/directory", outputdir)

            self.assertEqual(set(os.listdir(directory)), {"input-dir", "output-dir"})
            self.assertEqual(set(os.listdir(outputdir)), {"file1.txt"})

    def test_put_and_get_with_directories(self):

        with tempfile.TemporaryDirectory() as directory:

            # Make a directory of files and sub-files
            d = os.path.join(directory, 'testdir')

            os.mkdir(d)

            with open(os.path.join(d, 'test1.txt'), 'w') as fh:
                fh.write('1')

            # Sub directory
            dSub = os.path.join(d, 'subdir')

            os.mkdir(dSub)

            with open(os.path.join(dSub, 'test2.txt'), 'w') as fh:
                fh.write('2')

            art = self.manager.put(d, '/testdir')

            self.assertIsInstance(art, stow.artefacts.Directory)

    def test_putting_directories_overwrites(self):

        with tempfile.TemporaryDirectory() as directory:

            # Create a directory and a file on the manager
            self.manager.touch('/directory/file1.txt')

            # Create a local directory and similar file
            path = os.path.join(directory, 'directory')
            os.mkdir(path)
            open(os.path.join(path, 'file2.txt'), 'w').close()

            # Put the local directory into the machine, ensure that its overwritten
            self.manager.put(path, '/directory', overwrite=True)

            folder = self.manager['/directory']

            self.assertEqual(len(folder), 1)

            with pytest.raises(stow.exceptions.ArtefactNotFound):
                self.manager['/directory/file1.txt']

            self.manager['/directory/file2.txt']

    def test_putting_directories_overwrite_throws_error(self):
        """ Test that when putting a directory onto another that the system throws an error warning about the possible
        loss of data
        """

        # Create a file
        self.manager.touch("/directory/file1.txt")

        # Create and try and put a directory
        with tempfile.TemporaryDirectory() as directory:

            # Create a file in the directory
            open(os.path.join(directory, "file2.txt"), "w").close()

            with pytest.raises(stow.exceptions.OperationNotPermitted):
                self.manager.put(directory, "/directory")

    def test_putting_directories_strategy_overwrite(self):
        """ Test that when putting a directory onto another that the system throws an error warning about the possible
        loss of data
        """

        # Create a file
        self.manager.touch("/directory/file1.txt")

        # Create and try and put a directory
        with tempfile.TemporaryDirectory() as directory:

            # Create a file in the directory
            open(os.path.join(directory, "file2.txt"), "w").close()

            # Signal that it is okay to overwrite the directory
            self.manager.put(directory, "/directory", overwrite=True)

        # Get the directory
        remoteDirectory = self.manager["/directory"]

        # Assert that the directory object only has the uploaded file in it
        self.assertEqual({a.basename for a in remoteDirectory.ls()}, {"file2.txt"})

    def test_getBytes(self):

        file = self.manager.touch("/A/a.txt")
        file.content(b"content")

        self.assertEqual(self.manager.get("/A/a.txt"), b"content")

    def test_cp(self):

        file = self.manager.touch("/A/a.txt")
        file.content(b"content")

        self.manager.cp(file, "/A/b.txt")

        file2 = self.manager["/A/b.txt"]

        self.assertEqual(file2.content(), b"content")

    def test_ls(self):
        """ Create a hierarchy of files and show that listing the

        A
        B c.txt C
        d.txt e.txt |

        """

        # Create the filesystem
        self.manager.touch('/A/c.txt')
        self.manager.touch('/A/B/d.txt')
        self.manager.touch('/A/B/e.txt')
        self.manager.mkdir('/A/C')

        # Assert top level
        self.assertEqual(self.manager.ls(), {self.manager['/A']})
        self.assertEqual(self.manager.ls(), self.manager['/'].ls())

        # Assert Next level
        self.assertEqual(self.manager['/A'].ls(), {self.manager[x] for x in ['/A/c.txt', '/A/B', '/A/C']})

        # Assert Next level
        self.assertEqual(self.manager['/A/B'].ls(), {self.manager[x] for x in ['/A/B/d.txt', '/A/B/e.txt']})
        self.assertEqual(self.manager['/A/C'].ls(), set())

        # Assert the recursive function
        objects = {
            self.manager[x]
            for x in [
                "/A", "/A/c.txt",
                "/A/B", "/A/B/d.txt",
                "/A/B/e.txt",
                "/A/C"
            ]
        }

        self.assertEqual(self.manager.ls(recursive=True), objects)
        self.assertEqual(self.manager['/'].ls(recursive=True), objects)

    def test_ls_depth(self):

        self.setUpWithFiles()

        content = self.manager.ls("/directory-stack/directory-stack")

        self.assertEqual(len(content), 1)
        self.assertEqual(content.pop().path, f"{os.sep}directory-stack{os.sep}directory-stack{os.sep}initial_file3.txt")

    def test_mv_toplevel(self):
        # Check moving files at the top level works correctly.

        self.setUpWithFiles()

        self.manager.mv('initial_file1.txt', 'initial_file2.txt')
        self.manager.mv('/initial_file2.txt', '/initial_file3.txt')

        f = self.manager['/initial_file3.txt']

        self.manager.mv('initial_directory', 'initial_directory1')
        self.manager.mv('/initial_directory1', '/initial_directory2')

        d = self.manager['/initial_directory2']

    def test_mv_files(self):

        content = 'Here is some file content to be verified'

        # Create a file on the manager
        file_one = self.manager.touch('/file1.txt')
        file_two = self.manager.touch('/file2.txt')

        with file_one.open('w') as fh: fh.write(content)
        with file_two.open('w') as fh: fh.write(content)

        # Assert that the file exists
        self.assertTrue(self.manager['/file1.txt'])
        self.assertTrue(self.manager['/file2.txt'])

        with pytest.raises(stow.exceptions.ArtefactNotFound):
            self.manager['/file3.txt']

        with pytest.raises(stow.exceptions.ArtefactNotFound):
            self.manager['/file4.txt']

        # Move the file
        self.manager.mv('/file1.txt', '/file3.txt')
        self.manager.mv(file_two, '/file4.txt')

        # Assert that the file exists
        self.assertTrue(self.manager['/file3.txt'])
        self.assertTrue(self.manager['/file4.txt'])
        with pytest.raises(stow.exceptions.ArtefactNotFound): self.manager['/file1.txt']
        with pytest.raises(stow.exceptions.ArtefactNotFound): self.manager['/file2.txt']

        # Open the new file and assert that its content matches
        with self.manager.open('/file3.txt', 'r') as handle:
            self.assertEqual(handle.read(), content)

        with self.manager.open('/file4.txt', 'r') as handle:
            self.assertEqual(handle.read(), content)

    def test_mv_directory(self):

        with self.manager.open('/directory/file1.txt', 'w') as handle:
            handle.write('Some stuff')

        # Get the artefacts to compare with the result of moving the directory
        directory = self.manager['/directory']

        directory = self.manager.mv('/directory', '/another')
        self.assertEqual(directory.path, f"{os.sep}another")

        self.assertEqual({art.path for art in self.manager.ls(recursive=True)}, {f"{os.sep}another", f"{os.sep}another{os.sep}file1.txt"})

    def test_rm_file(self):

        # Create a file on the manager
        file = self.manager.touch('/file1.txt')

        os.stat(file.abspath)

        # Delete the file
        self.manager.rm('/file1.txt')

        # Demonstrate that the file has been removed from the manager
        with pytest.raises(stow.exceptions.ArtefactNotFound):
            self.manager['/file1.txt']

        with pytest.raises(FileNotFoundError):
            os.stat(file.abspath)

    def test_rm_empty_directory(self):

        # Make an empty directory to delete
        self.manager.mkdir('/directory')

        tempDir = self.manager['/directory']
        self.assertTrue(tempfile._exists)

        # Delete the directory
        self.manager.rm('/directory')


    def test_rm_non_empty_directory(self):

        with tempfile.TemporaryDirectory() as directory:

            # Make a directory and some content
            self.manager.mkdir('/directory')
            self.manager.touch('/directory/file1.txt')

            # Get the two items
            folder = self.manager['/directory']
            file = self.manager['/directory/file1.txt']

            # Ensure that they exist
            for i, (art, method) in enumerate([(folder, os.path.isdir), (file, os.path.isfile)]):

                local_path = os.path.join(directory, str(i))

                self.manager.get(art, local_path)

                self.assertTrue(os.path.exists(local_path))
                self.assertTrue(method(local_path))

            # Ensure that one cannot delete the directory while it still has contents
            with pytest.raises(stow.exceptions.OperationNotPermitted):
                self.manager.rm(folder)

            # Remove recursively
            self.manager.rm(folder, recursive=True)

            self.assertEqual(self.manager.artefact('/', type=stow.Directory).ls(), set())

    def test_manager_open(self):

        with self.manager.open('/directory/file.txt', 'w') as handle:
            handle.write('some content')

        file = self.manager.artefact('/directory/file.txt', type=stow.File)

        with file.open() as handle:
            self.assertEqual(handle.read(), 'some content')


    def write_some_files(self):

        with self.manager.open("/directory/subdirectory/file1.txt", "w") as handle:
            handle.write("Content")

        with self.manager.open("/directory/subdirectory/file2.txt", "w") as handle:
            handle.write("Content in the same directory")

        with self.manager.open("/directory/anotherdirectory/file2.txt", "w") as handle:
            handle.write("Content with info")

        with self.manager.open("/directory/anotherdirectorymark2/file42.txt", "w") as handle:
            handle.write("Content with info")

    def test_manager_localise_files(self):
        """ Test that the localisation method correctly makes files and directories accessible """


        with tempfile.TemporaryDirectory() as directory:

            self.write_some_files()

            # Assert that localising a file can then be accessed by using the os and local functions
            with self.manager.localise("/directory/subdirectory/file1.txt") as abspath:
                with open(abspath, "r") as handle:
                    self.assertEqual(handle.read(), "Content")

            # Assert that changing a localised file is then updated for the manager
            with self.manager.localise("/directory/subdirectory/file1.txt") as abspath:
                with open(abspath, "w") as handle:
                    handle.write("Overwriting the content of the file")

            self.manager.get("/directory/subdirectory/file1.txt", os.path.join(directory, "temp1.txt"))
            self.manager.get(self.manager["/directory/subdirectory/file1.txt"], os.path.join(directory, "temp2.txt"))
            self.manager["/directory/subdirectory/file1.txt"].save(os.path.join(directory, "temp3.txt"))

            for i in range(1, 4):
                with open(os.path.join(directory, "temp{}.txt".format(i)), "r") as handle:
                    self.assertEqual(handle.read(), "Overwriting the content of the file")

            # Assert that a non existent files can be localised and that they are created
            with self.manager.localise("/another/file3.txt") as abspath:

                # The file cannot be read as it doesn't exist - the user shall have to create the file
                with pytest.raises(FileNotFoundError):
                    with open(abspath, "r") as handle:
                        pass

                with open(abspath, "w") as handle:
                    handle.write("Some content")

            file = self.manager['/another/file3.txt']
            self.assertIsInstance(file, stow.artefacts.File)
            self.assertEqual(file.path, f"{os.sep}another{os.sep}file3.txt")
            with file.open("r") as handle:
                self.assertEqual(handle.read(), "Some content")

    def test_manager_localise_directories(self):

        # Some files shall be written
        self.write_some_files()

        # Assert that a directory can be localised
        with self.manager.localise("/directory") as abspath:
            self.assertSetEqual(set(os.listdir(abspath)), {"subdirectory", "anotherdirectory", "anotherdirectorymark2"})

            with open(os.path.join(abspath, "subdirectory", "file1.txt"), "r") as handle:
                self.assertEqual(handle.read(), "Content")


        # Assert that changing the localised directory shall change the directory in the manager
        with self.manager.localise("/directory") as abspath:

            # We want to test the creation of a new file, the fact that an old file is not changed, and the deletion of
            # a file. Then we want to check the deletion and creation of directories

            # Making a new files at top/lower levels
            with open(os.path.join(abspath, "file5.txt"), "w") as handle:
                handle.write("Running out of content to write")

            with open(os.path.join(abspath, "subdirectory", "newfile.txt"), "w") as handle:
                handle.write("Newly added content with a file")

            # Updating an old file
            with open(os.path.join(abspath, "subdirectory", "file2.txt"), "w") as handle:
                handle.write("EDITTED")

            # Deleting an old file
            os.remove(os.path.join(abspath, "anotherdirectory", "file2.txt"))

            # New empty directory
            os.mkdir(os.path.join(abspath, "new-empty-directory"))

            # New directory with some new content
            os.mkdir(os.path.join(abspath, "test-directory"))
            with open(os.path.join(abspath, "test-directory", "file4.txt"), "w") as handle:
                handle.write("Some stuff")

            # Delete a directory
            shutil.rmtree(os.path.join(abspath, "anotherdirectorymark2"))

        # Assert that the directory has 5 items
        self.assertEqual(
            len(self.manager['/directory'].ls()),
            5 # 4 directories and a file
        )

        self.assertEqual(len(self.manager['/directory/subdirectory']), 3)
        self.assertEqual(self.manager['/directory/subdirectory/newfile.txt'].content().decode(), "Newly added content with a file")
        self.assertEqual(self.manager['/directory/subdirectory/file1.txt'].content().decode(), "Content")
        self.assertEqual(self.manager['/directory/subdirectory/file2.txt'].content().decode(), "EDITTED")

        self.assertEqual(len(self.manager['/directory/anotherdirectory']), 0)
        self.assertEqual(len(self.manager['/directory/new-empty-directory']), 0)

        self.assertEqual(len(self.manager['/directory/test-directory']), 1)
        self.assertEqual(self.manager['/directory/test-directory/file4.txt'].content().decode(), "Some stuff")

        self.assertEqual(self.manager['/directory/file5.txt'].content().decode(), "Running out of content to write")


        # Assert that you can localise a non existent directory and make it so
        with self.manager.localise("/nonexistent") as abspath:
            if not os.path.exists(abspath): os.mkdir(abspath)  # NOTE example of how to write protection around the directory

            self.assertEqual(len(os.listdir(abspath)), 0)

        self.assertIsInstance(self.manager['/nonexistent'], stow.artefacts.Directory)

    def test_put_bytest(self):
        """ Put files with bytes
        """

        content = "Hello there"

        self.manager.put(bytes(content, "utf8"), "/file1.txt")

        self.assertEqual(self.manager["/file1.txt"].content().decode(), content)

    def test_put_bytes_overwrite(self):
        """ Put bytes overwriting a file that previously existed there
        """

        with self.manager.open("/file1.txt", "w") as handle:
            handle.write("0123456789")

        file = self.manager["/file1.txt"]
        size = file.size

        file = self.manager.put(bytes("hello world", "utf8"), "/file1.txt")

        self.assertNotEqual(file.size, size)

    def test_put_non_existent_file(self):

        with tempfile.TemporaryDirectory() as directory:

            with pytest.raises(FileNotFoundError):
                file = self.manager.put(os.path.join(directory, "file1.txt"), "/file1.txt")


    def test_sync_empty(self):
        """ Test that syncing a directory with an empty location puts the directory """

        with tempfile.TemporaryDirectory() as directory:

            # Create a local fs manager
            fsManager = stow.connect(manager="FS", path=directory)

            fsManager.touch("/file1.txt")
            fsManager.touch("/nested/file2.txt")

            folder = self.manager.mkdir("/sync_folder")
            self.manager.sync(fsManager["/"], folder)

            self.assertEqual(
                {art.path for art in self.manager.ls(recursive=True)},
                {
                    f"{os.sep}sync_folder{os.sep}file1.txt",
                    f"{os.sep}sync_folder{os.sep}nested",
                    f"{os.sep}sync_folder{os.sep}nested{os.sep}file2.txt",
                    f"{os.sep}sync_folder"
                }
            )

    def test_sync_update(self):
        """ Test that syncing a directory with an empty location puts the directory """

        with tempfile.TemporaryDirectory() as directory:

            # Create a local fs manager
            fsManager = stow.connect(manager="FS", path=directory)

            fsManager.touch("/file1.txt")
            f2 = fsManager.touch("/file2.txt")
            fsManager.touch("/nested/file3.txt")
            f4 = fsManager.touch("/nested/file4.txt")

            folder = self.manager.mkdir("/sync_folder")
            self.manager.sync(fsManager["/"], folder)

            # Have a calculate-able difference in time
            time.sleep(1)

            # Update the files at source
            with fsManager.open(f2, "w") as handle:
                handle.write("This file has been updated at source")

            with fsManager.open(f4, "w") as handle:
                handle.write("This file has been updated at source")

            # Update the files at destination
            with self.manager.open("/sync_folder/file1.txt", "w") as handle:
                handle.write("This file has been updated at destination")

            with self.manager.open("/sync_folder/nested/file3.txt", "w") as handle:
                handle.write("This file has been updated at destination")

            self.manager.sync(fsManager["/"], folder)

            self.assertEqual(
                {art.path for art in self.manager.ls(recursive=True)},
                {
                    f"{os.sep}sync_folder{os.sep}file1.txt",
                    f"{os.sep}sync_folder{os.sep}file2.txt",
                    f"{os.sep}sync_folder{os.sep}nested",
                    f"{os.sep}sync_folder{os.sep}nested{os.sep}file3.txt",
                    f"{os.sep}sync_folder{os.sep}nested{os.sep}file4.txt",
                    f"{os.sep}sync_folder"
                }
            )

            self.assertEqual(self.manager["/sync_folder/file1.txt"].content().decode(), "This file has been updated at destination")
            self.assertEqual(self.manager["/sync_folder/file2.txt"].content().decode(), "This file has been updated at source")

    def test_serialisation_is_equal(self):
        """ If a manager (directly initialised is created) can we recreated it """

        hydrated = pickle.loads(pickle.dumps(self.manager))

        self.assertEqual(type(hydrated), type(self.manager))
        self.assertDictEqual(hydrated.config, self.manager.config)

    def test_serialisation_uses_cache(self):

        manager = stow.connect('FS', **self.manager.config)

        hydrated = pickle.loads(pickle.dumps(self.manager))

        self.assertIs(manager, hydrated)


    def test_multiprocessing_manager(self):

        pool = multiprocessing.Pool(4)

        result = pool.map(mpManagerLSFunc, [self.manager]*8)

        self.assertEqual([{x.name for x in self.manager.ls()}]*8, result)


    def test_multiprocessing_artefact(self):

        pool = multiprocessing.Pool(4)

        result = pool.map(lambda x: x.name, self.manager.ls())

        self.assertEqual(set(x.name for x in self.manager.ls()), set(result))


