import unittest
import pytest

import os
import tempfile
import shutil
import contextlib
import abc
import time

import stow

class ManagerTests:

    def setUp(self):
        self.manager = stow.manager.Manager

    @abc.abstractmethod
    def setUpWithFiles(self): pass

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
            {"/directory", "/directory/file1.txt"}
        )

        with pytest.raises(stow.exceptions.OperationNotPermitted):
            self.manager.mkdir("/directory", ignoreExists=False)

    def test_mkdir_overwrite(self):


        self.manager.touch("/directory/file1.txt")

        # Again doesn't through error
        self.manager.mkdir("/directory", overwrite=True)

        self.assertEqual(
            {art.path for art in self.manager.ls(recursive=True)},
            {"/directory"}
        )

    def test_touch(self):

        self.manager.touch('/file1.txt')
        self.manager.touch('/directory/file2.txt')

        self.manager.mkdir('/otherdir')
        self.manager.touch('/otherdir/file3.txt')

        self.assertEqual(len(self.manager.ls(recursive=True)), 5)
        self.assertIn(self.manager['/directory/file2.txt'], self.manager['/directory'])
        self.assertIn(self.manager['/otherdir/file3.txt'], self.manager['/otherdir'])

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

    def test_put_and_get_with_artefacts(self):

        with tempfile.TemporaryDirectory() as directory:

            localInFP = os.path.join(directory, 'in.txt')
            localOutFP = os.path.join(directory, 'out.txt')

            content = 'here are some lines'

            # Create a file to be put into the manager
            with open(localInFP, 'w') as fh:
                fh.write(content)

            # Create a file on the manager
            file = self.manager.touch('/test1.txt')

            # Put the local file onto, using the file object
            file_b = self.manager.put(localInFP, file)

            # Assert its a file and that its the same file object as before
            self.assertIsInstance(file_b, stow.artefacts.File)
            self.assertIs(file, file_b)

            # Pull the file down again - using the file object
            self.manager.get(file, localOutFP)

            with open(localOutFP, 'r') as fh:
                self.assertEqual(fh.read(), content)

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

            with pytest.raises(stow.exceptions.OperationNotPermitted):
                self.manager.put(directory, "/directory", overwrite=True, merge=True)

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

    def test_putting_directories_strategy_merge(self):
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
            self.manager.put(directory, "/directory", merge=True)

        # Get the directory
        remoteDirectory = self.manager["/directory"]

        # Assert that the directory object only has the uploaded file in it
        self.assertEqual({a.basename for a in remoteDirectory.ls()}, {"file1.txt", "file2.txt"})


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
        self.assertEqual(content.pop().path, "/directory-stack/directory-stack/initial_file3.txt")

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

        self.assertEqual(file_one.path, "/file3.txt")
        self.assertEqual(file_two.path, "/file4.txt")

        self.assertEqual(file_one, self.manager['/file3.txt'])
        self.assertEqual(file_two, self.manager['/file4.txt'])

    def test_mv_directory(self):

        with self.manager.open('/directory/file1.txt', 'w') as handle:
            handle.write('Some stuff')

        # Get the artefacts to compare with the result of moving the directory
        directory = self.manager['/directory']
        file = self.manager['/directory/file1.txt']

        self.manager.mv('/directory', '/another')

        self.assertEqual(directory.path, "/another")
        self.assertEqual(file.path, "/another/file1.txt")

        self.assertEqual({art.path for art in self.manager.ls(recursive=True)}, {"/another", "/another/file1.txt"})

    def test_rm_file(self):

        with tempfile.TemporaryDirectory() as directory:

            # Delete a file
            # Delete a directory
            # Fail to delete a directory with contents
            # Delete an full directory

            # Create a file on the manager
            self.manager.touch('/file1.txt')

            # Demonstrate that the file can be collected/played with
            file = self.manager['/file1.txt']
            self.assertTrue(file._exists)
            self.manager.get('/file1.txt', os.path.join(directory, 'temp.txt'))
            os.stat(os.path.join(directory, 'temp.txt'))

            # Delete the file
            self.manager.rm('/file1.txt')

            # Demonstrate that the file has been removed from the manager
            with pytest.raises(stow.exceptions.ArtefactNotFound):
                self.manager['/file1.txt']

            self.assertFalse(file._exists)

            with pytest.raises(stow.exceptions.ArtefactNotFound):
                self.manager.get('/file1.txt', os.path.join(directory, 'temp.txt'))


    def test_rm_empty_directory(self):

        # Make an empty directory to delete
        self.manager.mkdir('/directory')

        tempDir = self.manager['/directory']
        self.assertTrue(tempfile._exists)

        # Delete the directory
        self.manager.rm('/directory')

        self.assertFalse(tempDir._exists)


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
                self.assertTrue(art._exists)

                local_path = os.path.join(directory, str(i))

                self.manager.get(art, local_path)

                self.assertTrue(os.path.exists(local_path))
                self.assertTrue(method(local_path))

            # Ensure that one cannot delete the directory while it still has contents
            with pytest.raises(stow.exceptions.OperationNotPermitted):
                self.manager.rm(folder)

            # Remove recursively
            self.manager.rm(folder, True)

            # Assert that the items are not removed
            # Ensure that they exist
            for art in [folder, file]:
                self.assertFalse(art._exists)

                with pytest.raises(KeyError):
                    self.manager[art.__dict__['path']]

            self.assertEqual(self.manager['/'].ls(), set())

    def test_manager_open(self):

        with self.manager.open('/directory/file.txt', 'w') as handle:
            handle.write('some content')

        file = self.manager['/directory/file.txt']

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
            self.assertEqual(file.path, "/another/file3.txt")
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
        self.assertEqual(self.manager['/directory/subdirectory/newfile.txt'].content.decode(), "Newly added content with a file")
        self.assertEqual(self.manager['/directory/subdirectory/file1.txt'].content.decode(), "Content")
        self.assertEqual(self.manager['/directory/subdirectory/file2.txt'].content.decode(), "EDITTED")

        self.assertEqual(len(self.manager['/directory/anotherdirectory']), 0)
        self.assertEqual(len(self.manager['/directory/new-empty-directory']), 0)

        self.assertEqual(len(self.manager['/directory/test-directory']), 1)
        self.assertEqual(self.manager['/directory/test-directory/file4.txt'].content.decode(), "Some stuff")

        self.assertEqual(self.manager['/directory/file5.txt'].content.decode(), "Running out of content to write")


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

        self.assertEqual(self.manager["/file1.txt"].content.decode(), content)

    def test_put_bytes_overwrite(self):
        """ Put bytes overwriting a file that previously existed there
        """

        with self.manager.open("/file1.txt", "w") as handle:
            handle.write("0123456789")

        file = self.manager["/file1.txt"]
        size = file.size

        file = self.manager.put(bytes("hello world", "utf8"), "/file1.txt")

        self.assertNotEqual(file.size, size)

    def test_put_files_in_directories_with_backslash(self):

        with tempfile.TemporaryDirectory() as directory:

            for i in range(5):
                path = os.path.join(directory, "file{}.txt".format(i))

                with open(path, "w") as handle:
                    handle.write(str(i))

                self.manager.put(path, "/")
                self.manager.put(path, "/directory/")

            self.assertEqual(
                {art.path for art in self.manager.ls(recursive=True)},
                {
                    "/file0.txt",
                    "/file1.txt",
                    "/file2.txt",
                    "/file3.txt",
                    "/file4.txt",
                    "/directory",
                    "/directory/file0.txt",
                    "/directory/file1.txt",
                    "/directory/file2.txt",
                    "/directory/file3.txt",
                    "/directory/file4.txt",
                }
            )

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
                {"/sync_folder/file1.txt", "/sync_folder/nested", "/sync_folder/nested/file2.txt", "/sync_folder"}
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
                {"/sync_folder/file1.txt", "/sync_folder/file2.txt", "/sync_folder/nested", "/sync_folder/nested/file3.txt", "/sync_folder/nested/file4.txt", "/sync_folder"}
            )

            self.assertEqual(self.manager["/sync_folder/file1.txt"].content.decode(), "This file has been updated at destination")
            self.assertEqual(self.manager["/sync_folder/file2.txt"].content.decode(), "This file has been updated at source")



class SubManagerTests:

    @staticmethod
    @contextlib.contextmanager
    def get_filepath(content=""):

        directory = tempfile.mkdtemp()
        filepath = os.path.join(directory, "testing-file.txt")
        with open(filepath, "w") as handle:
            handle.write(content)

        yield filepath

        shutil.rmtree(directory)

    @staticmethod
    @contextlib.contextmanager
    def get_dirpath(files=["file1.txt", "files2.txt", "files3.txt"]):

        directory = tempfile.mkdtemp()

        for i, f in enumerate(files):
            with open(os.path.join(directory, f), "w") as handle:
                handle.write("Content"[:i])

        yield directory

        shutil.rmtree(directory)

    def test_submanager_sub_put_files(self):

        # Create a sub directory at the given location - test putting a file into the
        subManager = self.manager.submanager("/sub-directory")

        with self.get_filepath("content") as filepath:
            # Test putting a file into the sub-manager
            subfile = subManager.put(filepath, "/file1.txt")

        # Get the main file that links to this one
        mainfile = self.manager['/sub-directory/file1.txt']

        # Assert that they have the same information
        self.assertEqual(mainfile.modifiedTime, subfile.modifiedTime)
        self.assertEqual(mainfile.size, subfile.size)

        with self.get_filepath("Some other content") as filepath:
            subfile2 = subManager.put(filepath, "/file1.txt")

        # Get the main file that links to this one
        mainfile2 = self.manager['/sub-directory/file1.txt']

        self.assertIs(subfile, subfile2)
        self.assertIs(mainfile, mainfile2)

        # Assert that they have the same information
        self.assertEqual(mainfile.modifiedTime, subfile.modifiedTime)
        self.assertEqual(mainfile.size, subfile.size)

    def test_submanager_main_put_files(self):

        # Create a sub manager
        subManager = self.manager.submanager("/sub-directory")

        with self.get_filepath("content") as filepath:
            # Test putting file in the main manager
            mainfile = self.manager.put(filepath, "/sub-directory/file1.txt")

        # Test
        subfile = subManager["/file1.txt"]

        self.assertEqual(mainfile.modifiedTime, subfile.modifiedTime)
        self.assertEqual(mainfile.size, subfile.size)

        with self.get_filepath("content") as filepath:
            # Test putting file in the main manager
            mainfile2 = self.manager.put(filepath, "/sub-directory/file1.txt")

        # Get the main file that links to this one
        subfile2 = subManager['/file1.txt']

        self.assertIs(subfile, subfile2)
        self.assertIs(mainfile, mainfile2)

        # Assert that they have the same information
        self.assertEqual(mainfile.modifiedTime, subfile.modifiedTime)
        self.assertEqual(mainfile.size, subfile.size)

    def test_submanager_sub_put_directories_overwrite(self):

        subManager = self.manager.submanager("/sub-directory")

        with self.get_dirpath() as directory:
            subdir = subManager.put(directory, "/directory")

        maindir = self.manager["/sub-directory/directory"]

        # Assert that the directories are consisten with each other
        self.assertEqual(len(maindir), len(subdir))

        artpath = lambda art: art.path
        for main, sub in zip(sorted(maindir, key=artpath), sorted(subdir, key=artpath)):

            self.assertEqual(main.modifiedTime, sub.modifiedTime)
            self.assertEqual(main.size, sub.size)

        # Put and overwrite
        with self.get_dirpath(['file2.txt', 'file3.txt']) as directory:

            with pytest.raises(stow.exceptions.OperationNotPermitted):
                subdir1 = subManager.put(directory, "/directory")

            subdir1 = subManager.put(directory, "/directory", overwrite=True)

        maindir1 = self.manager["/sub-directory/directory"]

        self.assertIs(subdir, subdir1)
        self.assertIs(maindir, maindir1)

        # Assert that the directories are consisten with each other
        self.assertEqual(len(maindir), len(subdir))

        artpath = lambda art: art.path
        for main, sub in zip(sorted(maindir, key=artpath), sorted(subdir, key=artpath)):

            self.assertEqual(main.modifiedTime, sub.modifiedTime)
            self.assertEqual(main.size, sub.size)

    def test_submanager_sub_put_directories_merge(self):

        subManager = self.manager.submanager("/sub-directory")

        with self.get_dirpath() as directory:
            subdir = subManager.put(directory, "/directory")

        maindir = self.manager["/sub-directory/directory"]

        # Assert that the directories are consisten with each other
        self.assertEqual(len(maindir), len(subdir))

        artpath = lambda art: art.path
        for main, sub in zip(sorted(maindir, key=artpath), sorted(subdir, key=artpath)):

            self.assertEqual(main.modifiedTime, sub.modifiedTime)
            self.assertEqual(main.size, sub.size)

        # Put and overwrite
        with self.get_dirpath(['file2.txt', 'file3.txt']) as directory:

            with pytest.raises(stow.exceptions.OperationNotPermitted):
                subdir1 = subManager.put(directory, "/directory")

            subdir1 = subManager.put(directory, "/directory", merge=True)

        maindir1 = self.manager["/sub-directory/directory"]

        self.assertIs(subdir, subdir1)
        self.assertIs(maindir, maindir1)

        # Assert that the directories are consisten with each other
        self.assertEqual(len(maindir), len(subdir))

        artpath = lambda art: art.path
        for main, sub in zip(sorted(maindir, key=artpath), sorted(subdir, key=artpath)):

            self.assertEqual(main.modifiedTime, sub.modifiedTime)
            self.assertEqual(main.size, sub.size)

    def test_submanager_main_put_directories_overwrite(self):

        subManager = self.manager.submanager("/sub-directory")

        with self.get_dirpath() as directory:
            maindir = self.manager.put(directory, "/sub-directory/directory")

        subdir = subManager["/directory"]

        # Assert that the directories are consisten with each other
        self.assertEqual(len(maindir), len(subdir))

        artpath = lambda art: art.path
        for main, sub in zip(sorted(maindir, key=artpath), sorted(subdir, key=artpath)):

            self.assertEqual(main.modifiedTime, sub.modifiedTime)
            self.assertEqual(main.size, sub.size)

        # Put and overwrite
        with self.get_dirpath(['file2.txt', 'file3.txt']) as directory:

            with pytest.raises(stow.exceptions.OperationNotPermitted):
                maindir1 = self.manager.put(directory, "/sub-directory/directory")

            maindir1 = self.manager.put(directory, "/sub-directory/directory", overwrite=True)

        subdir1 = subManager["/directory"]

        self.assertIs(subdir, subdir1)
        self.assertIs(maindir, maindir1)

        # Assert that the directories are consisten with each other
        self.assertEqual(len(maindir), len(subdir))

        artpath = lambda art: art.path
        for main, sub in zip(sorted(maindir, key=artpath), sorted(subdir, key=artpath)):

            self.assertEqual(main.modifiedTime, sub.modifiedTime)
            self.assertEqual(main.size, sub.size)

    def test_submanager_main_put_directories_merge(self):

        subManager = self.manager.submanager("/sub-directory")

        with self.get_dirpath() as directory:
            maindir = self.manager.put(directory, "/sub-directory/directory")

        subdir = subManager["/directory"]

        # Assert that the directories are consisten with each other
        self.assertEqual(len(maindir), len(subdir))

        artpath = lambda art: art.path
        for main, sub in zip(sorted(maindir, key=artpath), sorted(subdir, key=artpath)):

            self.assertEqual(main.modifiedTime, sub.modifiedTime)
            self.assertEqual(main.size, sub.size)

        # Put and overwrite
        with self.get_dirpath(['file2.txt', 'file3.txt']) as directory:

            with pytest.raises(stow.exceptions.OperationNotPermitted):
                maindir1 = self.manager.put(directory, "/sub-directory/directory")

            maindir1 = self.manager.put(directory, "/sub-directory/directory", merge=True)

        subdir1 = subManager["/directory"]

        self.assertIs(subdir, subdir1)
        self.assertIs(maindir, maindir1)

        # Assert that the directories are consisten with each other
        self.assertEqual(len(maindir), len(subdir))

        artpath = lambda art: art.path
        for main, sub in zip(sorted(maindir, key=artpath), sorted(subdir, key=artpath)):

            self.assertEqual(main.modifiedTime, sub.modifiedTime)
            self.assertEqual(main.size, sub.size)

    def test_write_fail_behaviour_for_files(self):
        """ Test what happens when an error is produced in the writing of a file

        The intended behaviour is that the writes up to the error are pushed to the file
        """

        try:
            with self.manager.open("file.txt", "w") as handle:
                handle.write("line 1")
                raise ValueError("Error during write")
                handle.write("line 2")

        except ValueError:

            file = self.manager["file.txt"]

            self.assertEqual(file.size, 6)

            with self.manager.open("file.txt", "r") as handle:
                self.assertEqual(handle.read(), "line 1")

    def test_write_fail_behaviour_for_files(self):
        """ Test what happens when an error is produced in the writing of a file that has been localised

        The intended behaviour is that the writes up to the error are pushed to the file
        """

        try:
            with self.manager.localise("file.txt") as abspath:
                with open(abspath, "w") as handle:
                    handle.write("line 1")
                raise ValueError("Error during write")
                with open(abspath, "w") as handle:
                    handle.write("line 2")

        except ValueError:

            file = self.manager["file.txt"]

            self.assertEqual(file.size, 6)

            with self.manager.open("file.txt", "r") as handle:
                self.assertEqual(handle.read(), "line 1")

    # def test_write_fail_behaviour_for_directories (self):
    #     """ Test what happens when an error is produced in the writing of a file

    #     The intended behaviour is that the writes up to the error are pushed to the file
    #     """


    #     try:
    #         with self.manager.localise("directory") as abspath:
    #             os.mkdir(abspath)


    #             handle.write("line 1")
    #             raise ValueError("Error during write")
    #             handle.write("line 2")

    #     except ValueError:

    #         file = self.manager["file.txt"]

    #         self.assertEqual(file.size, 6)

    #         with self.manager.open("file.txt", "r") as handle:
    #             self.assertEqual(handle.read(), "line 1")

