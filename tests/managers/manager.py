import os
import tempfile
import unittest

import storage

class ManagerTests:

    def setUp(self):
        self.manager = storage.interfaces.Manager

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
            self.assertIsInstance(file, storage.artefacts.File)

            # Pull the file down again
            self.manager.get('/test1.txt', localOutFP)

            with open(localOutFP, 'r') as fh:
                self.assertEqual(fh.read(), content)

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
            self.assertIsInstance(file_b, storage.artefacts.File)
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

            self.assertIsInstance(art, storage.artefacts.Directory)
