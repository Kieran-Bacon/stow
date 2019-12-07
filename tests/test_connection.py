import unittest

import os
import tempfile
import datetime

import storage

class Test_Files(unittest.TestCase):

    def test_connection(self):

        with tempfile.TemporaryDirectory() as directory:

            # Add some files to the temporary directory to see that they appear
            open(os.path.join(directory, 'file1'), 'w').close()

            fs = storage.connect('local', manager='FS', path=directory)
            #fs = storage.managers.FS('local', directory)

            print(fs.ls())

            self.fail()