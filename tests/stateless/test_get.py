import unittest

import os
import tempfile

import stow

class Test_GetStateless(unittest.TestCase):

    def test_get_from_local(self):
        """ Test getting a local file """

        with tempfile.TemporaryDirectory() as source, tempfile.TemporaryDirectory() as destination:

            sourceFile = os.path.join(source, "file1.txt")

            open(sourceFile, "w").close()

            stow.get(sourceFile, stow.join(destination, stow.basename(sourceFile)))

            self.assertEqual(
                set(os.listdir(destination)),
                {
                    "file1.txt"
                }
            )

    def test_get_from_remote(self):
        """ Test getting a remote file """

        with tempfile.TemporaryDirectory() as directory:



            stow.get(
                "s3://2745af67-bf0e-4a12-b623-2298e5f92d2f-pykb-storage-test-bucket")



    def test_get_local_bytes(self):
        """ Get local file bytes """

    def test_get_remote_bytes(self):
        """ Get remote file bytes """


