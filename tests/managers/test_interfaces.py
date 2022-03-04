import unittest

import os
import datetime
import tempfile

import stow
from stow.class_interfaces import LocalInterface, RemoteInterface

class Test_LocalIntefaces(unittest.TestCase):

    def test_fspath(self):

        with tempfile.TemporaryDirectory() as directory:

            test_file = os.path.join(directory, 'example.txt')

            with open(test_file, 'w') as handle:
                handle.write('here')

            class M(LocalInterface):
                def _abspath(self, path): return test_file

            file = stow.File(M(), 'something', 0, datetime.datetime.utcnow())

            self.assertAlmostEqual(os.path.getatime(file), datetime.datetime.utcnow().timestamp(), places=1)

class Test_RemoteInterface(unittest.TestCase):

    def test_fspath(self):
        """ Test that when an artefact is created that it cannot be used via the fspath method since it doesn't have a
        local implementation """

        file = stow.File(RemoteInterface(), 'path', 0, datetime.datetime.utcnow())

        with self.assertRaises(stow.exceptions.ArtefactNotAvailable):
            os.path.getatime(file)