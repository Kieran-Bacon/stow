import unittest
import unittest.mock

import os
import tempfile

import stow
import stow.utils
from stow.managers import FS

class Resource:
    name = "fs"

    @staticmethod
    def load():
        return FS

class Test_UtilFunctions(unittest.TestCase):

    @unittest.mock.patch('pkg_resources.iter_entry_points')
    def test_findFS(self, package_iter):

        # Overload entrypoint loader to return this entry
        package_iter.side_effect = lambda x: [Resource()]

        # Ensure that the manager hasn't already been cached
        stow.utils.MANAGERS = {}

        # Test that this returns the manager class
        managerClass = stow.utils.find("FS")

        # Check that the
        self.assertEqual(managerClass, FS)

        # Check that the package iter was only called once
        self.assertEqual(package_iter.call_count, 1)

        # Test that this returns the manager class
        managerClass = stow.utils.find("FS")

        # Check that the
        self.assertEqual(managerClass, FS)

        # Check that the package iter was only called once
        self.assertEqual(package_iter.call_count, 1)

    def test_findFails(self):

        with self.assertRaises(ValueError):
            stow.utils.find("Somethingthatdoesntexist")

    @unittest.mock.patch('stow.utils.find')
    def test_connect(self, mockFind):

        # Add a mock layer to the mock object
        mockFind.side_effect = lambda x: FS

        # Ensure that the cache is clear
        # stow.utils.connect.cache_clear()

        with tempfile.TemporaryDirectory() as directory:
            os.makedirs(os.path.join(directory, "demo"))

            # Create a fs
            manager = stow.utils.connect(manager="FS", path=directory)

            # We had to find the manager and return it
            self.assertEqual(mockFind.call_count, 1)

            managerB = stow.utils.connect(manager="FS", path=directory)

            # Assert that there is caching of the params at the connect level
            self.assertEqual(mockFind.call_count, 1)
            self.assertIs(manager, managerB)

            # Craete a submanager
            managerC = stow.utils.connect(manager="FS", path=directory, submanager="/demo")

            # Assert that there is caching of the params at the connect level
            # print(stow.utils.connect.cache_info())
            # print(stow.utils.connect.cache_parameters())
            self.assertEqual(mockFind.call_count, 1)
            self.assertIsNot(manager, managerC)
            self.assertIs(manager, managerC._owner)

    def test_parseURL(self):

        with tempfile.TemporaryDirectory() as directory:
            manager, relpath = stow.parseURL(directory)

            self.assertIsInstance(manager, FS)
            self.assertEqual(manager._abspath(manager[relpath].path), directory)

    def test_parseURLAWS(self):



        with tempfile.TemporaryDirectory() as directory:
            manager, relpath = stow.parseURL(directory)

            self.assertIsInstance(manager, FS)
            self.assertEqual(manager._abspath(manager[relpath].path), directory)