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

    def setUp(self) -> None:
        self.pkg_patcher = unittest.mock.patch('pkg_resources.iter_entry_points')
        self.package_iter = self.pkg_patcher.start()
        self.package_iter.side_effect = lambda x: [Resource()]

        stow.Manager._clearManagerCache()

    def tearDown(self) -> None:
        self.pkg_patcher.stop()

    # @unittest.mock.patch('pkg_resources.iter_entry_points')
    def test_findFS(self):

        # Test that this returns the manager class
        managerClass = stow.find("FS")

        # Check that the
        self.assertEqual(managerClass, FS)

        # Check that the package iter was only called once
        self.assertEqual(self.package_iter.call_count, 1)

        # Test that this returns the manager class
        managerClass = stow.find("FS")

        # Check that the
        self.assertEqual(managerClass, FS)

        # Check that the package iter was only called once
        self.assertEqual(self.package_iter.call_count, 1)

    def test_findFails(self):

        with self.assertRaises(ValueError):
            stow.find("Somethingthatdoesntexist")

    def test_connect(self):

        with tempfile.TemporaryDirectory() as directory:
            os.makedirs(os.path.join(directory, "demo"))

            # Create a fs
            manager = stow.connect(manager="FS", path=directory)

            # We had to find the manager and return it
            self.assertEqual(self.package_iter.call_count, 1)

            managerB = stow.connect(manager="FS", path=directory)

            # Assert that there is caching of the params at the connect level
            self.assertEqual(self.package_iter.call_count, 1)
            self.assertIs(manager, managerB)

            # Create another FS manager
            managerC = stow.connect(manager="FS", path=os.path.join(directory, 'submanager'))

            self.assertEqual(self.package_iter.call_count, 1)
            self.assertIsNot(manager, managerC)

    def test_parseURL(self):

        with tempfile.TemporaryDirectory() as directory:
            parsedURL = stow.parseURL(directory)

            self.assertIsInstance(parsedURL.manager, FS)
            self.assertEqual(stow.splitdrive(parsedURL.relpath)[1], stow.splitdrive(directory)[1])

            # self.assertIsInstance(manager, FS)
            # if os.name == 'nt':
            #     self.assertEqual(manager._abspath(manager[relpath].path)[1:], directory[1:])
            # else:
            #     self.assertEqual(manager._abspath(manager[relpath].path), directory)

