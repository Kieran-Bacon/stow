import unittest

import os

import storage

class Test_Stateless(unittest.TestCase):

    def test_ls(self):
        arts = {os.path.basename(art.path) for art in storage.ls(".")}
        files = {filename for filename in os.listdir()}
        self.assertEqual(arts, files)