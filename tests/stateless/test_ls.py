import unittest

import os
import tempfile

import stow

class Test_GetStateless(unittest.TestCase):

    def test_ls(self):

        sets = stow.ls('docs')

        print(sets)

