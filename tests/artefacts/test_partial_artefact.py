import pytest
import unittest

import os
import stow
import tempfile

from . import BasicSetup


class Test_PartialArtefact(BasicSetup, unittest.TestCase):

    def test_noLongerExisting(self):

        partial = self.manager.touch('/file-1')
        self.manager.rm('/file-1')

        with pytest.raises(stow.exceptions.ArtefactNoLongerExists):
            partial.content

    def test_worksWithOsPath(self):
        # Ensure that the partial artefacts are compatible with with fspath like the main artefacts are

        with tempfile.TemporaryDirectory() as directory:

            partial_artefact = stow.touch(stow.join(directory, 'testfile.txt'))

            self.assertEqual(stow.splitdrive(directory)[1], stow.splitdrive(os.path.dirname(partial_artefact))[1])
