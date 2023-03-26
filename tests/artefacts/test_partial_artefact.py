import pytest
import unittest

import stow

from . import BasicSetup


class Test_PartialArtefact(BasicSetup, unittest.TestCase):

    def test_noLongerExisting(self):

        partial = self.manager.touch('/file-1')
        self.manager.rm('/file-1')

        with pytest.raises(stow.exceptions.ArtefactNoLongerExists):
            partial.content



