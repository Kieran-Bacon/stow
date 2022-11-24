""" Test the artefact """

import os
import shutil
import tempfile

import stow

class BasicSetup:
    """ Define the default setup for tests """

    def setUp(self):

        self.directory = tempfile.mkdtemp()

        # Create a file
        self.filepath = os.path.join(self.directory, 'file1')
        self.filetext = 'Another one bits the dust'
        with open(self.filepath, 'w') as handle:
            handle.write(self.filetext)

        # Make a directory
        os.mkdir(os.path.join(self.directory, 'directory1'))

        self.manager = stow.connect(manager='FS', path=self.directory)

    def tearDown(self):
        shutil.rmtree(self.directory)