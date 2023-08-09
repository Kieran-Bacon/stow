import os
import tempfile
import shutil

import stow

class BasicSetup:

    def setUp(self):

        self.directory = os.path.splitdrive(tempfile.mkdtemp())
        self.directory = self.directory[0].lower() + self.directory[1]

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
