import unittest

import os
import tempfile
import shutil
import random
import string
import time
import pyini
import paramiko

import stow
from stow.managers import SSH

from .. import ETC_DIR
from .manager import ManagerTests, SubManagerTests

CONFIG_PATH = os.path.join(ETC_DIR, 'ssh_credentials.ini')

@unittest.skipIf(False or not os.path.exists(CONFIG_PATH), 'No credentials at {} to connect to an SSH server'.format(CONFIG_PATH))
class Test_SecureShell(unittest.TestCase, ManagerTests, SubManagerTests):

    def setUp(self):

        self.config = pyini.ConfigParser().read(CONFIG_PATH)

        self.manager = SSH(**self.config)

    def setUpWithFiles(self):
        # Make the managers local space to store files

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        client.connect(
            hostname=self.config.get("hostname"),
            port=self.config.get("port", 22),
            username=self.config.get("username"),
            password=self.config.get("password"),
            pkey=self.config.get("privateKey"),
            key_filename=self.config.get("privateKeyFilePath"),
            timeout=self.config.get("timeout"),
        )

        client.exec_command("; ".join([
            f"mkdir {self.config['root']}",
            f"cd {self.config['root']}",
            "echo 'Content' >> initial_file1.txt",
            "mkdir initial_directory",
            "echo 'Content' >> initial_directory/initial_file2.txt",
            "mkdir directory-stack",
            "mkdir directory-stack/directory-stack",
            "echo 'Content' >> directory-stack/directory-stack/initial_file3.txt"
        ]), 5)

    def tearDown(self):

        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=self.config.get("hostname"),
                port=self.config.get("port", 22),
                username=self.config.get("username"),
                password=self.config.get("password"),
                pkey=self.config.get("privateKey"),
                key_filename=self.config.get("privateKeyFilePath"),
                timeout=self.config.get("timeout"),
            )

            client.exec_command(f"rm -rf {self.config['root']}")
        except:
            pass
