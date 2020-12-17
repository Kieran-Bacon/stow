import unittest

import os
import pyini
import boto3
from botocore.exceptions import ClientError
import uuid
import tempfile

import stow
from stow.managers import Amazon

from .. import ETC_DIR
from .manager import ManagerTests, SubManagerTests

CONFIG_PATH = os.path.join(ETC_DIR, 'aws_credentials.ini')
BUCKET_NAME_INCLUDE = 'pykb-storage-test-bucket'

@unittest.skipIf(False or not os.path.exists(CONFIG_PATH), 'No credentials at {} to connect to aws'.format(CONFIG_PATH))
class Test_Amazon(unittest.TestCase, ManagerTests, SubManagerTests):

    @classmethod
    def setUpClass(cls):

        # Load aws information
        cls._config = pyini.ConfigParser().read(CONFIG_PATH)

        # Connect to aws
        cls.s3 = boto3.resource(
            's3',
            aws_access_key_id=cls._config['aws_access_key_id'],
            aws_secret_access_key=cls._config['aws_secret_access_key'],
            region_name=cls._config['region_name']
        )

        for bucket in cls.s3.buckets.all():

            if BUCKET_NAME_INCLUDE in bucket.name:
                # Found a valid bucket for testing use
                cls.bucket_name = bucket.name
                break

        else:
            # No bucket exists - create a new bucket

            counter = 0
            while True:
                bucket_name = '{}-{}'.format(uuid.uuid4(), BUCKET_NAME_INCLUDE)

                try:
                    cls.s3.create_bucket(
                        Bucket=bucket_name,
                        # CreateBucketConfiguration={'LocationConstraint': cls._config['region_name']}
                    )

                except ClientError as e:
                    if counter < 2:
                        counter += 1
                        continue
                    else:
                        raise RuntimeError('Failed to initialise a testing bucket')

                break

            cls.bucket_name = bucket_name

    def setUp(self):

        # Define the manager
        self.manager = Amazon(
            bucket=self.bucket_name,
            aws_access_key_id=self._config['aws_access_key_id'],
            aws_secret_access_key=self._config['aws_secret_access_key'],
            region_name=self._config['region_name']
        )

    def setUpWithFiles(self):
        # Make the managers local space to store files
        self.directory = tempfile.mkdtemp()

        # Define the manager
        self.manager = Amazon(
            bucket=self.bucket_name,
            aws_access_key_id=self._config['aws_access_key_id'],
            aws_secret_access_key=self._config['aws_secret_access_key'],
            # region_name=self._config['region_name']
        )

        with open(os.path.join(self.directory, "initial_file1.txt"), "w") as handle:
            handle.write("Content")

        os.mkdir(os.path.join(self.directory, "initial_directory"))
        with open(os.path.join(self.directory, "initial_directory", "initial_file2.txt"), "w") as handle:
            handle.write("Content")

        os.mkdir(os.path.join(self.directory, "directory-stack"))
        os.mkdir(os.path.join(self.directory, "directory-stack", "directory-stack"))
        with open(os.path.join(self.directory, "directory-stack", "directory-stack", "initial_file3.txt"), "w") as handle:
            handle.write("Content")

        for root, _, files in os.walk(self.directory):
            dir_name = root.replace(self.directory, "").replace("\\", "/").strip("/")
            for file in files:
                remote_path = "/".join([dir_name, file]) if dir_name else file
                self.manager._bucket.upload_file(os.path.join(root, file), remote_path)

    def tearDown(self):
        self.s3.Bucket(self.bucket_name).objects.delete()

    def test_connect_submanager(self):

        self.setUpWithFiles()

        sub_manager = stow.connect(
            "s3",
            submanager="/initial_directory",
            bucket=self.bucket_name,
            aws_access_key_id=self._config['aws_access_key_id'],
            aws_secret_access_key=self._config['aws_secret_access_key']
        )

        self.assertIsInstance(sub_manager, stow.SubManager)
        self.assertEqual(len(sub_manager.ls()), 1)

    def test_stateless_put_file_with_manager(self):
        # Test that when putting with the stateless interface that put actually works

        os.environ["AWS_ACCESS_KEY_ID"] = self._config['aws_access_key_id']
        os.environ["AWS_SECRET_ACCESS_KEY"] = self._config['aws_secret_access_key']

        path = "s3://{}/{}".format(self.bucket_name, "file_put.txt")

        with tempfile.TemporaryDirectory() as directory:
            with open(os.path.join(directory, "file.txt"), "w") as handle:
                handle.write("hello")

            stow.put(
                os.path.join(directory, "file.txt"),
                path
            )

        self.assertEqual(
            self.manager["/file_put.txt"].content.decode(), "hello"
        )


    def test_stateless_put_bytes_with_manager(self):
        # Test that when putting with the stateless interface that put actually works

        os.environ["AWS_ACCESS_KEY_ID"] = self._config['aws_access_key_id']
        os.environ["AWS_SECRET_ACCESS_KEY"] = self._config['aws_secret_access_key']

        path = "s3://{}/{}".format(self.bucket_name, "bytes_put.txt")

        stow.put(
            b"hello",
            path
        )

        self.assertEqual(
            self.manager["/bytes_put.txt"].content.decode(), "hello"
        )

