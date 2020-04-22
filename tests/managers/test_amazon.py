import unittest

import os
import pyini
import boto3
from botocore.exceptions import ClientError
import uuid
import tempfile

import storage
from storage.managers import Amazon

from .. import ETC_DIR
from .manager import ManagerTests, SubManagerTests

CONFIG_PATH = os.path.join(ETC_DIR, 'aws_credentials.ini')
BUCKET_NAME_INCLUDE = 'pykb-storage-test-bucket'

@unittest.skipIf(not os.path.exists(CONFIG_PATH), 'No credentials at {} to connect to aws'.format(CONFIG_PATH))
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
            # region_name=cls._config['region_name']
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
            # region_name=self._config['region_name']
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

        self.manager._bucket.upload_file(
            os.path.join(self.directory, "initial_file1.txt"),
            "initial_file1.txt"
        )

        os.mkdir(os.path.join(self.directory, "initial_directory"))
        with open(os.path.join(self.directory, "initial_directory", "initial_file2.txt"), "w") as handle:
            handle.write("Content")

        self.manager._bucket.upload_file(
            os.path.join(self.directory, "initial_directory", "initial_file2.txt"),
            "initial_directory/initial_file2.txt"
        )

    def tearDown(self):
        self.s3.Bucket(self.bucket_name).objects.delete()

    def test_abspath(self):

        paths = [
            ('/hello/kieran', 'hello/kieran'),
            ('/hello/kieran', 'hello/kieran'),
            (r'\what\the\hell', 'what/the/hell'),
            (r'C:\\what\\the\\something', 'what/the/something'),
            ('s3://path/like/this', 'path/like/this')
        ]


        for i, o in paths:
            self.assertEqual(self.manager.abspath(i), o)
    def test_relPath(self):

        paths = [
            ('/hello/kieran', '/hello/kieran'),
            ('/hello/kieran/', '/hello/kieran'),
            (r'\what\the\hell', '/what/the/hell'),
            (r'C:\\what\\the\\hell', '/what/the/hell'),
            ('s3://path/like/this', '/path/like/this')
        ]


        for i, o in paths:
            self.assertEqual(self.manager.relpath(i), o)

    def test_basename(self):

        paths = [
            ('/hello/kieran', 'kieran'),
            ('/hello/', 'hello'),
            (r'\what\the\hell', 'hell'),
            (r'C:\\what\\the\\something', 'something'),
            ('s3://path/like/this', 'this')
        ]


        for i, o in paths:
            self.assertEqual(self.manager.basename(i), o)

    def test_connect_submanager(self):

        self.setUpWithFiles()

        sub_manager = storage.connect(
            "s3",
            submanager="/initial_directory",
            bucket=self.bucket_name,
            aws_access_key_id=self._config['aws_access_key_id'],
            aws_secret_access_key=self._config['aws_secret_access_key']
        )

        self.assertIsInstance(sub_manager, storage.manager.SubManager)
        self.assertEqual(len(sub_manager.ls()), 1)