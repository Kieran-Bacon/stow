import unittest

import os
import better
import boto3
from botocore.exceptions import ClientError
import uuid

from .. import ETC_DIR
from .manager import ManagerTests

import storage

BUCKET_NAME_INCLUDE = 'pykb-storage-test-bucket'

class Test_Amazon(unittest.TestCase, ManagerTests):

    @classmethod
    def setUpClass(cls):

        # Load aws information
        cls._config = better.ConfigParser().read(os.path.join(ETC_DIR, 'aws_credentials.ini'))

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
                        CreateBucketConfiguration={'LocationConstraint': cls._config['region_name']}
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
        self.manager = storage.connect(
            'test',
            manager='AWS',
            bucket=self.bucket_name,
            aws_access_key_id=self._config['aws_access_key_id'],
            aws_secret_access_key=self._config['aws_secret_access_key'],
            region_name=self._config['region_name']
        )

    def tearDown(self):
        self.s3.Bucket(self.bucket_name).objects.delete()

    def test_toAWSPath(self):
        pass

    def test_fromAWSPath(self):
        pass

    def test_dirpath(self):
        self.assertEqual(storage.managers.amazon.dirpath('/file1'), '/')

