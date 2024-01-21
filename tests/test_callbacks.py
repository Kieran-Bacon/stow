import unittest
from moto import mock_s3

import boto3
import tempfile

import stow
from stow.managers.amazon import Amazon

@mock_s3
class Test_ProgressCallback(unittest.TestCase):

    def setUp(self):

        self.s3 = boto3.client('s3')
        self.s3.create_bucket(
            Bucket="bucket_name",
            CreateBucketConfiguration={"LocationConstraint":"eu-west-2"}
        )

    def test_amazon_get(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="file.txt",
            Body=b"A file with some bytes",
        )

        self.s3.put_object(
            Bucket="bucket_name",
            Key="file2.txt",
            Body=b"another file with a lot more bytes than the first one",
        )

        s3 = Amazon('bucket_name')

        with tempfile.TemporaryDirectory() as directory:
            s3.get('/', stow.join(directory, 'download'), callback=stow.callbacks.ProgressCallback())

    def test_amazon_put(self):

        with tempfile.TemporaryDirectory() as directory:
            with open(stow.join(directory, 'file1.txt'), 'w') as handle:
                handle.write('A file with some bytes')

            with open(stow.join(directory, 'file2.txt'), 'w') as handle:
                handle.write('A file with some bytes')

            s3 = Amazon('bucket_name')
            s3.put(directory, '/directory', callback=stow.callbacks.ProgressCallback())

    def test_combined_callback(self):

        s3 = Amazon('bucket_name')

        combinedCallback = stow.callbacks.composeCallback([stow.callbacks.ProgressCallback()])

        with tempfile.TemporaryDirectory() as directory:
            for i in range(10):
                with open(stow.join(directory, f'{i}.txt'), 'w') as handle:
                    handle.write('A file with some bytes')

            s3.put(directory, '/directory', callback=combinedCallback)

            s3.rm('/directory', recursive=True, callback=combinedCallback)
