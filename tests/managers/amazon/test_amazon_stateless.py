import unittest

import os
import tempfile

import stow

from . import getBucketObjects

class Test_AmazonStateless(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.s3, cls.bucket, cls.bucket_name = getBucketObjects()

    def tearDown(self) -> None:
        self.s3.Bucket(self.bucket_name).objects.delete()

    def test_get(self):

        # Create a file to download
        self.bucket.put_object(
            Key='file.txt',
            Body=b'contents',
        )

        with tempfile.TemporaryDirectory() as directory:

            file = stow.get('s3://{}/file.txt'.format(self.bucket_name), stow.join(directory, 'file1.txt'))

            self.assertIsInstance(file, stow.File)
            self.assertEqual(os.listdir(directory), ['file1.txt'])

            self.assertEqual(file.content, b'contents')

    def test_get_bytes(self):

        # Create a file to download
        self.bucket.put_object(
            Key='file.txt',
            Body=b'contents',
        )

        fbytes = stow.get('s3://{}/file.txt'.format(self.bucket_name))

        self.assertIsInstance(fbytes, bytes)
        self.assertEqual(fbytes, b'contents')



