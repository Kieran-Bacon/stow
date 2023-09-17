import unittest

import io
import boto3
from moto import mock_s3

import stow
from stow.manager.manager import Manager
from stow.managers.amazon import Amazon

class Test_BaseManager(unittest.TestCase):

    def test_wrongTypeRaiseError(self):

        manager = Manager()

        with self.assertRaises(TypeError):
            manager.mklink(10, 'path')


class Test_LocalManager(unittest.TestCase):
    pass

@mock_s3
class Test_RemoteManager(unittest.TestCase):

    def setUp(self):

        self.s3 = boto3.client('s3')
        self.s3.create_bucket(
            Bucket="bucket_name",
            CreateBucketConfiguration={"LocationConstraint":"eu-west-2"}
        )

    def test_localisation_with_context(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="file.txt",
            Body=b"here are some file bytes",
        )

        s3 = Amazon('bucket_name')

        with s3.localise('/file.txt') as abspath:
            with open(abspath) as handle:
                self.assertEqual(handle.read(), "here are some file bytes")

    def test_localisation_without_context(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="file.txt",
            Body=b"here are some file bytes",
        )

        s3 = Amazon('bucket_name')

        localiser = s3.localise('/file.txt')
        abspath = localiser.start()

        with open(abspath) as handle:
            self.assertEqual(handle.read(), "here are some file bytes")

        localiser.close()

    def test_localisation_hierarchy(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="directory/file.txt",
            Body=b"here are some file bytes",
        )

        self.s3.put_object(
            Bucket="bucket_name",
            Key="directory/dir/1.txt",
            Body=b"here are some file bytes",
        )

        self.s3.put_object(
            Bucket="bucket_name",
            Key="directory/dir/2.txt",
            Body=b"here are some file bytes",
        )

        s3 = Amazon('bucket_name')

        localiser = s3.localise('/directory')
        abspath = localiser.start()

        with open(stow.join(abspath, 'file.txt')) as handle:
            self.assertEqual(handle.read(), "here are some file bytes")

        # Delete the file
        stow.rm(stow.join(abspath, 'dir', '1.txt'))

        # Edit a file
        with open(stow.join(abspath, 'dir', '1.txt'), 'w') as handle:
            handle.write('fun')

        # Add a file
        stow.touch(stow.join(abspath, 'dir', '3.txt'))

        localiser.close()

    def test_localisation_add_file(self):

        s3 = Amazon('bucket_name')

        with s3.localise('/directory/file.txt') as abspath:
            with open(abspath, 'w') as handle:
                handle.write('file data!')

        bytes_buffer = io.BytesIO()
        self.s3.download_fileobj(
            Bucket="bucket_name",
            Key="directory/file.txt",
            Fileobj=bytes_buffer
        )
        self.assertEqual(b"file data!", bytes_buffer.getvalue())

    def test_localisation_overwrite_file(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="directory/file.txt",
            Body=b"here are some file bytes",
        )

        s3 = Amazon('bucket_name')

        with s3.localise('/directory/file.txt') as abspath:
            with open(abspath, 'w') as handle:
                handle.write('file data!')

        bytes_buffer = io.BytesIO()
        self.s3.download_fileobj(
            Bucket="bucket_name",
            Key="directory/file.txt",
            Fileobj=bytes_buffer
        )
        self.assertEqual(b"file data!", bytes_buffer.getvalue())

    def test_localisation_overwrite_file_in_hierarchy(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="directory/file.txt",
            Body=b"here are some file bytes",
        )

        s3 = Amazon('bucket_name')

        with s3.localise('/directory') as abspath:
            with open(s3.join(abspath, 'file.txt'), 'w') as handle:
                handle.write('file data!')

        bytes_buffer = io.BytesIO()
        self.s3.download_fileobj(
            Bucket="bucket_name",
            Key="directory/file.txt",
            Fileobj=bytes_buffer
        )
        self.assertEqual(b"file data!", bytes_buffer.getvalue())


