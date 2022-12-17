""" Test the amazon underlying manager """

from multiprocessing.sharedctypes import Value
import os
import io
import tempfile

import unittest
import pytest
from moto import mock_s3
from moto.core import set_initial_no_auth_action_count

import boto3
from botocore.exceptions import ClientError

import stow.testing
import stow.exceptions
from stow.managers.amazon import Amazon

@mock_s3
class Test_Amazon(unittest.TestCase):

    def setUp(self):

        self.s3 = boto3.client('s3')
        self.s3.create_bucket(
            Bucket="bucket_name",
            CreateBucketConfiguration={"LocationConstraint":"eu-west-2"}
        )

    def test_aws_session(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="file.txt",
            Body=b"",
        )

        manager = Amazon("bucket_name", aws_session=boto3.Session())
        self.assertTrue(manager.exists("/file.txt"))

    def test_invalid_filenames(self):

        manager = Amazon("bucket_name")

        with pytest.raises(ValueError):
            manager.touch('/file%.txt')

    def test_get_file(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="file.txt",
            Body=b"",
        )

        manager = Amazon("bucket_name")
        file = manager['/file.txt']

        self.assertIsInstance(file, stow.File)

    def test_get_directory(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="directory/",
            Body=b"",
        )

        manager = Amazon("bucket_name")
        directory = manager['/directory']

        self.assertIsInstance(directory, stow.Directory)

    def test_get_missing(self):

        manager = Amazon("bucket_name")

        with pytest.raises(stow.exceptions.ArtefactNotFound):
            manager["/file.txt"]

    @set_initial_no_auth_action_count(1)
    def test_get_forbiddenAccess(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="file.txt",
            Body=b"",
        )

        manager = Amazon("bucket_name")
        with pytest.raises(ClientError):
            _ = manager['/file.txt']

    def test_metadata(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="file.txt",
            Body=b"",
            Metadata={'key': 'value'}
        )

        manager = Amazon("bucket_name")
        file = manager['/file.txt']

        self.assertEqual({'key': 'value'}, file.metadata)

    def test_metadata_missing(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="file.txt",
            Body=b"",
            Metadata={'key': 'value'}
        )

        manager = Amazon("bucket_name")
        file = list(manager.ls('/'))[0]  # This method returns objects that would need to metadata to be loaded

        self.s3.delete_object(
            Bucket="bucket_name",
            Key="file.txt"
        )

        with pytest.raises(stow.exceptions.ArtefactNoLongerExists):
            file.metadata

        with pytest.raises(stow.exceptions.ArtefactNoLongerExists):
            manager._metadata('/file-2.txt')

    @set_initial_no_auth_action_count(5)
    def test_metadata_forbidden(self):
        manager = Amazon('bucket_name')
        manager.touch('/file.txt')

        with pytest.raises(ClientError):
            manager._metadata('/file.txt')



    def test_exists(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="file.txt",
            Body=b"",
        )

        manager = Amazon("bucket_name")
        self.assertTrue(manager.exists('/file.txt'))

    def test_exists_missing(self):

        manager = Amazon("bucket_name")
        self.assertFalse(manager.exists('/file.txt'))

    def test_link(self):

        manager = Amazon('bucket_name')
        self.assertFalse(manager.islink("/file.txt"))

    def test_mount(self):

        manager = Amazon('bucket_name')
        self.assertFalse(manager.ismount("/file.txt"))

    def test_abspath(self):

        manager = Amazon("bucket_name")

        self.assertEqual(manager.abspath("/file.txt"), "s3://bucket_name/file.txt")

    def test_download_file(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="file.txt",
            Body=b"This is the content of the file",
        )

        manager = Amazon("bucket_name")

        with tempfile.TemporaryDirectory() as directory:
            manager.get('/file.txt', stow.join(directory, 'file.txt'))

    def test_download_directory(self):

        keys = ["directory/file1.txt", "directory/file2.txt", "directory/sub-directory/file3.txt"]

        for key in keys:

            self.s3.put_object(
                Bucket="bucket_name",
                Key=key,
                Body=b"This is the content of the file",
            )

        manager = Amazon('bucket_name')

        with tempfile.TemporaryDirectory() as directory:
            manager.get('/directory', directory, overwrite=True)

            for key in keys:
                self.assertTrue(os.path.exists(os.path.join(directory, key[10:])))

    def test_download_bytes(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key='file.txt',
            Body=b"This is the content of the file",
        )

        self.assertEqual(b"This is the content of the file", Amazon('bucket_name').get('/file.txt'))


    def test_upload_file(self):

        with tempfile.TemporaryDirectory() as directory:

            local_path = os.path.join(directory, 'file.txt')

            with open(local_path, "w") as handle:
                handle.write('Content')

            manager = Amazon("bucket_name")
            manager.put(local_path, '/file.txt')

        # Fetch the file bytes and write them to the buffer
        bytes_buffer = io.BytesIO()
        self.s3.download_fileobj(
            Bucket="bucket_name",
            Key="file.txt",
            Fileobj=bytes_buffer
        )
        self.assertEqual(b"Content", bytes_buffer.getvalue())

    def test_upload_file_callback(self):

        with tempfile.TemporaryDirectory() as directory:

            if os.name == "nt":
                directory = directory[:directory.index(':')].lower() + directory[directory.index(':'):]

            local_path = os.path.join(directory, 'file.txt')

            with open(local_path, "w", encoding="utf-8") as handle:
                handle.write('Content')

            manager = Amazon("bucket_name")
            manager.put(local_path, '/file.txt', Callback=stow.testing.mock.TestCallback)


        raise ValueError(f"{stow.testing.mock.TestCallback.artefacts.keys()}")
        data = stow.testing.mock.TestCallback.artefacts[local_path]

        self.assertEqual(data['artefact'].path, local_path)
        self.assertFalse(data['is_downloading'])
        self.assertEqual(data['bytes_transferred'], 7)

    def test_upload_directory(self):

        with tempfile.TemporaryDirectory() as directory:

            with open(os.path.join(directory, 'file.txt'), 'w') as handle:
                handle.write('content')

            with open(os.path.join(directory, 'file2.txt'), 'w') as handle:
                handle.write('content')

            os.mkdir(os.path.join(directory, 'sub-directory'))

            with open(os.path.join(directory, 'sub-directory', 'file3.txt'), 'w') as handle:
                handle.write('content')

            manager = Amazon('bucket_name')
            manager.put(directory, '/upload')

            self.s3.head_object(Bucket='bucket_name', Key='upload/file.txt')
            self.s3.head_object(Bucket='bucket_name', Key='upload/file2.txt')
            self.s3.head_object(Bucket='bucket_name', Key='upload/sub-directory/file3.txt')



    def test_upload_bytes(self):

        written_bytes = b"These are some contents bytes"

        manager = Amazon("bucket_name")
        manager.put(written_bytes, "/file.txt")

        bytes_buffer = io.BytesIO()

        # Fetch the file bytes and write them to the buffer
        self.s3.download_fileobj(
            Bucket="bucket_name",
            Key="file.txt",
            Fileobj=bytes_buffer
        )

        self.assertEqual(b"These are some contents bytes", bytes_buffer.getvalue())



