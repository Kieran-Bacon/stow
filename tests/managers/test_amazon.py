""" Test the amazon underlying manager """

from multiprocessing.sharedctypes import Value
import os
import io
import tempfile

import binascii
import hashlib
import zlib

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

    def test_root(self):

        manager = Amazon('bucket_name')
        self.assertEqual('bucket_name', manager.root)

        file = stow.touch("s3://bucket_name/file.txt")
        self.assertEqual('bucket_name', file.manager.root)

    def test_get_root_directory(self):

        manager = Amazon('bucket_name')

        self.assertIsInstance(manager.artefact('/'), stow.Directory)
        self.assertTrue(manager.exists('/'), stow.Directory)

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
        self.assertEqual("/file.txt", file.path)

    def test_get_directory(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="directory/",
            Body=b"",
        )

        manager = Amazon("bucket_name")
        directory = manager['/directory']

        self.assertIsInstance(directory, stow.Directory)
        self.assertEqual("/directory", directory.path)

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

        metadata = file.metadata
        metadata.pop('ETag')

        self.assertEqual({'key': 'value'}, metadata)

        file = list(manager.ls())[0]

        self.assertEqual({'key': 'value'}, metadata)

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

    @set_initial_no_auth_action_count(3)
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

        self.s3.put_object(Bucket='bucket_name', Key='file.txt', Body=b'here')

        manager = Amazon('bucket_name')
        self.assertFalse(manager.islink("/file.txt"))

    def test_mount(self):

        self.s3.put_object(Bucket='bucket_name', Key='directory/', Body=b'here')

        manager = Amazon('bucket_name')
        self.assertFalse(manager.ismount("/directory"))

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

        keys = [
            "directory/file1.txt",
            "directory/file2.txt",
            "directory/sub-directory/file3.txt",
            "directory/empty-directory/"
        ]

        for key in keys:

            self.s3.put_object(
                Bucket="bucket_name",
                Key=key,
                Body=b"This is the content of the file",
            )

        manager = Amazon('bucket_name')

        with tempfile.TemporaryDirectory() as directory:
            manager.get('/directory', directory, overwrite=True)

            self.assertTrue(os.path.exists(os.path.join(directory, 'file1.txt')))
            self.assertTrue(os.path.exists(os.path.join(directory, 'file2.txt')))
            self.assertTrue(os.path.exists(os.path.join(directory, 'sub-directory/file3.txt')))
            self.assertTrue(os.path.exists(os.path.join(directory, 'empty-directory')))

    def test_download_bytes(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key='file.txt',
            Body=b"This is the content of the file",
        )

        self.assertEqual(b"This is the content of the file", Amazon('bucket_name').get('/file.txt'))

    def test_download_max_keys_reached(self):
        """ Download more than the max keys sixe of the manager """

        for i in range(50):

            self.s3.put_object(
                Bucket="bucket_name",
                Key=f'file-{i}.txt',
                Body=b"This is the content of the file",
            )

        manager = Amazon('bucket_name', max_keys=25)

        self.assertEqual(
            {f"/file-{i}.txt" for i in range(50)},
            {art.path for art in manager.ls()}
        )

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

    def test_upload_file_with_metadata(self):

        with tempfile.TemporaryDirectory() as directory:

            local_path = os.path.join(directory, 'file.txt')

            with open(local_path, "w") as handle:
                handle.write('Content')

            manager = Amazon("bucket_name")
            manager.put(local_path, '/file.txt', metadata={"key": "value"})

        # Get file metadata
        file_metadata = self.s3.head_object(Bucket='bucket_name', Key='file.txt')
        self.assertEqual(file_metadata['Metadata'], {'key': 'value'})

        # Fetch the file bytes and write them to the buffer
        bytes_buffer = io.BytesIO()
        self.s3.download_fileobj(
            Bucket="bucket_name",
            Key="file.txt",
            Fileobj=bytes_buffer
        )
        self.assertEqual(b"Content", bytes_buffer.getvalue())

    def test_upload_directory(self):

        with tempfile.TemporaryDirectory() as directory:

            with open(os.path.join(directory, 'file.txt'), 'w') as handle:
                handle.write('content')

            with open(os.path.join(directory, 'file2.txt'), 'w') as handle:
                handle.write('content')

            os.mkdir(os.path.join(directory, 'sub-directory'))
            os.mkdir(os.path.join(directory, 'second-directory'))

            with open(os.path.join(directory, 'sub-directory', 'file3.txt'), 'w') as handle:
                handle.write('content')

            manager = Amazon('bucket_name')
            manager.put(directory, '/upload')

            self.s3.head_object(Bucket='bucket_name', Key='upload/file.txt')
            self.s3.head_object(Bucket='bucket_name', Key='upload/file2.txt')
            self.s3.head_object(Bucket='bucket_name', Key='upload/sub-directory/file3.txt')

            self.assertSetEqual(
                {
                    '/upload',
                    '/upload/file.txt',
                    '/upload/file2.txt',
                    '/upload/sub-directory',
                    '/upload/sub-directory/file3.txt',
                    '/upload/second-directory'
                },
                {art.path for art in manager.ls(recursive=True)}
            )

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

    def test_ls_root(self):

        self.s3.put_object(Bucket="bucket_name", Key="source/empty-directory/", Body=b"")
        self.s3.put_object(Bucket="bucket_name", Key="source/file.txt", Body=b"")
        self.s3.put_object(Bucket="bucket_name", Key="file.txt", Body=b"")

        s3 = Amazon('bucket_name')

        self.assertEqual(
            {a.path for a in s3.ls()},
            {'/source', '/file.txt'}
        )

        self.assertEqual(
            {a.path for a in s3.ls(recursive=True)},
            {'/source', '/file.txt','/source/empty-directory', '/source/file.txt'}
        )

        self.assertEqual(
            {a.path for a in stow.ls('s3://bucket_name')},
            {'/source', '/file.txt'}
        )


    def test_ls_empty_directory(self):
        """ Test that the way the console will create empty directories correctly comes back as a directory only """

        self.s3.put_object(Bucket="bucket_name", Key="source/empty-directory/", Body=b"")

        manager = Amazon('bucket_name')

        expected_artefacts = {
            '/source': stow.Directory,
            '/source/empty-directory': stow.Directory
        }

        for artefact in manager.ls(recursive=True):
            expected_type = expected_artefacts.pop(artefact.path)
            self.assertIsInstance(artefact, expected_type)

        if expected_artefacts:
            raise ValueError(f'Expected artefacts remaining {list(expected_artefacts.keys())}')

    def test_copy_file(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key='file.txt',
            Body=b"This is the content of the file",
        )

        manager = Amazon('bucket_name')

        manager.cp('/file.txt', '/file-copied.txt')

        bytes_buffer = io.BytesIO()

        # Fetch the file bytes and write them to the buffer
        self.s3.download_fileobj(
            Bucket="bucket_name",
            Key="file-copied.txt",
            Fileobj=bytes_buffer
        )

        self.assertTrue(manager.exists('/file.txt'))
        self.assertTrue(manager.exists('/file-copied.txt'))

        self.assertEqual(b"This is the content of the file", bytes_buffer.getvalue())

    def test_copy_directory(self):

        self.s3.put_object(Bucket="bucket_name", Key="source/file-1.txt", Body=b"")
        self.s3.put_object(Bucket="bucket_name", Key="source/sub-directory/file-2.txt", Body=b"")
        self.s3.put_object(Bucket="bucket_name", Key="source/empty-directory/", Body=b"")

        manager = Amazon('bucket_name')

        manager.cp('/source', '/destination')

        self.assertEqual(
            {
                "/source",
                "/source/file-1.txt",
                "/source/sub-directory",
                "/source/sub-directory/file-2.txt",
                "/source/empty-directory",
                "/destination",
                "/destination/file-1.txt",
                "/destination/sub-directory",
                "/destination/sub-directory/file-2.txt",
                "/destination/empty-directory",
            },
            {art.path for art in manager.ls(recursive=True)}
        )

    def test_move_file(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key='file.txt',
            Body=b"This is the content of the file",
        )

        manager = Amazon('bucket_name')

        manager.mv('/file.txt', '/file-copied.txt')

        bytes_buffer = io.BytesIO()

        # Fetch the file bytes and write them to the buffer
        self.s3.download_fileobj(
            Bucket="bucket_name",
            Key="file-copied.txt",
            Fileobj=bytes_buffer
        )

        self.assertFalse(manager.exists('/file.txt'))
        self.assertTrue(manager.exists('/file-copied.txt'))

        self.assertEqual(b"This is the content of the file", bytes_buffer.getvalue())

    def test_move_directory(self):

        self.s3.put_object(Bucket="bucket_name", Key="source/file-1.txt", Body=b"")
        self.s3.put_object(Bucket="bucket_name", Key="source/sub-directory/file-2.txt", Body=b"")
        self.s3.put_object(Bucket="bucket_name", Key="source/empty-directory/", Body=b"")

        manager = Amazon('bucket_name')

        manager.mv('/source', '/destination')

        expected_artefacts = {
            "/destination": stow.Directory,
            "/destination/file-1.txt": stow.File,
            "/destination/sub-directory": stow.Directory,
            "/destination/sub-directory/file-2.txt": stow.File,
            "/destination/empty-directory": stow.Directory,
        }

        for artefact in manager.ls(recursive=True):
            expected_type = expected_artefacts.pop(artefact.path)
            self.assertIsInstance(artefact, expected_type)

        if expected_artefacts:
            raise ValueError(f'Remaining expected artefacts: {list(expected_artefacts)}')

    def test_remove_file(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key='file.txt',
            Body=b"This is the content of the file",
        )

        manager = Amazon('bucket_name')
        manager.rm('/file.txt')

        try:
            self.s3.head_object(Bucket="bucket_name", Key='file.txt')

        except ClientError as e:
            self.assertEqual(e.response['ResponseMetadata']['HTTPStatusCode'], 404)

    def test_remove_directory_empty(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="directory/empty/",
            Body=b""
        )

        manager = Amazon('bucket_name')
        self.assertIsInstance(manager['/directory/empty'], stow.Directory)

        manager.rm('/directory/empty')

        try:
            self.s3.head_object(Bucket="bucket_name", Key='directory/empty/')

        except ClientError as e:
            self.assertEqual(e.response['ResponseMetadata']['HTTPStatusCode'], 404)

    def test_remove_directory(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="directory/file-1.txt",
            Body=b"Content"
        )

        self.s3.put_object(
            Bucket="bucket_name",
            Key="directory/file-2.txt",
            Body=b"Content"
        )

        self.s3.put_object(
            Bucket="bucket_name",
            Key="directory/sub-directory/",
            Body=b""
        )

        manager = Amazon('bucket_name')

        with pytest.raises(stow.exceptions.OperationNotPermitted):
            manager.rm('/directory')

        manager.rm('/directory', recursive=True)

    def test_toConfig(self):

        sess = boto3.Session()

        manager = Amazon('bucket_name')
        self.assertDictEqual({
            "manager": 'AWS',
            'bucket': 'bucket_name',
            'aws_access_key': 'foobar_key',
            'aws_secret_key': 'foobar_secret',
            'aws_session_token': None,
            'region_name': sess.region_name,
            'profile_name': os.environ.get('AWS_PROFILE', 'default'),
            }, manager.toConfig())

    def test_digest_md5(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="file-1.txt",
            Body=b"Content"
        )

        manager = Amazon('bucket_name')

        with pytest.raises(NotImplementedError):
            manager['/file-1.txt'].digest(stow.HashingAlgorithm.MD5)

        with pytest.raises(NotImplementedError):
            manager.digest('/file-1.txt', stow.HashingAlgorithm.MD5)

    def test_digest_sha1(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="file-1.txt",
            Body=b"Content",
            ChecksumAlgorithm="SHA1"
        )

        manager = Amazon('bucket_name')

        art_checksum = manager['/file-1.txt'].digest(stow.HashingAlgorithm.SHA1)
        man_checksum = manager.digest('/file-1.txt', stow.HashingAlgorithm.SHA1)
        sha1_checksum = hashlib.sha1(b'Content').hexdigest()


        self.assertEqual(art_checksum, man_checksum)
        self.assertEqual(sha1_checksum, man_checksum)

    def test_digest_sha256(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="file-1.txt",
            Body=b"Content",
            ChecksumAlgorithm="SHA256"
        )

        manager = Amazon('bucket_name')

        art_checksum = manager['/file-1.txt'].digest(stow.HashingAlgorithm.SHA256)
        man_checksum = manager.digest('/file-1.txt', stow.HashingAlgorithm.SHA256)
        sha1_checksum = hashlib.sha256(b'Content').hexdigest()


        self.assertEqual(art_checksum, man_checksum)
        self.assertEqual(sha1_checksum, man_checksum)

    def test_digest_sha256(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="file-1.txt",
            Body=b"Content",
            ChecksumAlgorithm="SHA256"
        )

        manager = Amazon('bucket_name')

        art_checksum = manager['/file-1.txt'].digest(stow.HashingAlgorithm.SHA256)
        man_checksum = manager.digest('/file-1.txt', stow.HashingAlgorithm.SHA256)
        sha256_checksum = hashlib.sha256(b'Content').hexdigest()

        self.assertEqual(art_checksum, man_checksum)
        self.assertEqual(sha256_checksum, man_checksum)

    def test_digest_crc32(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="file-1.txt",
            Body=b"Content",
            ChecksumAlgorithm="CRC32"
        )

        manager = Amazon('bucket_name')

        art_checksum = manager['/file-1.txt'].digest(stow.HashingAlgorithm.CRC32)
        man_checksum = manager.digest('/file-1.txt', stow.HashingAlgorithm.CRC32)
        crc32_checksum = hex(binascii.crc32(b'Content') & 0xFFFFFFFF)[2:]

        self.assertEqual(art_checksum, man_checksum)
        self.assertEqual(crc32_checksum, man_checksum)

    @unittest.skip("No crc32c certificate")
    def test_digest_crc32c(self):

        self.s3.put_object(
            Bucket="bucket_name",
            Key="file-1.txt",
            Body=b"Content",
            ChecksumAlgorithm="CRC32C"
        )

        manager = Amazon('bucket_name')

        art_checksum = manager['/file-1.txt'].digest(stow.HashingAlgorithm.CRC32C)
        man_checksum = manager.digest('/file-1.txt', stow.HashingAlgorithm.CRC32C)
        # crc32c_checksun = hex(zlib.crc32(b'Content') & 0xffffffff)[2:]
        from crc32c import crc32c
        crc32c_checksum = crc32c(b'Content')

        self.assertEqual(art_checksum, man_checksum)
        self.assertEqual(crc32c_checksum, man_checksum)

    def test_mv_between_buckets(self):

        self.s3.create_bucket(
            Bucket="bucket_name_2",
            CreateBucketConfiguration={"LocationConstraint":"eu-west-2"}
        )

        self.s3.put_object(
            Bucket="bucket_name",
            Key="file-1.txt",
            Body=b"Content",
        )

        self.s3.put_object(
            Bucket="bucket_name",
            Key="file-2.txt",
            Body=b"Content",
        )

        stow.mv('s3://bucket_name/file-1.txt', 's3://bucket_name_2/file-1.txt')

        self.assertFalse(stow.exists('s3://bucket_name/file-1.txt'))
        self.assertTrue(stow.exists('s3://bucket_name_2/file-1.txt'))

        manager = Amazon('bucket_name')
        manager2 = Amazon('bucket_name_2')

        manager.mv('/file-2.txt', 's3://bucket_name_2/file-2.txt')
        self.assertFalse(manager.exists('/file-1.txt'))
        self.assertTrue(manager2.exists('s3://bucket_name_2/file-1.txt'))