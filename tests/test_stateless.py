import unittest
import boto3
from moto import mock_s3

import os
import tempfile
import uuid
import datetime
import time

import stow
import stow.managers
from stow.managers.filesystem import FS
from stow.managers.amazon import Amazon

class Test_Stateless(unittest.TestCase):

    def test_open_object(self):
        """ Use stow open like a normal file handle """

        with tempfile.TemporaryDirectory() as directory:
            file_path = stow.join(directory, 'new_file.txt')
            fileDescriptor = stow.open(file_path, 'w')
            fileDescriptor.write("Hello there")
            fileDescriptor.close()

            fileDescriptor2 = open(file_path)
            self.assertEqual(os.path.splitdrive(fileDescriptor.name)[1], os.path.splitdrive(fileDescriptor2.name)[1])

            self.assertEqual(fileDescriptor2.read(), 'Hello there')

            fileDescriptor2.close()

    def test_find(self):

        filesystemManager = stow.find("FS")
        self.assertTrue(filesystemManager, FS)

        amazonS3 = stow.find("s3")
        self.assertTrue(amazonS3, Amazon)

        with self.assertRaises(ValueError):
            stow.find("missing")

    def test_connect(self):

        with tempfile.TemporaryDirectory() as directory:
            os.mkdir(os.path.join(directory, "directory1"))

            filesystem = stow.connect(manager="FS", path=directory)

            self.assertIsInstance(filesystem, stow.managers.FS)
            self.assertEqual(len(filesystem.ls()), 1)

    def test_parseURL(self):

        with tempfile.TemporaryDirectory() as directory:
            _, pdirectory = stow.splitdrive(directory)

            # Get the manager and path of the directory
            parsed = stow.parseURL(directory)
            self.assertIsInstance(parsed.manager, stow.managers.FS)
            self.assertEqual(stow.splitdrive(parsed.relpath)[1], pdirectory)



    def test_findManagers(self):
        # Check that

        stow.isfile(os.path.abspath(__file__))

        with self.assertRaises(TypeError):
            stow.isfile(("other://file.txt",)) # type: ignore

    def test_artefact(self):

        with tempfile.TemporaryDirectory() as directory:

            filepath = os.path.join(directory, "filename.txt")
            text = "hello there"

            with open(filepath, "w") as handle:
                handle.write(text)

            stats = os.stat(filepath)

            artefact = stow.artefact(filepath, type=stow.File)

            self.assertIsInstance(artefact, stow.File)
            self.assertEqual(artefact.size, len(text))

            self.assertEqual(datetime.datetime.utcfromtimestamp(stats.st_ctime), artefact.createdTime.replace(tzinfo=None))
            self.assertEqual(datetime.datetime.utcfromtimestamp(stats.st_mtime), artefact.modifiedTime.replace(tzinfo=None))
            self.assertAlmostEqual(
                datetime.datetime.utcfromtimestamp(stats.st_atime),
                artefact.accessedTime.replace(tzinfo=None),
                delta=datetime.timedelta(milliseconds=5)
            )

    def driveLessAssertEqual(self, strVal1, strVal2):
        self.assertEqual(os.path.splitdrive(strVal1)[1], os.path.splitdrive(strVal2)[1])


    def test_abspath(self):

        odrive, opath = os.path.splitdrive(os.path.abspath('.'))
        sdrive, spath = os.path.splitdrive(stow.abspath('.'))

        self.assertEqual(odrive.lower(), sdrive.lower())
        self.assertEqual(opath, spath)

        self.driveLessAssertEqual(os.path.abspath("."), stow.abspath("."))
        self.driveLessAssertEqual(os.path.abspath("filename"), stow.abspath("filename"))
        self.driveLessAssertEqual(os.path.abspath(".."), stow.abspath(".."))

        self.driveLessAssertEqual(stow.abspath("/hello/there"), os.path.abspath("/hello/there"))
        self.driveLessAssertEqual(stow.abspath("hello/there"), os.path.abspath("hello/there"))

        testfile = stow.artefact(__file__)
        self.driveLessAssertEqual(stow.abspath(testfile), os.path.abspath(__file__))

    def test_commonpath(self):

        self.assertEqual(
            stow.commonpath(["/hello/there", "/hello/there/buddy", "/hello/friend"]),
            os.path.commonpath(["/hello/there", "/hello/there/buddy", "/hello/friend"]),
            # "/hello"
        )

        # List the directory and check out the common path
        testDirectory = os.path.dirname(os.path.abspath(__file__))
        artefacts = [os.path.join(testDirectory, filename) for filename in os.listdir(testDirectory)]

        self.assertEqual(stow.commonpath(artefacts), testDirectory)

    def test_commonprefix(self):

        self.assertEqual(stow.commonprefix(["/hello/there", "/hello/there/buddy", "/hello/friend"]), "/hello/")
        self.assertEqual(stow.commonprefix(["/hello/there", "/hello/there/buddy", "/hello/thriend"]), "/hello/th")

        # List the directory and check out the common path
        testDirectory = os.path.dirname(os.path.abspath(__file__))
        artefacts = [os.path.join(testDirectory, filename) for filename in os.listdir(testDirectory)]

        self.assertEqual(stow.commonpath(artefacts), testDirectory)

    def test_digest(self):

        with tempfile.TemporaryDirectory() as directory:

            fp = os.path.join(directory, '1.txt')

            with open(fp, 'w') as handle:
                handle.write('hello')

            self.assertEqual(stow.digest(fp), '5d41402abc4b2a76b9719d911017c592')

            with self.assertRaises(TypeError):
                stow.digest(directory)

    def test_dirname_with_path(self):

        self.assertEqual(stow.dirname("/hello/there"), "/hello")
        self.assertEqual(stow.dirname("hello/there"), "hello")
        self.assertEqual(stow.dirname("./hello/there"), "./hello")
        self.assertEqual(stow.dirname("/"), os.path.dirname("/"))
        self.assertEqual(stow.dirname("s3://bucket/there"), "s3://bucket/")
        self.assertEqual(stow.dirname("s3://bucket/hello/there"), "s3://bucket/hello")
        self.assertEqual(stow.dirname("s3://hello/there?param1=value1"), "s3://hello/?param1=value1")

    def test_dirname_with_artefact(self):

        test_file = stow.artefact(__file__)
        self.assertEqual(os.path.splitdrive(stow.dirname(test_file))[1], os.path.splitdrive(os.path.dirname(__file__))[1])

    def test_expandusers(self):

        self.assertEqual(stow.expanduser("~/Documents"), os.path.expanduser("~/Documents"))
        self.assertEqual(stow.expanduser("~/${MODELS}/Documents"), os.path.expanduser("~/${MODELS}/Documents"))

    def test_expandvars(self):

        self.assertEqual(stow.expandvars("~/Documents"), os.path.expandvars("~/Documents"))
        self.assertEqual(stow.expandvars("~/${MODELS}/Documents"), os.path.expandvars("~/${MODELS}/Documents"))

    def test_isabs(self):

        self.assertEqual(stow.isabs("/hello/there"), True)
        self.assertEqual(stow.isabs("hello/there"), False)

        self.assertEqual(stow.isabs(os.path.join("/hello", "there")), True)
        self.assertEqual(stow.isabs(os.path.join("hello", "there")), False)

    def test_name(self):

        file = '/path/to/file.txt'
        self.assertEqual(stow.name(file), 'file')

        file = '/path/to/file'
        self.assertEqual(stow.name(file), 'file')

    def test_extensions(self):

        file = '/path/to/file.txt'
        self.assertEqual(stow.extension(file), 'txt')

        file = '/path/to/file'
        self.assertEqual(stow.extension(file), '')

    def test_normcase(self):

        targets = [
            "/hello/there",
            "/HeLLo/THeRe"
        ]

        for case in targets:
            self.assertEqual(stow.normcase(case), os.path.normcase(case))

    def test_normpath(self):

        targets = [
            "/hello/there",
            "/HeLLo/THeRe",
            "/hello/../there",
            "/hello/./there/okay"
        ]

        for case in targets:
            self.assertEqual(stow.normpath(case), os.path.normpath(case))

        self.assertEqual(stow.normpath("s3://bucket/hello/there?param1=value1"), "s3://bucket/hello/there?param1=value1")
        self.assertEqual(stow.normpath("s3://bucket/hello/./there/../okay?param1=value1"), "s3://bucket/hello/okay?param1=value1")

    def test_realpath(self):

        targets = [
            "/hello/there",
            "/HeLLo/THeRe",
            "/hello/../there",
            "/hello/./there/okay"
        ]

        for case in targets:
            self.assertEqual(stow.realpath(case), os.path.realpath(case))

    def test_relpath(self):

        for s in [
            ["/hello/there/buddy", "/hello/there"], # buddy
            ["/hello/buddy", "/hello/there"],  # ../buddy
            ["/hello/there", "/hello/there"], # .
        ]:
            self.assertEqual(stow.relpath(*s), os.path.relpath(*s))

    def test_samefile(self):

        file = stow.artefact(__file__)

        self.assertTrue(stow.samefile(__file__, __file__))
        self.assertTrue(stow.samefile(__file__, file))
        self.assertTrue(stow.samefile(file, file))

        f1, f2 = os.listdir()[:2]

        self.assertFalse(stow.samefile(f1, f2))
        self.assertFalse(stow.samefile(stow.artefact(f1), f2))
        self.assertFalse(stow.samefile(f1, stow.artefact(f2)))

    def test_sameopenfile(self):

        with tempfile.TemporaryDirectory() as directory:

            # Write files to check whether they are being openned
            with open(os.path.join(directory, "file1.txt"), "w") as handle:
                handle.write("content")

            with open(os.path.join(directory, "file2.txt"), "w") as handle:
                handle.write("content")

            fd1 = os.open(os.path.join(directory, "file1.txt"), os.O_RDONLY)
            fd2 = os.open(os.path.join(directory, "file1.txt"), os.O_RDONLY)

            file = open(os.path.join(directory, "file1.txt"))
            fd3 = file.fileno()

            self.assertTrue(stow.sameopenfile(fd1, fd2))
            self.assertTrue(stow.sameopenfile(fd1, fd3))

            fd3 = os.open(os.path.join(directory, "file2.txt"), os.O_RDONLY)

            self.assertFalse(stow.sameopenfile(fd1, fd3))

            os.close(fd1)
            os.close(fd2)
            os.close(fd3)
            file.close()

            print('what?')

    def test_samestat(self):

        file = stow.artefact(__file__)

        self.assertTrue(stow.samestat(os.stat(__file__), os.stat(__file__)))
        self.assertTrue(stow.samestat(os.stat(__file__), os.stat(file)))
        self.assertTrue(stow.samestat(os.stat(file), os.stat(file)))

        f1, f2 = os.listdir()[:2]

        self.assertFalse(stow.samestat(os.stat(f1), os.stat(f2)))
        self.assertFalse(stow.samestat(os.stat(stow.artefact(f1)), os.stat(f2)))
        self.assertFalse(stow.samestat(os.stat(f1), os.stat(stow.artefact(f2))))

    def test_split(self):

        for s in [
            "/hello/there/buddy.txt"
            "hello/there/buddy.txt"
        ]:
            self.assertEqual(stow.split(s), os.path.split(s))

        if os.name == 'nt':
            a, b = stow.split(stow.artefact(__file__))
            A, B = os.path.split(os.path.abspath(__file__))

            self.assertEqual(stow.abspath(a)[1:], A[1:])
            self.assertEqual(b, B)
        else:
            self.assertEqual(stow.split(stow.artefact(__file__)), os.path.split(os.path.abspath(__file__)))

    def test_splitdirve(self):

        teststring = [
            "c:/dir",
            "C:/hello/there",
            "C:\\hello\\there"
        ]

        for string in teststring:
            self.assertEqual(stow.splitdrive(string), os.path.splitdrive(string), msg=string)

    def test_splitext(self):

        self.assertEqual(stow.splitext("hello.txt"), ("hello", ".txt"))
        self.assertEqual(stow.splitext("hello.txt"), os.path.splitext("hello.txt"))

    def test_isfile(self):

        with tempfile.TemporaryDirectory() as directory:
            filepath = os.path.join(directory, "file.txt")

            with open(filepath, "w") as handle:
                handle.write("Hero")

            self.assertTrue(stow.isfile(filepath))
            self.assertTrue(stow.isfile(stow.artefact(filepath)))
            self.assertFalse(stow.isfile(directory))

    def test_isdir(self):

        with tempfile.TemporaryDirectory() as directory:
            filepath = os.path.join(directory, "file.txt")

            with open(filepath, "w") as handle:
                handle.write("Hero")

            self.assertTrue(stow.isdir(directory))
            self.assertTrue(stow.isdir(stow.artefact(directory)))
            self.assertFalse(stow.isdir(filepath))

    def test_createLink(self):

        with tempfile.TemporaryDirectory() as directory:
            file = stow.touch(stow.join(directory, 'file1.txt'))

            linked_file = stow.mklink(file, stow.join(directory, 'file-linked.txt'))

            stow.islink(linked_file)
            os.path.islink(linked_file.abspath)


    def test_islink(self):

        with tempfile.TemporaryDirectory() as directory:
            filepath = os.path.join(directory, "file.txt")

            with open(filepath, "w") as handle:
                handle.write("Hero")

            linkpath = os.path.join(directory, "file-link.txt")
            os.symlink(filepath, linkpath)

            self.assertFalse(stow.islink(filepath))
            self.assertFalse(stow.islink(directory))

            self.assertFalse(stow.islink(stow.artefact(filepath)))
            self.assertFalse(stow.islink(stow.artefact(directory)))

            self.assertTrue(stow.islink(linkpath))
            self.assertTrue(stow.islink(stow.artefact(linkpath)))

    def test_ismount(self):

        if os.name == 'nt':
            self.assertTrue(stow.ismount('G:\\'))
            self.assertFalse(stow.ismount(stow.expanduser('~')))
        else:
            self.assertTrue(stow.ismount('/dev'))
            self.assertFalse(stow.ismount(stow.expanduser('~')))

    def test_getctime(self):

        with tempfile.TemporaryDirectory() as directory:
            filepath = os.path.join(directory, "file.txt")

            with open(filepath, "w") as handle:
                handle.write("Hero")

            self.assertAlmostEqual(stow.getctime(directory), os.path.getctime(directory), places=5)
            self.assertAlmostEqual(stow.getctime(filepath), os.path.getctime(filepath), places=5)
            self.assertAlmostEqual(stow.getctime(stow.artefact(directory)), os.path.getctime(directory), places=5)
            self.assertAlmostEqual(stow.getctime(stow.artefact(filepath)), os.path.getctime(filepath), places=5)

    def test_getmtime(self):

        with tempfile.TemporaryDirectory() as directory:
            filepath = os.path.join(directory, "file.txt")

            with open(filepath, "w") as handle:
                handle.write("Hero")

            self.assertAlmostEqual(stow.getmtime(directory), os.path.getmtime(directory), places=5)
            self.assertAlmostEqual(stow.getmtime(filepath), os.path.getmtime(filepath), places=5)
            self.assertAlmostEqual(stow.getmtime(stow.artefact(directory)), os.path.getmtime(directory), places=5)
            self.assertAlmostEqual(stow.getmtime(stow.artefact(filepath)), os.path.getmtime(filepath), places=5)

    def test_setmtime(self):

        with tempfile.TemporaryDirectory() as directory:
            filepath = os.path.join(directory, "file.txt")

            with open(filepath, "w") as handle:
                handle.write("Hero")

            timestamp = time.time()
            stow.setmtime(filepath, timestamp)
            self.assertEqual(os.path.getmtime(filepath), timestamp)

            timestamp = datetime.datetime.now()
            stow.setmtime(filepath, timestamp)
            self.assertEqual(os.path.getmtime(filepath), timestamp.timestamp())

    def test_getatime(self):

        with tempfile.TemporaryDirectory() as directory:
            filepath = os.path.join(directory, "file.txt")

            with open(filepath, "w") as handle:
                handle.write("Hero")

            self.assertAlmostEqual(stow.getatime(directory), os.path.getatime(directory), places=5)
            self.assertAlmostEqual(stow.getatime(filepath), os.path.getatime(filepath), places=5)

            self.assertAlmostEqual(stow.getatime(stow.artefact(directory)), os.path.getatime(directory), places=5)
            self.assertAlmostEqual(stow.getatime(stow.artefact(filepath)), os.path.getatime(filepath), places=5)

    def test_setatime(self):

        with tempfile.TemporaryDirectory() as directory:
            filepath = os.path.join(directory, "file.txt")

            with open(filepath, "w") as handle:
                handle.write("Hero")

            timestamp = time.time()
            stow.setatime(filepath, timestamp)
            self.assertEqual(os.path.getatime(filepath), timestamp)

            timestamp = datetime.datetime.now()
            stow.setatime(filepath, timestamp)
            self.assertEqual(os.path.getatime(filepath), timestamp.timestamp())

    def test_exists(self):

        with tempfile.TemporaryDirectory() as directory:
            filepath = os.path.join(directory, "file.txt")

            with open(filepath, "w") as handle:
                handle.write("Hero")

            filepath2 = os.path.join(directory, "file2.txt")

            self.assertTrue(stow.exists(filepath))
            self.assertTrue(stow.exists(directory))
            self.assertFalse(stow.exists(filepath2))

            self.assertTrue(stow.exists(stow.artefact(filepath)))
            self.assertTrue(stow.exists(stow.artefact(directory)))

    def test_lexists(self):

        with tempfile.TemporaryDirectory() as directory:
            filepath = os.path.join(directory, "file.txt")

            with open(filepath, "w") as handle:
                handle.write("Hero")

            filepath2 = os.path.join(directory, "file2.txt")

            self.assertTrue(stow.lexists(filepath))
            self.assertTrue(stow.lexists(directory))
            self.assertFalse(stow.lexists(filepath2))

            self.assertTrue(stow.lexists(stow.artefact(filepath)))
            self.assertTrue(stow.lexists(stow.artefact(directory)))

    def test_touch(self):

        with tempfile.TemporaryDirectory() as directory:
            filepath = stow.join(directory, "file.txt")
            file = stow.touch(filepath)
            stats = os.stat(filepath)


            dt = datetime.datetime(2020,10,10,10,10,10, tzinfo=datetime.timezone.utc)
            updated = stow.touch(file, dt)

            self.assertEqual(updated.modifiedTime, dt)

    def test_mkdir(self):

        with tempfile.TemporaryDirectory() as directory:
            directorypath = stow.join(directory, "dir1")

            file = stow.mkdir(directorypath)

            stats = os.stat(directorypath)

    def test_mkdir_exceptions(self):
        with tempfile.TemporaryDirectory() as directory:
            fp = stow.join(directory, '1.txt')
            stow.touch(fp)

            with self.assertRaises(stow.exceptions.OperationNotPermitted):
                stow.mkdir(fp)



    def test_localise(self):

        with tempfile.TemporaryDirectory() as directory:

            sd = lambda x: stow.splitdrive(x)[1]

            with stow.localise(directory) as abspath:
                self.assertEqual(sd(directory), sd(abspath))

            with stow.localise(stow.artefact(directory)) as abspath:
                self.assertEqual(sd(directory), sd(abspath))


    def test_ls(self):
        arts = {os.path.basename(art.path) for art in stow.ls(".")}
        files = {filename for filename in os.listdir()}
        self.assertEqual(arts, files)

    def test_ls_none(self):
        arts = {os.path.basename(art.path) for art in stow.ls()}
        files = {filename for filename in os.listdir()}
        self.assertEqual(arts, files)


    def test_join(self):

        self.assertEqual(stow.join(), '')

        for s in [
            ("hello//there", "buddy/"),
            ("hello", "/", "there"),
            ("hello"),
            ("", "hello"),
            ("/", "/hello", "/"),
            ("/", "/", "/"),
            ("./example", "there"),
            ("example", "there"),
            ("example", "/there"),
            ('/', '/directory'),
            ('/', 'directory'),
        ]:
            self.assertEqual(stow.join(*s), os.path.join(*s))

        for s, t in [
            (('s3://example-bucket/a/b', 'hello', 'there'), 's3://example-bucket/a/b/hello/there'),
            (('s3://example-bucket/a/b', '/hello', 'there'), 's3://example-bucket/hello/there'),
            (('s3://example-bucket/a/b', 'c:/hello', 'there'), 'c:/hello/there'),
            (("s3://example-location/directory", "filename.txt"), "s3://example-location/directory/filename.txt"),
        ]:
            self.assertEqual(stow.join(*s, separator='/'), t)

    def test_joinAbsolute(self):

        for s in [
            [["/hello", "/there", "/buddy"], "/hello/there/buddy", "/hello/there/buddy"],
            [['s3://example-bucket/a/b', 'c:/hello', 'there'], "c:/a/b/hello/there", "c:/a/b/hello\\there"],
            [['s3://example-bucket/a/b', 'hello', 'there'], "s3://example-bucket/a/b/hello/there", "s3://example-bucket/a/b\\hello\\there"],
        ]:

            target = s[2] if os.name == "nt" else s[1]

            self.assertEqual(stow.join(*s[0], joinAbsolutes=True), target)

    def test_joinURLs(self):

        for s in [
            [['s3://bucket/here?storage_class=STANDARD_IA', 'there'], 's3://bucket/here/there?storage_class=STANDARD_IA']
        ]:

            self.assertEqual(stow.join(*s[0], joinAbsolutes=True, separator='/'), s[1])

    def test_joiningWithArtefacts(self):

        with tempfile.TemporaryDirectory() as directory:

            dir1 = stow.mkdir(stow.join(directory, 'sub'))

            if os.name == 'nt':
                self.assertEqual(stow.abspath(stow.join(dir1, 'file1'))[1:], os.path.join(directory, 'sub', 'file1')[1:])
            else:
                self.assertEqual(stow.join(dir1, 'file1'), os.path.join(directory, 'sub', 'file1'))

    def test_put(self):

        with tempfile.TemporaryDirectory() as source, tempfile.TemporaryDirectory() as destination:

            sourceFile = os.path.join(source, "file1.txt")

            open(sourceFile, "w").close()

            file = stow.put(sourceFile, stow.join(destination, stow.basename(sourceFile)))

            if os.name == 'nt':
                self.assertEqual(os.path.abspath(file.path)[1:], os.path.join(destination, "file1.txt")[1:])
            else:
                self.assertEqual(file.path, os.path.join(destination, "file1.txt"))

            self.assertEqual(
                set(os.listdir(destination)),
                {
                    "file1.txt"
                }
            )

    def test_put_relative(self):

        relfiles = [
            "{}".format(uuid.uuid4()),
            ".{}{}".format(os.sep, uuid.uuid4())
        ]

        try:
            with tempfile.TemporaryDirectory() as destination:

                for i, relpath in enumerate(relfiles):

                    with open(relpath, "w") as handle:
                        handle.write("Content")

                    file = stow.put(relpath, stow.join(destination, "file{}.txt".format(i)))

                    if os.name == 'nt':
                        self.assertEqual(os.path.abspath(file.path)[1:], os.path.join(destination, "file{}.txt".format(i))[1:])
                    else:
                        self.assertEqual(file.path, os.path.join(destination, "file{}.txt".format(i)))

                self.assertEqual(
                    set(os.listdir(destination)),
                    {
                        "file0.txt",
                        "file1.txt",
                    }
                )
        finally:
            for path in relfiles:
                if os.path.exists(path): os.remove(path)

    def test_get(self):

        with tempfile.TemporaryDirectory() as source, tempfile.TemporaryDirectory() as destination:

            with self.assertRaises(stow.exceptions.ArtefactTypeError):
                stow.get(source)

            sourceFile = os.path.join(source, "file1.txt")

            open(sourceFile, "w").close()

            stow.get(sourceFile, stow.join(destination, stow.basename(sourceFile)))

            self.assertEqual(
                set(os.listdir(destination)),
                {
                    "file1.txt"
                }
            )

    def test_open(self):

        with tempfile.TemporaryDirectory() as source:

            filename = stow.join(source, str(uuid.uuid4()))
            with stow.open(filename, "w") as handle:
                handle.write("content")

            with open(filename, "r") as handle:
                self.assertEqual(handle.read(), "content")

    def test_cp(self):

        with tempfile.TemporaryDirectory() as directory:

            filepath1 = stow.join(directory, "file1.txt")
            filepath2 = stow.join(directory, "file2.txt")
            filepath3 = stow.join(directory, "file3.txt")

            content = "somecontent to check"

            with open(filepath1, "w") as handle:
                handle.write(content)

            stow.cp(filepath1, filepath2)

            with open(filepath2) as handle:
                self.assertEqual(content, handle.read())

            stow.cp(stow.artefact(filepath1), filepath3)

            with stow.open(filepath3) as handle:
                self.assertEqual(content, handle.read())

    def test_cpOverwrite(self):

        with tempfile.TemporaryDirectory() as dir1, tempfile.TemporaryDirectory() as dir2:
            dir1_file = stow.join(dir1, 'file1.txt')
            dir2_file = stow.join(dir2, 'file1.txt')

            stow.touch(dir1_file)
            stow.touch(dir2_file)

            # This will replace the second file
            stow.cp(dir1_file, dir2_file)

            self.assertTrue(stow.exists(dir1_file, dir2_file))

            with self.assertRaises(stow.exceptions.OperationNotPermitted):
                stow.cp(dir1, dir2)

            stow.cp(dir1, dir2, overwrite=True)

    @mock_s3
    def test_cp_between_managers(self):

        s3 = boto3.client('s3')
        s3.create_bucket(
            Bucket="bucket_name",
            CreateBucketConfiguration={"LocationConstraint":"eu-west-2"}
        )

        with tempfile.TemporaryDirectory() as directory:

            file = stow.join(directory, 'file1.txt')
            with open(file, 'w') as handle:
                handle.write('content')

            stow.cp(file, 's3://bucket_name/file1.txt')

            self.assertEqual(stow.get('s3://bucket_name/file1.txt'), b'content')


    def test_mv(self):

        with tempfile.TemporaryDirectory() as directory:

            filepath1 = stow.join(directory, "file1.txt")
            filepath2 = stow.join(directory, "file2.txt")
            filepath3 = stow.join(directory, "file3.txt")

            content = "somecontent to check"

            with open(filepath1, "w") as handle:
                handle.write(content)

            stow.mv(filepath1, filepath2)

            self.assertFalse(stow.exists(filepath1))

            with open(filepath2) as handle:
                self.assertEqual(content, handle.read())

            stow.mv(stow.artefact(filepath2), filepath3)

            self.assertFalse(stow.exists(filepath2))

            with stow.open(filepath3, "r") as handle:
                self.assertEqual(content, handle.read())

    def test_mvOverwrite(self):

        with tempfile.TemporaryDirectory() as dir1, tempfile.TemporaryDirectory() as dir2:
            dir1_file = stow.join(dir1, 'file1.txt')
            dir2_file = stow.join(dir2, 'file1.txt')

            stow.touch(dir1_file)
            stow.touch(dir2_file)

            # This will replace the second file
            stow.mv(dir1_file, dir2_file)

            with self.assertRaises(stow.exceptions.OperationNotPermitted):
                stow.mv(dir1, dir2)

            stow.mv(dir1, dir2, overwrite=True)


    @mock_s3
    def test_mv_between_managers(self):

        s3 = boto3.client('s3')
        s3.create_bucket(
            Bucket="bucket_name",
            CreateBucketConfiguration={"LocationConstraint":"eu-west-2"}
        )

        with tempfile.TemporaryDirectory() as directory:

            file = stow.join(directory, 'file1.txt')
            with open(file, 'w') as handle:
                handle.write('content')

            stow.mv(file, 's3://bucket_name/file1.txt')

            self.assertEqual(stow.get('s3://bucket_name/file1.txt'), b'content')

    def test_sync(self):

        with tempfile.TemporaryDirectory() as directory:

            # File one should not be copied by the second file
            stow.touch(stow.join(directory, "dir1", "file1.txt"))
            file = stow.touch(stow.join(directory, "dir2", "file1.txt"))
            file.content(b"content")

            # File two should be replaced the second
            file = stow.touch(stow.join(directory, "dir1", "file2.txt"))
            stow.touch(stow.join(directory, "dir2", "file2.txt"))
            time.sleep(.001)
            file.content(b"file2")

            # File should stay
            file = stow.touch(stow.join(directory, "dir2", "file3.txt"))
            file.content(b"Original")

            # File should be copied in
            file = stow.touch(stow.join(directory, "dir1", "file4.txt"))
            file.content(b"copied")

            stow.sync(
                stow.join(directory, "dir1"),
                stow.join(directory, "dir2")
            )

            self.assertEqual(stow.artefact(stow.join(directory, "dir2", "file1.txt")).content(), b"content")
            self.assertEqual(stow.artefact(stow.join(directory, "dir2", "file2.txt")).content(), b"file2")
            self.assertEqual(stow.artefact(stow.join(directory, "dir2", "file3.txt")).content(), b"Original")
            self.assertEqual(stow.artefact(stow.join(directory, "dir2", "file4.txt")).content(), b"copied")

    def test_sync_with_delete(self):

        with tempfile.TemporaryDirectory() as directory:

            # File one should not be copied by the second file
            stow.touch(stow.join(directory, "dir1", "file1.txt"))
            file = stow.touch(stow.join(directory, "dir2", "file1.txt"))
            file.content(b"content")

            # File two should be replaced the second
            file = stow.touch(stow.join(directory, "dir1", "file2.txt"))
            stow.touch(stow.join(directory, "dir2", "file2.txt"))
            time.sleep(.001)
            file.content(b"file2")

            # File should stay
            file = stow.touch(stow.join(directory, "dir2", "file3.txt"))
            file.content(b"Original")


            stow.sync(
                stow.join(directory, "dir1"),
                stow.join(directory, "dir2"),
                delete=True
            )

            self.assertEqual(stow.artefact(stow.join(directory, "dir2", "file1.txt")).content(), b"content")
            self.assertEqual(stow.artefact(stow.join(directory, "dir2", "file2.txt")).content(), b"file2")
            with self.assertRaises(stow.exceptions.ArtefactNotFound):
                stow.artefact(stow.join(directory, "dir2", "file3.txt"))

    def test_sync_to_non_existent_location(self):

        with tempfile.TemporaryDirectory() as directory:

            stow.touch(stow.join(directory, 'dir1', 'hello.txt'))

            stow.sync(
                stow.join(directory, 'dir1'),
                stow.join(directory, 'dir2')
            )

            self.assertTrue(stow.exists(stow.join(directory, 'dir2', 'hello.txt')))

    def test_sync_overwrite(self):

        with tempfile.TemporaryDirectory() as directory:

            stow.mkdir(stow.join(directory, 'dir1', 'there'))
            stow.touch(stow.join(directory, 'dir2', 'there'))


            stow.sync(
                stow.join(directory, 'dir1'),
                stow.join(directory, 'dir2')
            )

            stow.touch(stow.join(directory, 'dir1', 'hello'))
            stow.mkdir(stow.join(directory, 'dir2', 'hello'))
            stow.mkdir(stow.join(directory, 'dir2', 'hello', 'file1.txt'))

            with self.assertRaises(stow.exceptions.OperationNotPermitted):
                stow.sync(
                    stow.join(directory, 'dir1'),
                    stow.join(directory, 'dir2')
                )

            stow.sync(
                stow.join(directory, 'dir1'),
                stow.join(directory, 'dir2'),
                overwrite=True
            )

    def test_rm(self):

        with tempfile.TemporaryDirectory() as directory:

            filepath1 = stow.join(directory, "file1.txt")
            filepath2 = stow.join(directory, "file2.txt")

            stow.touch(filepath1)
            file = stow.touch(filepath2)

            self.assertTrue(stow.exists(filepath1))
            self.assertTrue(stow.exists(file))

            stow.rm(filepath1)
            stow.rm(file)

            self.assertFalse(stow.exists(filepath1))
            self.assertFalse(stow.exists(file))

    def test_ls_exceptions(self):

        with tempfile.TemporaryDirectory() as directory:
            fp = stow.join(directory, '1.txt')
            fp2 = stow.join(directory, '2.txt')
            obj = stow.touch(fp)

            with self.assertRaises(TypeError):
                stow.ls(obj)

            with self.assertRaises(stow.exceptions.ArtefactNotFound):
                stow.ls(fp2)

            stow.ls(fp2, ignore_missing=True)

    def test_set_artefact_timestamps(self):

        with tempfile.TemporaryDirectory() as directory:
            fp = stow.join(directory, '1.txt')

            file = stow.touch(fp)
            md = file.modifiedTime
            time.sleep(0.001)

            stow.set_artefact_time(file, None, None)
            self.assertTrue(file.modifiedTime > md)



