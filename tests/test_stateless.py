import unittest

import os
import tempfile
import uuid

import stow

class Test_Stateless(unittest.TestCase):

    def test_abspath(self):
        self.assertEqual(os.path.abspath("."), stow.abspath("."))
        self.assertEqual(os.path.abspath("filename"), stow.abspath("filename"))
        self.assertEqual(os.path.abspath(".."), stow.abspath(".."))

    def test_ls(self):
        arts = {os.path.basename(art.path) for art in stow.ls(".")}
        files = {filename for filename in os.listdir()}
        self.assertEqual(arts, files)

    def test_join(self):
        self.assertEqual(stow.join("hello//there", "buddy/"), "hello//there/buddy/")
        self.assertEqual(stow.join("hello", "/", "there"), "/there")
        self.assertEqual(stow.join("hello"), "hello")
        self.assertEqual(stow.join("", "hello"), "hello")
        self.assertEqual(stow.join("/", "/hello", "/"), "/")
        self.assertEqual(stow.join("/", "/", "/"), "/")
        self.assertEqual(stow.join("./example", "there"), "./example/there")
        self.assertEqual(stow.join("example", "there"), "example/there")
        self.assertEqual(stow.join("example", "/there"), "/there")
        self.assertEqual(stow.join('/', '/directory'), "/directory")
        self.assertEqual(stow.join('/', 'directory'), "/directory")
        self.assertEqual(stow.join('s3://example-bucket/a/b', 'hello', 'there'), "s3://example-bucket/a/b/hello/there")
        self.assertEqual(stow.join('s3://example-bucket/a/b', '/hello', 'there'), "s3://example-bucket/hello/there")
        self.assertEqual(stow.join('s3://example-bucket/a/b', 'c:/hello', 'there'), "c:/hello/there")
        self.assertEqual(stow.join("s3://example-location/directory", "filename.txt"), "s3://example-location/directory/filename.txt")

    def test_joinAbsolute(self):

        self.assertEqual(stow.join("/hello", "/there", "/buddy", joinAbsolutes=True), "/hello/there/buddy")
        self.assertEqual(stow.join('s3://example-bucket/a/b', 'c:/hello', 'there', joinAbsolutes=True), "c:/a/b/hello/there")
        self.assertEqual(stow.join('s3://example-bucket/a/b', 'hello', 'there', joinAbsolutes=True), "s3://example-bucket/a/b/hello/there")

    def test_put(self):

        with tempfile.TemporaryDirectory() as source, tempfile.TemporaryDirectory() as destination:

            sourceFile = os.path.join(source, "file1.txt")

            open(sourceFile, "w").close()

            file = stow.put(sourceFile, stow.join(destination, stow.basename(sourceFile)))

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











