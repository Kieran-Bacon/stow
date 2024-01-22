import unittest
from click.testing import CliRunner

import os
import tempfile
import time
from stow.cli import cli


# def test_error_entry_point_handled():

#     import pkg_resources
#     # Create the fake entry point definition
#     ep = pkg_resources.EntryPoint.parse('dummy = dummy_module:DummyPlugin')

#     # Create a fake distribution to insert into the global working_set
#     d = pkg_resources.Distribution()

#     # Add the mapping to the fake EntryPoint
#     d._ep_map = {'stow_managers': {'dummy': ep}}

#     # Add the fake distribution to the global working_set
#     pkg_resources.working_set.add(d, 'dummy')

#     from stow.cli import cli
#     runner = CliRunner()
#     result = runner.invoke(cli, ['--help'])
#     assert result.exit_code == 0
#     assert result.output == 'Hello Peter!\n'

class Test_CLI(unittest.TestCase):

    def setUp(self) -> None:
        self.runner = CliRunner()

    def test_debug_command(self):

        result = self.runner.invoke(cli, ['--debug', 'exists', 'README.md'])

        assert result.exit_code == 0
        assert result.output == (
            # "Debugging enabled\n"  # Apparently logs don't show up in the testing
            "README.md                                True\n"
        )

    def test_manager_selector(self):

        result = self.runner.invoke(cli, ['-m', 'fs', 'exists', 'README.md'])
        assert result.exit_code == 0


    def test_cat(self):

        result = self.runner.invoke(cli, ['cat', '.gitignore'])

        assert result.exit_code == 0
        with open('.gitignore') as handle:
            self.assertEqual(result.output, handle.read() + '\n')

    def test_cp_artefact(self):

        with tempfile.TemporaryDirectory() as directory:

            file1path = os.path.join(directory, 'file1.txt')
            file2path = os.path.join(directory, 'file2.txt')
            content = 'Do the stuff'

            with open(file1path, 'w') as handle:
                handle.write(content)

            result = self.runner.invoke(cli, ['cp', file1path, file2path])

            assert result.exit_code == 0

            with open(file2path, 'r') as handle:
                self.assertEqual(content, handle.read())

    def test_cp_file_with_merge(self):

        with tempfile.TemporaryDirectory() as directory:

            file1path = os.path.join(directory, 'file1.txt')
            file2path = os.path.join(directory, 'file2.txt')
            content = 'Do the stuff'

            with open(file1path, 'w') as handle:
                handle.write(content)

            result = self.runner.invoke(cli, ['cp', '-m', file1path, file2path])

            assert result.exit_code == 1

    def test_cp_merge_replace(self):

        with tempfile.TemporaryDirectory() as source:
            with tempfile.TemporaryDirectory() as destination:

                os.mkdir(os.path.join(source, 'directory_example'))

                with open(os.path.join(source, '1.txt'), 'w') as handle:
                    handle.write('original data')

                with open(os.path.join(destination, '1.txt'), 'w') as handle:
                    handle.write('to be replaced')

                with open(os.path.join(destination, '2.txt'), 'w') as handle:
                    handle.write('Uneffected file')

                result = self.runner.invoke(cli, ['cp', '-m', source, destination])
                assert result.exit_code == 0

                self.assertSetEqual({'1.txt', '2.txt', 'directory_example'}, set(os.listdir(destination)))

                with open(os.path.join(destination, '1.txt'), 'r') as handle:
                    self.assertEqual(handle.read(), 'original data')

                with open(os.path.join(destination, '2.txt'), 'r') as handle:
                    self.assertEqual(handle.read(), 'Uneffected file')

    def test_cp_merge_rename(self):

        with tempfile.TemporaryDirectory() as source:
            with tempfile.TemporaryDirectory() as destination:

                os.mkdir(os.path.join(source, 'directory_example'))

                with open(os.path.join(source, '1.txt'), 'w') as handle:
                    handle.write('original data')

                with open(os.path.join(destination, '1.txt'), 'w') as handle:
                    handle.write('to be preserved')

                with open(os.path.join(destination, '2.txt'), 'w') as handle:
                    handle.write('Uneffected file')

                result = self.runner.invoke(cli, ['cp', '-m', '-ms', 'rename', source, destination])
                assert result.exit_code == 0

                self.assertSetEqual({'1.txt', '1-COPY.txt', '2.txt', 'directory_example'}, set(os.listdir(destination)))

                with open(os.path.join(destination, '1.txt'), 'r') as handle:
                    self.assertEqual(handle.read(), 'to be preserved')

                with open(os.path.join(destination, '1-COPY.txt'), 'r') as handle:
                    self.assertEqual(handle.read(), 'original data')

                with open(os.path.join(destination, '2.txt'), 'r') as handle:
                    self.assertEqual(handle.read(), 'Uneffected file')

    def test_touch(self):

        with tempfile.TemporaryDirectory() as source:

            result = self.runner.invoke(cli, ['touch', os.path.join(source, 'new_file.txt')])
            assert result.exit_code == 0
            self.assertTrue(os.path.exists(os.path.join(source, 'new_file.txt')))

    def test_mkdir(self):

        with tempfile.TemporaryDirectory() as source:

            result = self.runner.invoke(cli, ['mkdir', os.path.join(source, 'directory')])
            assert result.exit_code == 0
            self.assertTrue(os.path.exists(os.path.join(source, 'directory')))

    def test_mklink(self):

        with tempfile.TemporaryDirectory() as source:

            with open(os.path.join(source, '1.txt'), 'w') as handle:
                handle.write('original data')

            result = self.runner.invoke(cli, ['mklink', os.path.join(source, '1.txt'), os.path.join(source, '2.txt')])
            assert result.exit_code == 0

            os.path.islink(os.path.join(source, '2.txt'))

    def test_get(self):

        with tempfile.TemporaryDirectory() as source:

            with open(os.path.join(source, '1.txt'), 'w') as handle:
                handle.write('original data')

            result = self.runner.invoke(cli, ['get', os.path.join(source, '1.txt'), os.path.join(source, '2.txt')])
            assert result.exit_code == 0

            os.path.isfile(os.path.join(source, '2.txt'))

    def test_ls(self):

        with tempfile.TemporaryDirectory() as source:

            with open(os.path.join(source, '1.txt'), 'w') as handle:
                handle.write('original data')

            result = self.runner.invoke(cli, ['ls', source])
            assert result.exit_code == 0

            self.assertEqual(len(result.output.splitlines()), 5, msg=result.output)

    def test_ls_k8s(self):

        result = self.runner.invoke(cli, ['ls', 'k8s://development-esp'])
        assert result.exit_code == 0

    def test_mv(self):

        with tempfile.TemporaryDirectory() as source:
            with tempfile.TemporaryDirectory() as destination:

                f1 = os.path.join(source, '1.txt')
                f2 = os.path.join(destination, '1.txt')

                with open(f1, 'w') as handle:
                    handle.write('original data')

                result = self.runner.invoke(cli, ['mv', f1, f2])
                assert result.exit_code == 0

                self.assertFalse(os.path.exists(f1))
                self.assertTrue(os.path.exists(f2))

    def test_put(self):

        with tempfile.TemporaryDirectory() as source:

                f1 = os.path.join(source, '1.txt')
                f2 = os.path.join(source, '2.txt')

                with open(f1, 'w') as handle:
                    handle.write('original data')

                result = self.runner.invoke(cli, ['put', f1, f2])
                assert result.exit_code == 0

                self.assertTrue(os.path.exists(f1))
                self.assertTrue(os.path.exists(f2))

    def test_digest(self):

        with tempfile.TemporaryDirectory() as directory:

            filepath = os.path.join(directory, 'file1.txt')

            with open(filepath, 'w') as handle:
                handle.write('This is a file with the same content all the time')

            result = self.runner.invoke(cli, ['digest', filepath])
            assert result.exit_code == 0
            assert result.output == "d63fd5fa049caa04c774ff3bdb8c3632\n"

    def test_sync(self):

        with tempfile.TemporaryDirectory() as source:
            with tempfile.TemporaryDirectory() as destination:

                os.mkdir(os.path.join(source, 'directory_example'))

                with open(os.path.join(source, '1.txt'), 'w') as handle:
                    handle.write('original data')
                time.sleep(0.001)

                with open(os.path.join(destination, '1.txt'), 'w') as handle:
                    handle.write('not updated!')
                time.sleep(0.001)

                with open(os.path.join(destination, '2.txt'), 'w') as handle:
                    handle.write('Uneffected file')
                time.sleep(0.001)

                with open(os.path.join(destination, '3.txt'), 'w') as handle:
                    handle.write('Uneffected file')
                time.sleep(0.001)

                with open(os.path.join(source, '2.txt'), 'w') as handle:
                    handle.write('Updated')
                time.sleep(0.001)

                result = self.runner.invoke(cli, ['sync', source, destination])
                assert result.exit_code == 0, result.output

                self.assertSetEqual({'1.txt', '2.txt', '3.txt', 'directory_example'}, set(os.listdir(destination)))

                with open(os.path.join(destination, '1.txt'), 'r') as handle:
                    self.assertEqual(handle.read(), 'not updated!')

                with open(os.path.join(destination, '2.txt'), 'r') as handle:
                    self.assertEqual(handle.read(), 'Updated')

                with open(os.path.join(destination, '3.txt'), 'r') as handle:
                    self.assertEqual(handle.read(), 'Uneffected file')

    def test_sync_reqired_comparator(self):

        with tempfile.TemporaryDirectory() as source:
            with tempfile.TemporaryDirectory() as destination:

                result = self.runner.invoke(cli, ['sync', '--ignore-modified', source, destination])
                assert result.exit_code == 1

    def test_sync_use_comparator(self):

        with tempfile.TemporaryDirectory() as source:
            with tempfile.TemporaryDirectory() as destination:

                result = self.runner.invoke(cli, ['sync', '--comparator', 'stow.managers.amazon.etagComparator', source, destination])
                assert result.exit_code == 0

    def test_rm(self):

        with tempfile.TemporaryDirectory() as source:

            with open(os.path.join(source, '1.txt'), 'w') as handle:
                handle.write('original data')

            result = self.runner.invoke(cli, ['rm', os.path.join(source, '1.txt')])
            assert result.exit_code == 0

            self.assertSetEqual(set(), set(os.listdir(source)))


