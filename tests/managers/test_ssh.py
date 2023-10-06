import unittest

from stow.managers.ssh import SSH

class Test_SSH(unittest.TestCase):

    def setUp(self):
        pass

    def test_ls(self):

        ssh = SSH("workstation", username="kieran", password="K1mP0$%1bl3")

        self.assertEqual(ssh.artefact(r'\Users\kieran\Projects\personal\stow\mkdocs.yml'), None)
        self.assertEqual(ssh.ls(), [])
