import unittest

from stow.managers.kubernetes import Kubernetes

class Test_Kubernetes(unittest.TestCase):

    def setUp(self) -> None:
        self.manager = Kubernetes()
