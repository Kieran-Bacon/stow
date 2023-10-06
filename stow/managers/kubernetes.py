from kubernetes import client, config

from ..artefacts import File, Directory
from ..manager.base_managers import RemoteManager

config.load_kube_config()

class Kubernetes(RemoteManager):

    def __init__(self, default_namespace: str = 'default'):
        self.client = client.CoreV1Api()

    def _ls(self, artefact: str, recursive: bool = False):

        parts = artefact.split('/')
        if len(parts) == 1:
            namespace, pod, path = parts[0], '', ''

        elif len(parts) == 2:
            namespace, pod, path = parts[0], parts[1], ''

        else:
            namespace, pod, path = parts[0], parts[1], "/".join(parts[2:])


        if not namespace:
            pods = self.client.list_pod_for_all_namespaces()
            for pod in pods.items:
                art = Directory(self, f"/{pod.metadata.namespace}/{pod.metadata.name}")
                if recursive:
                    yield from self._ls()



        if artefact is None or True:
            pods = self.client.list_pod_for_all_namespaces()

            for pod in pods.items:
                yield Directory(self, f"/{pod.metadata.namespace}/{pod.metadata.name}")

        else:

            self.client.connect_get_namespaced_pod_exec()