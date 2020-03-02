import os
import pyini

from .manager import Manager
from .sync import Sync
from .utils import find, connect

CONFIG_DIRECTORY = os.path.expanduser(os.path.join('~', '.backup_configs'))
if not os.path.exists(CONFIG_DIRECTORY): os.mkdir(CONFIG_DIRECTORY)

class Backup:

    def __init__(self, name: str, local: Manager, remote: Manager, *, policy: str = 'STOP'):
        self._name = name
        self._local = local
        self._remote = remote
        self._policy = policy

    def sync(self):

        # Create a synchronizer object for the two objects
        synchronizer = Sync(self._local, self._remote)

        # Perform sync
        synchronizer.sync()

    @classmethod
    def load(cls, name: str):

        # Define the path to the config
        config_paths = os.path.join(CONFIG_DIRECTORY, name)
        if not os.path.exists(config_paths):
            raise ValueError("Not backup config with that name")

        # Read the config
        config = pyini.ConfigParser().read(config_paths)

        return cls(
            name,
            connect(config['container1'].pop('name'), **config['container1']),
            connect(config['container2'].pop('name'), **config['container2']),
            **config['options']
        )

    def save(self):

        # Save the files
        config_paths = os.path.join(CONFIG_DIRECTORY, self._name)
        pyini.ConfigParser({
            'container1': self._local.toConfig(),
            'container2': self._remote.toConfig(),
            'options': {
                'policy': self._policy
            }
        }).write(config_paths)


class BackupManager:

    @classmethod
    def main(cls):

        import argparse

        parser = argparse.ArgumentParser(prog='Python backup utility (pypi storage)')
        group = parser.add_mutually_exclusive_group()

        group.add_argument('run', nargs='?', help='Run the backup process with the given name')
        group.add_argument('-c', '--create', help='Create a new backup process', nargs='?')
        group.add_argument('-r', '--remove', help='Remove a backup process', nargs='?')
        group.add_argument('-l', '--list', action='store_true', help='List all backup processes')

        arguments = parser.parse_args()

        if arguments.create: cls.createBackup(arguments.create)
        elif arguments.run: cls.executeBackup(arguments.run)
        elif arguments.remove: cls.removeBackup(arguments.remove)
        elif arguments.list: cls.listBackups()
        else: parser.print_help()

    @staticmethod
    def createBackup(name):

        print('Creating a new backup process')

        managers = []
        for dest in ['local', 'remote']:
            while True:
                mType = input('Specify the {} manager type: '.format(dest))

                try:
                    # Find the manager class to initialise
                    mClass = find(mType)

                    # Load that classes CLI
                    managers.append(mClass.CLI())
                    break

                except ValueError:
                    print("Couldn't find a manager with that name. please try again.\n")

        # Select the backup policy
        while True:
            policy = input('Select a backup policy(ACCEPT_1, ACCEPT_2, STOP, CONFLICT): ')

            if policy not in ("ACCEPT_1", "ACCEPT_2", "STOP", "CONFLICT"):
                print("Invalid selection. please try again.")

            break

        # Create the backup process and save its details
        Backup(name, *managers, policy=policy).save()

    @staticmethod
    def executeBackup(name):
        Backup.load(name).sync()

    @staticmethod
    def removeBackup(name):
        os.remove(os.path.join(CONFIG_DIRECTORY, name))

    @staticmethod
    def listBackups():
        print(os.listdir(CONFIG_DIRECTORY))

