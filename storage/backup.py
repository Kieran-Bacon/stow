import os
import better

from .sync import Sync
from .utils import connect

CONFIG_DIRECTORY = os.path.expanduser(os.path.join('~', '.backup_configs'))

def backup(name: str = None):

    # Ensure that the backup directory is defined
    if not os.path.exists(CONFIG_DIRECTORY): os.mkdir(CONFIG_DIRECTORY)

    # Load for the selected backup process and sync
    filenames = [name] if name is not None else os.listdir(CONFIG_DIRECTORY)

    for filename in filenames:

        # Open a backup config
        config = better.ConfigParser().read(os.path.join(CONFIG_DIRECTORY, filename))

        # Setup a Synchroniser
        synchroniser = Sync(
            connect(config['container1'].pop('name'), **config['container1']),
            connect(config['container2'].pop('name'), **config['container2']),
            **config['tracked'],
            **config.get('options', {})
        )

        # Begin the Synchroniser method
        synchroniser.sync()

        # Overwrite the config stored originall with the new tracked information
        better.ConfigParser(synchroniser.toConfig()).write(os.path.join(CONFIG_DIRECTORY, filename))



