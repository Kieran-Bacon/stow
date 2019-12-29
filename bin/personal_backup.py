import os
import storage
import better

if __name__ == '__main__':

    # Initialise the managers
    local = storage.connect('local', manager='Locals', directories=['~/Documents', '~/Downloads'])

    remote_config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'etc', 'aws_credentials.ini')
    remote = storage.connect('remote', manager='AWS', **better.ConfigParser().read(remote_config_path))

    # Pass them to backup to initialise
    backup = storage.Backup('personal', local, remote)

    # Sync
    backup.sync()