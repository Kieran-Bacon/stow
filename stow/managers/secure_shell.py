import paramiko.client
import paramiko.sftp_client
import paramiko.ssh_exception

import os
import io
import pytz
import stat
import typing
import datetime
import urllib.parse
from functools import wraps

from ..artefacts import Artefact, File, Directory
from ..manager import Manager, RemoteManager
from .. import exceptions

def _ensureConnection(function: typing.Callable) -> typing.Callable:
    """ Instance method decorator to ensure that a sftp connection exists for manager calls. In the event that it
    has timed out, a new connection will be attempted.

    Args:
        function: The function to wrap

    Returns:
        Callable: A new function ensuring sftp connection
    """

    @wraps(function)
    def connect(self, *args, **kwargs):

        try:
            # Attempt to perform action
            return function(self, *args, **kwargs)

        except paramiko.ssh_exception.NoValidConnectionsError:
            # Connection has dropped - reconnect and attempt function again.
            # Do not handle subsequent connection issues

            self._connect()
            return function(self, *args, **kwargs)

    return connect

class SSH(RemoteManager):
    """ Manage a filesystem hosted on a remote machine using the SSH protocol

    Args:
        hostname: The hostname of the computer to manage
        port: The port where ssh connections are being handled
        username: The username of the user on the remote machine that the manager will use
        password: The password for the user
        privateKey: The private key used for ssh authentication
        privateKeyFilePath: A path to the private key for ssh authentication
        sshConfigs: Additional ssh config files for configurations lookup. Extending open ssh config locations.
            Order of Read will be: passed configs, user config, global config.
    """

    # Construct the base configs paths - only store them if they exists in the environment
    BASE_CONFIGS = [
        x
        for x in [
            os.path.expanduser(os.path.join('~', '.ssh', 'config')),
            os.path.expandvars(os.path.join("$ProgramData", 'ssh', 'ssh_config')) if os.name == 'nt' else "/etc/ssh/ssh_config"
        ]
        if os.path.exists(x)
    ]

    def __init__(
        self,
        hostname: str,
        root: str = None,
        port: int = 22,
        username: str = None,
        password: str = None,
        privateKey: str = None,
        privateKeyFilePath: str = None,
        autoAddMissingHost: bool = True,
        timeout: float = 30,
        sshConfigs: typing.Iterable[str] = None
        ):
        super().__init__()

        # Define the ssh and sftp client objects
        self._sshClient = paramiko.client.SSHClient()
        self._ftpClient: paramiko.sftp_client.SFTPClient = None

        # Configure the ssh connection parameters - Determine whether the client can accept an unknown host
        self._autoAddMissingHost = autoAddMissingHost
        if autoAddMissingHost:
            self._sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Load in the configuration files and check for the hostname
        self._sshConfigs = (sshConfigs or [])
        for path in self._sshConfigs + self.BASE_CONFIGS:

            sshConfig = paramiko.SSHConfig()
            sshConfig.parse(open(path))

            for configHostname in sshConfig.get_hostnames():
                if hostname == configHostname or hostname == configHostname.lower():
                    # A configuration has been found

                    # Fetch the config values
                    config = sshConfig.lookup(configHostname)

                    # Fetch the configuration values if they haven't been overwritten by the interface
                    hostname = config['hostname']
                    port = (port or config.get('port'))
                    username = (username or config.get('user'))
                    password = (password or config.get('password'))
                    privateKeyFilePath = (privateKeyFilePath or config.get('identityfile', [None])[0])
                    timeout = (timeout or config.get('timeout'))

        # Save connection information
        self._hostname = hostname
        self._port = port
        self._username = username
        self._password = password
        self._privateKey = privateKey
        self._privateKeyFilePath = privateKeyFilePath
        self._autoAddMissingHost = autoAddMissingHost
        self._timeout = timeout

        # Save remote parameters
        self._root = root  # Root address for the user
        self._home = f'/home/{username}'

        # Assuming that manager is initialised jit for operation - open connection
        self._connect()

    def _connect(self):
        # Connect to the remove machine
        self._sshClient.connect(
            hostname=self._hostname,
            port=self._port,
            username=self._username,
            password=self._password,
            pkey=self._privateKey,
            key_filename=self._privateKeyFilePath,
            timeout=self._timeout
        )

        self._ftpClient = self._sshClient.open_sftp()

    def __repr__(self):
        pass

    def _abspath(self, managerPath: str) -> str:

        if self._root:
            # A root path has been set - everything must be done relative to this path

            if managerPath == '/':
                # Root path has been selected
                return self._root

            # Join the path onto root
            return self.join(self._root, managerPath, joinAbsolutes=True, separator='/')

        else:
            # no root path given - interact with path like normal FS (assume cwd is home)
            if os.path.isabs(managerPath):
                return managerPath

            else:
                return self.join(self._home, managerPath, separator='/')

    def _statsToArtefact(self, artefactStat, artefactPath):

        # Determine whether the artefact is a file or directory
        isDirectory = stat.S_ISDIR(artefactStat.st_mode)

        # # Created time
        # createdTime = datetime.datetime.utcfromtimestamp(stats.st_ctime)
        # createdTime = pytz.UTC.localize(createdTime)

        # Modified time
        modifiedTime = datetime.datetime.utcfromtimestamp(artefactStat.st_mtime)
        modifiedTime = pytz.UTC.localize(modifiedTime)

        # Access time
        accessedTime = datetime.datetime.utcfromtimestamp(artefactStat.st_atime)
        accessedTime = pytz.UTC.localize(accessedTime)

        if isDirectory:
            return Directory(
                self,
                artefactPath,
                modifiedTime=modifiedTime,
                accessedTime=accessedTime
            )

        else:
            return File(
                self,
                artefactPath,
                size=artefactStat.st_size,
                modifiedTime=modifiedTime,
                accessedTime=accessedTime
            )

    @_ensureConnection
    def _identifyPath(self, managerPath: str) -> typing.Union[str, Artefact]:
        """ Given a manager relative path, identify what object (if any) is at that location and store its information
        as a result. Should call _addArtefact in the call stack for the returned item to ensure that things are updated

        Args:
            relpath: Manager relative path

        Returns:
            typing.Union[str, Artefact]: return None, "file", "directory" for nothing exists, file exists, directory
                respectively
        """

        try:
            stats = self._ftpClient.lstat(self._abspath(managerPath))
        except FileNotFoundError:
            return None

        return self._statsToArtefact(stats, artefactPath=managerPath)

    @_ensureConnection
    def _ensureDestination(self, path: str) -> None:
        """ Ensure that the path is a directory - if no artefact is found at location create it, and make any parent
        directories that are needed for the path to be valid

        Args:
            path: The remote fs path to a location where a directory is to exist
        """

        subpath = path
        parts = []
        while True:
            try:
                # Try and get the path stat - File not found error if it doesn't exist
                artefact = self._statsToArtefact(self._ftpClient.stat(subpath), subpath)

                # Artefact will need to be a directory
                if not isinstance(artefact, Directory):
                    raise exceptions.ArtefactTypeError(f'Cannot ensure directory path {path} as subpath {subpath} is a file')

                # The path exists - make dir for the parts not yet created
                for part in parts:
                    subpath = self.join(subpath, part, separator='/')
                    self._ftpClient.mkdir(subpath)

                # All directories created
                break
            except FileNotFoundError:
                subpath, dirname = os.path.split(subpath)
                parts.insert(0, dirname)

    def _recursiveGetDirectory(self, path, destination):

        # Create the directory at the desintation
        os.mkdir(destination)

        # Get the files at this level
        artefacts = self._ftpClient.listdir_attr(path)

        for artefact in artefacts:
            # Construct the destination path
            sourceFilepath = self.join(path, artefact.filename)
            artefactDestinationPath = os.path.join(destination, artefact.filename)

            if stat.S_ISDIR(artefact.st_mode):
                # The artefact is a directory - recursively create it and its children
                self._recursiveGetDirectory(sourceFilepath, artefactDestinationPath)

            else:
                # The item is a file - pull and place the file in the newly created directory
                self._ftpClient.get(sourceFilepath, artefactDestinationPath)


    @_ensureConnection
    def _get(self, source: Artefact, destination: str):

        if isinstance(source, Directory):
            self._recursiveGetDirectory(self._abspath(source.path), destination)

        else:
            # Fetch the file item
            self._ftpClient.get(self._abspath(source.path), destination)

    @_ensureConnection
    def _getBytes(self, source: Artefact) -> bytes:
        with self._ftpClient.open(self._abspath(source.path), 'rb') as handle:
            return handle.read()

    @_ensureConnection
    def _put(self, source: str, destination: str):

        destinationAbs = self._abspath(destination)

        if os.path.isdir(source):

            # Create the target location
            self._ensureDestination(destinationAbs)

            sourcePathLength = len(source) + 1
            for root, dirs, files in os.walk(source):

                # Create the path of the destination
                dRoot = self.join(destinationAbs, root[sourcePathLength:], separator='/')

                # Make the directory
                for dirname in dirs:
                    self._ftpClient.mkdir(self.join(dRoot, dirname, separator='/'))

                # For each file at this point - construct their local absolute path and their relative remote path
                for file in files:
                    self._ftpClient.put(
                        os.path.join(root, file),
                        self.join(dRoot, file, separator='/')
                    )

            # Return the empty directory object
            return self._statsToArtefact(self._ftpClient.stat(destinationAbs), destination)

        else:
            # Ensure the directory the file is meant to exist in and then put the file into the location
            self._ensureDestination(os.path.dirname(destinationAbs))
            return self._statsToArtefact(self._ftpClient.put(source,destinationAbs), destination)

    @_ensureConnection
    def _putBytes(self, fileBytes: bytes, destination: str):
        absDestination = self._abspath(destination)
        self._ensureDestination(os.path.dirname(absDestination))

        stats = self._ftpClient.putfo(io.BytesIO(fileBytes), absDestination, confirm=True)

        return self._statsToArtefact(stats, destination)

    @_ensureConnection
    def _cp(self, source: Artefact, destination: str):
        sourcePath = self._abspath(source.path)
        destination = self._abspath(destination)

        if isinstance(source, Directory):
            self._sshClient.exec_command(f'cp -R {sourcePath} {destination}')

        else:
            self._sshClient.exec_command(f'cp {sourcePath} {destination}')

    @_ensureConnection
    def _mv(self, source: Artefact, destination: str):
        sourcePath = self._abspath(source.path)
        destination = self._abspath(destination)

        self._sshClient.exec_command(f'mv {sourcePath} {destination}')

    @_ensureConnection
    def _rm(self, artefact: Artefact):
        artefactPath = self._abspath(artefact.path)
        if isinstance(artefact, Directory):
            self._sshClient.exec_command(f'rm -rf {artefactPath}')

        else:
            self._sshClient.exec_command(f'rm -f {artefactPath}')

    @_ensureConnection
    def _ls(self, managerPath: str) -> Directory:

        absManagerPath = self._abspath(managerPath)

        for stats in self._ftpClient.listdir_attr(absManagerPath):

            artefactPath = self.join(managerPath, stats.filename, separator='/')

            self._addArtefact(self._statsToArtefact(stats, artefactPath))

    @classmethod
    def _signatureFromURL(cls, url: urllib.parse.ParseResult):

        # Extract the query data passed into
        queryData = urllib.parse.parse_qs(url.query)

        signature = {
            "hostname": url.hostname,
            "port": (url.port or 22),
            "username": url.username,
            "password": url.password,
            "privateKey": queryData.get("privateKey", [None])[0],
            "privateKeyFilePath": queryData.get("privateKeyFilePath", [None])[0],
            "autoAddMissingHost": queryData.get("autoAddMissingHost", [True])[0],
            "timeout": queryData.get("timeout", 30),
            "sshConfigs": queryData.get("sshConfig"),
        }

        return signature, (url.path or '/')

    def toConfig(self):

        return {
            "manager": "ssh",
            "hostname": self._hostname,
            "port": self._port,
            "username": self._username,
            "password": self._password,
            "privateKey": self._privateKey,
            "privateKeyFilePath": self._privateKeyFilePath,
            "autoAddMissingHost": self._autoAddMissingHost,
            "timeout": self._timeout,
            "sshConfigs": self._sshConfigs
        }