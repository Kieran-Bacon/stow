# Managers

Every manager presents the same interface so that code can be abstracted from the chosen storage backend. This interface is outlined [here](/reference/stow.manager).

<p role="list-header">There are three ways to use a manager:</p>
1. Use the stateless interface using manager prefix and parameters;
1. Initialise the manager directly by importing the manager or using the `stow.find` method;
1. By _connecting_ to the manager using the `stow.connect` method.

`stow.connect` will cache created managers so as to avoid re-initialising a connection. Having multiple managers may lead to having conflicting versions of the manager's files/directories. **As a result it is the recommended method for creating a specific manager object.**

Each `Manager` will need to be configured and they will have their own configuration. This configuration can be passed either in the `__init__` for the `Manager` or via its urlencoded string. A url encoded string holds the protocol for the manager and its parameters, each manager will have a different method for how these are to be passed.

Any `Manager` can create a `SubManager` that wraps a sub-directory in the interface of the `Manager`. This `SubManager` can then behavior like a fully implemented manager, with its own `Files` and `Directories`. `SubManagers` use the concrete manager to fulfill its operations and will update the concrete `Manager` with changes.

Managers | import path | protocol
--- | --- | ---
[Filesystem](#local-filesystem) | stow.managers.FS |
[AWS S3](#amazon-s3) | stow.managers.S3 | _s3_
[Secure Shell ](secure-shell) | stow.managers.SSH | _ssh_

```python
fs = stow.connect(manager="FS", path="~/Documents")
s3 = stow.connect(manager="S3", bucket="bucket-name")  # Assuming aws creds are installed - else pass them

s3 = stow.managers.Amazon(
    "bucket-name"
    aws_access_key_id = "***",
    aws_secret_access_key = "***",
    region_name = "***"
)

stow.ls("/home/user")
stow.ls("s3://example-bucket")
```

## Local filesystem

```python
import stow
from stow.managers import FS

stow.ls("/home/kieran")

fs = stow.connect(manager="fs", path="/home/kieran")
fs.ls()

fs = FS(path="/home/kieran/Downloads")
fs.ls()  # Lists top level of the manager which is the downloads
```

The file system manager is responsible for handling artefacts on the local file system. Using and handling compatibility issues of the `os.path` package. The `Manager` takes a _path_ which is to act as the root, operations and paths will be relative to this location.

!!! Note
    The stateless interface will initialise the `Manager` at the root of the filesystem. This allows all absolute paths to work as expected.

## Amazon S3

```python
import stow
from stow.managers import S3

stow.ls("s3://bucket-name/data")

s3 = stow.connect(manager="s3", bucket="bucket-name")
s3.ls()

s3 = stow.managers.S3(
    bucket="bucket-name"
    aws_access_key_id = "***",
    aws_secret_access_key = "***",
    region_name = "***"
)
s3.ls()
```

Creates a connection to a s3 bucket with the credentials that are provided. If no credentials are provided during initialisation, credentials will be loaded as per [amazon's documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html){target=_blank}

```python
"s3://{bucket}{path}?{{parameters}}"
```

Key | Required | Value
--- | --- | ---
bucket | yes | The bucket name
path | yes | The artefact path in the bucket
aws_access_key_id | no | The IAM user access key to perform the connection with
aws_secret_access_key | no | The IAM user secret access key to perform the connection with
region_name | no | The region for the connection to use.



## Secure shell

```python
from stow.managers import SSH

ssh = SSH(
    host="www.host.com",
    port=22,
    path="/",
    username="kieran",
    password="***",
    key="/path/to/key",
    keep_alive=False
)
ssh.ls()
```

Create a secure connection to a remote machine using the SSH and SFTP protocols. Similar to the `FileSystem`, `SSH` takes a _path_ parameter where operations will be relative too. In the stateless interface, the connection is made at the root of the remote filesystem.

The `SSH` manager can load connection details from the typical ssh configs (`~/.ssh/config`, `/etc/ssh/ssh_config`), but passing the name of the configuration in as the _hostname_. Additional configs can be read from by passing them into via the _sshConfigs_ parameter.

```python
"ssh://{username}:{password}@{hostname}:{port}{path}?{{parameters}}"
"ssh://admin:password@10.80.142.123/path/to/artefact?timeout=5"
"ssh://admin:password@Connection/path/to/artefact?timeout=5"

```

Key | Required | Value
--- | --- | ---
username | no | The bucket name
password | no | The artefact path in the bucket
hostname | yes | The IAM user access key to perform the connection with
path | yes | The IAM user secret access key to perform the connection with
privateKey | no | The region for the connection to use.
privateKeyFilePath | no | Filepath to private key (key.pem) to use for authentication
autoAddMissingHost | no | Choose whether to allow connections to unknown hosts
timeout | no | Time in seconds to before connection timeouts
sshConfig | no |  An additional ssh configuration file to query when connecting