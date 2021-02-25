# Managers

Every manager presents the same interface so that code can be abstracted from the chosen storage backend. This interface is outlined [here](/reference/stow.manager).

<p role="list-header">There are three ways to use a manager:</p>

1. Use the stateless interface with the manager prefix;
1. Initialise the manager directly by importing the manager or using the `stow.find` method;
1. Or by _connecting_ to the manager using the `stow.connect` method.

`stow.connect` will cache created managers so as to avoid re-initialising a connection, or having conflicting versions of the manager's files/directories. As a result it is the recommended method for creating a specific manager object.

Managers | import path | prefix
--- | --- | ---
[Filesystem](#local-filesystem) | stow.managers.FS |
[AWS S3](#amazon-s3) | stow.managers.S3 | _s3://{bucket-name}_
[Secure Shell ](secure-shell) | stow.managers.SSH | _ssh://{username}:{password}@{address}{port}_

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

The file system manager is responsible for handling artefacts on the local file system. Using and handling compatibility issues of the `os.path` package.

The manager is setup on a directory and only acts on artefacts found nested within it.

!!! Note
    When using the stateless interface (when not using an filesystem manager specifically) a manager will be created at the root of your filesystem.


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

Creates a connection to a s3 bucket with the credentials that are provided. If no credentials are provided during initialisation, credentials will be sort as per [amazon's documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html){target=_blank}

## Secure shell

```python
from stow.managers import SSH

ssh = SSH(
    host="www.host.com",
    port=22,
    path="/",
    username="kieran",
    password="foiled-again-hacker",
    key="/path/to/key",
    keep_alive=False
)
ssh.ls()
```