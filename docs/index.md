# Stow

Stow is a package that allows for the seemless communication between vairous different data storage systems via a hemogenous "manager" interface. This allows code to be written for a local filestore and then immediately have a AWS bucket plugged in without having to update code.

```python
import stow

for art in stow.ls():
    print(art)
# <stow.Directory: /home/kieran/stow/.pytest_cache>
# <stow.Directory: /home/kieran/stow/tests>
# <stow.File: /home/kieran/stow/mkdocs.yml modified(2020-06-27 10:24:10.909885) size(68 bytes)>
# <stow.File: /home/kieran/stow/requirements.txt modified(2020-05-25 14:00:59.423165) size(16 bytes)>
# ...

with stow.open("requirements.txt", "r") as handle:
    print(handle.read())
# tqdm
# pyini
# boto3

s3 = stow.connect(manager="S3", bucket="example-bucket")

s3.put("requirements.txt", "projects/stow/requirements.txt")
```

## Getting started
### Managers

Managers wrap a storage method. Currently two managers.

Managers | import path | connect manager names | prefix
--- | --- | --- | ---
Filesystem | stow.managers.FS | FS, LFS |
Amazon AWS | stow.managers.Amazon | S3, AWS | _s3://_


> Note - lowercase versions of the connect manager names are also permitted

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

### Working in a directory

There are multiple methods for how you might want to work in a directory and have the changes you make be present in a manager.

#### work then push

One method is to do the work you intend to do in a directory and push the directory after the fact to the manager. This is conventionally how most people currently work with local and remote storage.

```python
import os
import stow

# Do some work in a base directory
DIRECTORY = "/path/..."
for i in range(10):
    open(os.path.join(DIRECTORY, str(i)), "w").write(f"line {i}")

# Put the directory into an s3 bucket
stow.put(DIRECTORY, "s3://bucket-name/path/...)
```

Alternatively you can use stow managers solely.

```python
import stow

for i in range(10):
    with stow.open(stow.join("/path/...", i), "w") as handle:
        handle.write(f"{i}")

stow.put("/path/...", "s3://example-bucket/path/...")
```

```python
import stow

local = stow.connect(manager="FS", path="/path/...")
s3 = stow.connect(manager="s3", bucket="example-bucket")

for i in range(10):
    with local.open(i, "w") as handle:
        handle.write(f"{i}")

s3.put(local, "destination/path")
```
---
**NOTE** - `os` was not imported

For constructing files with depth `stow.join()` or a manager's `.join()` can be used to construct the manager relative path.

---

---
**NOTE** - AWS credentials are setup for the user

In the example above - stow is able to communicate with s3 because user credentials have already been configured on the machine. Otherwise, the keys are required to be passed to the manager.

---

This works well but it means that you've now got files locally and remotely, and there wasn't any interaction with files that might have already been on the target.

#### work in localised remote directory

If we don't want to be storing these files locally, and want to be able to interact with what is already there, we can `localise` the directory and interact with it's files. As a bonus, there isn't a requirement to push an update for the files.

```python
import os
import stow

with stow.localise("s3://example-bucket/folder1") as abspath:
    if not os.path.exists(abspath): os.mkdir(abspath)

    for i in range(10):
        with open(os.path.join(abspath, i), "w") as handle:
            handle.write(f"{i}")

    os.mkdir(os.path.join(abspath, "subdir"))

    with open(os.path.join(abspath, "subdir", "filename"), "w") as handle:
        handle.write("writing a file in a subdirectory")

    with open(os.path.join(abspath, "existing file"), "r") as handle:
        print(handle.read())

```
---
**NOTE** - `localise` doesn't create an artifact if it doesn't exist on the remote

`localise`-ing a non existent artefact from the remote will not create a file or directory at the `abspath` location. If the user intends it to be a file they can open the path as they would normally, but, if they intend to use the location as a directory they will need make it so. stow will upload what ever is created to the remote location specified.

---
#### work on remote and walk away

An improvement on this might be that we work as if we are directly on the remote machine, without having to use the paths of the os.

**Directories when you need them and do not need specific invocation. Although this is possible via `stow.mkdir()`



```python
import stow

prefix = "s3://example-bucket"
#! Could just be using a manager

for i in range(10):
    with stow.open(stow.join(prefix, i), "w") as handle:
        handle.write(f"{i}")

with stow.open(stow.join(prefix, "subdir", "filename"), "w") as handle:
    handle.write("writing a file in a subdirectory")

print(stow.artefact(stow.join(prefix, "existing file")).content)

# OR
# with stow.open(stow.join(prefix, "existing file"), "r") as handle:
#     print(handle.read())

# OR
# print(s3["existing file"].content)     If you made a manager instead
```



### Localising

Localising gives a path to a location -

