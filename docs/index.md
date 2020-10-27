# Stow

`Stow` is a package that enables the manipulation of local and remote artefacts seemlessly and effortlessly via a powerful and well defined interface. Importantly it provides you with the abstraction you want from storage implementations and solves compatibility issues and concerns.

`stow` enables you to deploy solutions anywhere with any storage backend without concern, and without having to re-implement how you access your files and directories.

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


stow.put("requirements.txt", "s3://example-bucket/projects/stow/requirements.txt")

with stow.open("s3://example-bucket/projects/stow/requirements.txt", "r") as handle:
    print(handle.read())
# tqdm
# pyini
# boto3

reqs = stow.artefact("s3://example-bucket/projects/stow/requirements.txt")
reqs
# <stow.File: s3://example-bucket/projects/stow/requirements.txt modified(2020-10-10 09:32:59.423165) size(16 bytes)>

print(reqs.name, reqs.size, reqs.extension)
# requirements 16 txt
```

## Getting started

### Installation

You can get stow by:
```bash
$ pip install stow
```

You can get a specific verion of stow by:
```sh
$ pip install stow==0.2.0
```

To use `stow`, simply import the package and begin to use its rich interface

```python
import stow

stow.ls()
```

!!! Note
    The latest development version can always be found on [GitHub](https://github.com/Kieran-Bacon/stow){target=_blank}.

    For best results, please ensure your version of Python is up-to-date. For more information on how to get the latest version of Python, please refer to the official [Python documentation](https://www.python.org/downloads/){target=_blank}.

### Paths, Artefacts and Managers

<p role="list-header"> <code>stow</code> only defines two fundamental objects:</p>
- `Artefact` and
- `Manager`

`Artefacts` represent a storage object like a file or directory (`stow.File`, `stow.Directory`) and it provides methods for accessing those objects contents and extracting relevant metadata. All `Artefact` objects belong to a `Manager` which orchestrates communication between your session and the storage medium.

Each `Manager` is a backend for a specific storage medium, and all adhere to a rich `Manager` interface. This allows you to write storage medium agnostic code, and switch between (or use multiple varieties of) backends confidently.

**`Artefacts` can be used more often than not in place of a path string on the stateless and manager interfaces.**

```python
reqFile = manager["/requirements.txt"]

with reqFile.open("r") as handle:
    ...

with manager.open(reqFile, "r") as handle:
    ...

with stow.open(reqFile, "r") as handle:
    ...

with stow.open("/requirements.txt", "r") as handle:
    ...
```

`Artefacts` use the expressive interface of the `Manager` object to perform their actions so inheriting from `Manager` to support a new storage backend is a breeze, and can be looked into [here](/extending).

`Managers` use the unix filesystem syntax for their paths, across all mediums and platform implementations. These paths are translated to the medium specific paths when operations are executed.

<p role="code-header">As such, the following will run for both <b>unix</b> and <b>windows</b> platforms:</p>

```python
stow.ls("~/Documents")
manager["/directory/file1.txt"].size
```

Windows paths are accepted as part of the stow stateless interface, but, to truely be agnostic you should use the unix filesystem syntax.

!!! Warning
    A result of this choice is that for development on Windows the `Manager` and `Directory` objects will not accept windows relative paths. The local filesystem manager will act just like any other manager, as a unix filesystem.

    The stateless interface can be dropped in without an issue into pre-existing code as a replacement for os, but, to use the Manager object you will need to be aware of its intolerance of windows.

### Things to remember

Listed below are a few things to remember about this package that may save you some head scratching.

#### Don't hold onto things you don't need

Words to live by, but especially here. As `Artefact` objects are created to provide a convenient means of accessing filesystem objects, their existence is tied intrinsically with those objects. Holding references to objects that are deleted will result in the `Artefact` objects raising existence errors when you next try to interact with them.

If an object is actively deleted you should try and make sure you aren't holding a reference to it.

That being said, updates to files, overwrites, copies and moves onto operations will not be an issue. The metadata for the the `Artefact` object will be updated accordingly.

## Ways of working

The package is intend to be as simple to use as the builtin `os.path` package but far more powerful. An important aspect of any powerful package is a good abstraction layer that means that the user doesn't have to cater to the package's implementation, but with any road paved with good intentions...

Fortunately there isn't any special, or specific edge cases that will surprise you or break the intuitive nature of the package. Everything will work as you expect and as the interface states, however, the are paradigms for working with remote files that will aid (slightly) in the performance when in such a context that will have no impact/benefit to local file system `Manager`s. As such, it would be recommended to program as that were the context but I discuss below these subtleties more so you can decide for yourself.

Furthermore, Managers may have performance quirks specific to them but these should be slight as compatibility is the primary focus, any such differences will be outlined [here](managers).

Any third party package/plugin `Manager` is a of course untested, however if following the steps [here](extending) they should be highly effective.


### Working with stow

`stow` aims to replace `os.path` so automatically any example usage of `os.path` will be just as valid when replacing `os.path` with `stow`.

```python
import os
import stow

p1, p2 ,p3 = ...

os.path.commonpath([p1, p2, p3]) == stow.commonpath([p1, p2 ,p3]) # True
```

There is an endless list of different scenarios and requirements you may have for working with files. Here we will illustrate a few that may give an insight into how you can incorporate stow to great affect in your own applications.

#### working locally and then pushing

Conventionally working with a remote destination, you would create your artefacts and then push/put them on the target using a bespoke package for the storage manager. It would look similar to below but more complicated on the pushing front.

```python
import os
import stow

LOCAL_DIRECTORY = "/path/..." # Or temporary directory
REMOTE_DIRECTORY = "s3://example-bucket"

# Do some work in a base directory
for i in range(10):
    with open(os.path.join(LOCAL_DIRECTORY, str(i)), "w") as handle:
        handle.write(f"line {i}")

# Put the directory into an s3 bucket
stow.put(LOCAL_DIRECTORY, REMOTE_DIRECTORY)

# Remove local artefacts that are now on remote (or keep them... that's up to you really)
os.rmdir(LOCAL_DIRECTORY)
```

By `localising` a directory you can wrap your code without having to change it to immediately benefit writing to remote destinations

```python
import stow

REMOTE_DIRECTORY = "s3://example-bucket"  # Could be a path to any remote

with stow.localise(REMOTE_DIRECTORY) as LOCAL_DIRECTORY:
    stow.mkdir(LOCAL_DIRECTORY)  # If the target location doesn't yet exist, we will create it as a directory now

    # Do some work in a base directory
    for i in range(10):
        with open(os.path.join(LOCAL_DIRECTORY, str(i)), "w") as handle:
            handle.write(f"line {i}")
```

Alternatively you can use stow to write directly to the manager, via the stow package interface or the manager interface

```python
import stow

REMOTE_DIRECTORY = "s3://example-bucket"  # Could be a path to any remote

# Do some work in a base directory
for i in range(10):
    with stow.open(stow.join(REMOTE_DIRECTORY, str(i)), "w") as handle:
        handle.write(f"line {i}")
```

```python
import stow

s3 = stow.connect(manager="s3", bucket="example-bucket")

# Do some work in a base directory
for i in range(10):
    with s3.open(i, "w") as handle:
        handle.write(f"{i}")
```

!!! Note
    `os` was not imported. For constructing files with depth `stow.join()` or a manager's `.join()` can be used to construct the manager relative path.


!!! Note
    AWS credentials were setup for the user via one of the methods that can be read about <a href="https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html" target="_blank">__*here*__</a>. This allows `stow` to able to communicate with s3 simply be using the qualified url for the artefacts. Otherwise, the IAM secret keys are required to be passed to the manager as keyword arguments which can be looked at in [managers](managers).

#### merging directories

Wanted to merge or synchronise two directory is rather a pain. You will need to setup a loop to fetch files metadata and do the comparison for each file. This method is even more difficult when considering fetching metadata from a remote source.

```python
import os
import shutil

source = "..."
destination = "..."

for file in os.listdir(source):
    sourceStat = os.stat(os.path.join(source, file))
    destinationStat = os.stat(os.path.join(destination, file))

    if (
        (destinationStat is None) or # There isn't a destination file
        (sourceStat.st_mtime > destinationStat.st_mtime)  # Source has been updated
        ):
        # Update the destination
        shutil.copyfile(os.path.join(source, file), os.path.join(destination, file))
```
`stow` makes this easy.
```python
import stow

# paths to directories anywhere (local, aws, ssh, ...)
source = "..."
destination = "..."

# Sync the directories
stow.sync(source, destination)
```

### Writing manager agnostic code

The ultimate power that `stow` provides is the time saving and confidence brought by removing the need to write complicated methods for handling multiple backend storage solutions in your application.

Importantly, time spent making your solution robust in multiple environments (e.g development and production) to then simple abandon the good work when only a subset of those environments is where your live application runs, can be saved.

**You** shouldn't be focusing on storage management, you should be focusing on your solutions and be confident that artefacts needed will be available regardless where you are deploying and whatever storage method you are using.

#### Current situation

Imagine we have the following method that loads in configuration information for our solution (please don't judge on the quality of the method, just for demonstration purposes only).
```python
import os
import json

def loadInConfigs(configDirectory: str) -> dict:
    """ Open and parser the system configurations

    Args:
        configDirectory: The path to the config directory

    Returns:
        dict: A diction of configuration names to values

    Raises:
        FileNotFoundError: if the configDirectory path does not exist
    """

    with open(os.path.join(configDirectory, "config1.json"), "r") as handle:
        config1 = json.load(handle)

    with open(os.path.join(configDirectory, "config2.json"), "r") as handle:
        config2 = json.load(handle)

    with open(os.path.join(configDirectory, "config3.json"), "r") as handle:
        config3 = json.load(handle)

    combined = {"lazers": config1, "cannons": config2, "doors": config3}

    return combined
```

This works perfectly fine locally, but, the target of your work is an docker container running in AWS ecs and the configs need to be configurable from a distance so they will actually be hosted on s3...

Well... I guess we are going to have to write a different method that uses boto3 to connect to the bucket and pull them out. We'll write this code but not test it until we deploy to our testing/staging environment as that looks a lot more like production with similar polices and the like, and for now we will continue with this method!

Great! will still need to write some sort of toggle for this and that method and fit that in to our cli, api, etc, so we do that. Or as the over-engineers will inevitably do, create a function to parse the `configDirectory` path and decide the best method for accessing the data.

Then from word up high, we want to update the running solution on the fly by changing some of these configs and (I can't image a use case where you'd have to handle it like this) to do so you want to check the update time of the file to trigger a change.

You'd need implemented this for both your AWS backend solution and your local development directory.

Then we write tests! (of if you are better than I, you've done this first and you've finished now because the methods work).

**but what have you achieved?** Sad to say, nothing. You've loaded in some files. And made a questionable decision about how your program flows logically. What can we do about this?

#### Use stow methods and paths

First off lets remove the requirement for os and make this as similar to the original method as possible
```python
import stow
import json

def loadInConfigs(configDirectory: str) -> dict:
    """ Open and parser the system configurations

    Args:
        configDirectory: The path to the config directory

    Returns:
        dict: A diction of configuration names to values

    Raises:
        FileNotFoundError: if the configDirectory path does not exist
    """

    with stow.open(stow.join(configDirectory, "config1.json"), "r") as handle:
        config1 = json.load(handle)

    with stow.open(stow.join(configDirectory, "config2.json"), "r") as handle:
        config2 = json.load(handle)

    with stow.open(stow.join(configDirectory, "config3.json"), "r") as handle:
        config3 = json.load(handle)

    combined = {"lazers": config1, "cannons": config2, "doors": config3}

    return combined
```

Almost identical code but the consequence is that you can pass in a stow path such as `/home/kieran/configs`, `s3://example-bucket/configs` or `ssh://kieran@ec2.../home/ubuntu/configs` and have the same behaviour.

You can write a test for this function and be done with it. Nothing complicated but highly functional.

#### Use a stow object

Useful when you have a structure of nested related items, you may make absolute paths which will become relative to the manager. A result is that this structure could be moved and the code would continue to work perfectly, now with the benefit of the manager interface to use inside the function, and a clear indication of what is going on in the interface.

```python
import stow
import json

#def loadInConfigs(configDirectory: stow.Manager) -> dict:
def loadInConfigs(configDirectory: stow.Directory) -> dict:
    """ Open and parser the system configurations

    Args:
        configDirectory: directory holding config files

    Returns:
        dict: A diction of configuration names to values

    Raises:
        FileNotFoundError: if the configDirectory path does not exist
    """

    with configDirectory["/config1.json"].open("r") as handle:
        config1 = json.load(handle)

    with configDirectory["/config2.json"].open("r") as handle:
        config2 = json.load(handle)

    with configDirectory["/config3.json"].open("r") as handle:
        config3 = json.load(handle)

    combined = {"lazers": config1, "cannons": config2, "doors": config3}

    return combined
```
written elsewhere
```python
import os
import stow

manager = stow.manager(os.environ.get("DATA_DIRECTORY", "s3://example-bucket/application_data"))

#configs = loadInConfigs(manager)
configs = loadInConfigs(manager["/configs"])

```

This change has altered the method quite a bit and now requires that interactions be handled via the manager object. However, you've gained a lot in terms of compatibility with an storage backend and functionality provided on the interface of the `Manager` object.

### Considerations for remote file systems

#### Orchestrating managers

If moving artefacts between multiple remote file systems, it is extremely likely that the `Manager`s will require the artefacts be pulled to your local machine and pushed to the target destination. As such your local machine will be orchestrating the communication between the remote file systems and may need to hold in memory, or use storage to hold artefact information as it is being pushed up to the target.

```python
import stow

stow.cp("s3://example-bucket/here","s3://different-bucket/here")

for art in stow.ls("ssh://ubuntu@ec2../files/here"):
    if isinstance(art, stow.File):
        stow.put(art, "s3://example-bucket/instance/")
```

#### Dealing with added latency

Working with remote file systems will incur noticeable amounts of latency (in comparison to local artefacts) which many pose a problem for a system. Unfortunately this kind of lag, when reading and writing to files and directories, can only really be solved by improving your connectivity to the remote, and by cutting down on the number of operations you are performing.

This second point is something we can address in our programs, and its a good habit even when working explicitly with local files. You should try to minimise the number of read write functions you have to make, and program to make updates in bulk whenever possible.

!!! Note
    Read and write operations are not the same as reading metadata/listing directories. These operations are extremely cheap to execute and values are cached whenever possible.

Bulking updates/changes is akin to working on directories rather than files. To open and read from a `File` the contents will need to be fetched. Inversely, if you are writing to a file, when the `File` handle is closed, the new contents needs to be pushed onto the remote.

Instead of working on files individually, we can work within a directory and pull/push the directory all at once.

```python
import stow

# Five push of files to s3
for i in range(5):
    with stow.open("s3://example-bucket/files/{}".format(i), "w") as handle:
        handle.write(i)

# One push of directory of five files
with stow.localise("s3://example-bucket/files", mode="a") as abspath:
    for i in range(5):
        with stow.open(stow.join(abspath, str(i)), "w") as handle:
            handle.write(i)
```

Caveats to this approach:

- This bulking method requires the files you are working on to touch the local file system before they are pushed to the remote, so if local storage is a scarce resource then this approach may not be feasible.
- The performance of the bulk upload is dependent on the availability of the underlying backend. If the storage provider doesn't provide a utility for bulk uploading then there isn't an improvement to be had.
- Network usage will be grouped at a single point (exiting of the localise context) in your program flow.

!!! Note
    The behaviour for local file systems is exactly the same for both approaches as they both will access the artefacts directly. There is no negative impact locally.
