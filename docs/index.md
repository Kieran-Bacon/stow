# Stow

`stow` is a package that enables you to write filesystem agnostic code. With `stow` you can access and manipulate local and remote artefacts seamlessly with a rich and familiar interface. `stow` gives abstraction from storage implementations and solves compatibility issues, allowing code to be highly flexible.

`stow` is meant to be a drop in replacement for the `os.path` module, providing full coverage of its interface. Furthermore, `stow` extends the interface to work with remote files and directories and to include methods that follow conventional artefact manipulation paradigms like `put`, `get`, `ls`, `rm`, in a concise and highly functional manner.

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

print(stow.getmtime("s3://example-bucket/projects/stow/requirements.txt"))
# 1617381185.341602
```

## Why use stow?

The ultimate power that `stow` provides is the time saving and confidence brought by removing the need to write complicated methods for handling multiple backend storage solutions in your application.

Importantly, time spent implementing these handlers while trying to cater to various environment nuances, can be saved. Especially when you consider effort spent supporting the various stages of an applications development cycle, to then simple abandon good work when only a particular implementation is used live. (Yes, preferably all those stages are identical, but, this is never the case).

**You shouldn't be focusing on storage management, you should be focusing on your solutions**

Consider the following scenario: As part of a development team, you have been asked to write the code that handles the loading of application configuration, and you've been sent a few json files. You create and test a method that reads in the configuration files, and passes them on to the next step in your application.

This works perfectly fine locally, but, it turns out that the application is going to be deployed as a docker container running in AWS ecs. The configuration files will need to be hosted on s3 and accessed by the container on startup.

Well, you have to write a different method that uses `boto3` to connect to the bucket and pull them out. You setup a test bucket and an application IAM user with optimistic permissions to test your new method with, and get cracking.

You'll then have to add in some logic before this section in the application to handle the possibility of reading the files locally or remotely. This may come in the form of changes to your cli, api, etc, so you do that.

Then from word up high, some of the configuration you are doing will need to change dynamically while the application is running. Your team has decided that the app will monitor one of the configuration files for changes and reload it when it does.

To maintain the local and remote duality of your application, you get to work updating both methods to check for updates, and then test.

**so what have you achieved?** Sad to say, very little. You've spent a lot of time getting up to speed with `boto3` (or re-implementing work from another project), and then you dived back into the deep end trying to understand how to get the modified time of files out. You've supported two methods for the same thing, when only one is going to be used. **You've loaded in some files.**

<p role="code-header">An example solution using <code>stow</code></p>

```python
import stow
import json
import datetime
import typing

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

def reloadConfigIfUpdated(configPath: str, time: datetime = None) -> typing.Union[dict, None]:
    """ Fetch and return config if it has been updated """

    if time is None or stow.artefact(configPath).modifiedTime > time:
        with stow.open(configPath) as handle:
            return json.load(handle)

# Demonstrate how the function is called with different managers
configs = loadInConfigs('/local/app/configs')  # local
configs = loadInConfigs('s3://organisation/project/team/live/app/configs')  # S3
configs = loadInConfigs('ssh://admin:password@.../configs')  # SSH
```

And with that you can handle configurations files being stored locally, on s3, on another container. Simple yet powerful.

## Installation

You can get stow by:
```bash
$ pip install stow
$ pip install stow==1.0.0
```

To use `stow`, simply import the package and begin to use its rich interface

```python
import stow

stow.ls()
```

!!! Note
    The latest development version can always be found on [GitHub](https://github.com/Kieran-Bacon/stow){target=_blank}.

    For best results, please ensure your version of Python is up-to-date. For more information on how to get the latest version of Python, please refer to the official [Python documentation](https://www.python.org/downloads/){target=_blank}.

## Paths, Artefacts, and Managers

<p role="list-header"> Conceptually, <code>stow</code> defines two fundamental objects:</p>
- `Artefact` - A storage object such as a file or directory; and
- `Manager` - An orchestration object for a storage implementation such as s3

**_Paths_** do not have their own object, paths are represented as strings. `Artefacts` wraps files and directories and provides an interface to interact with storage items directly. `Managers` privately define how certain actions will be carried out on a given storage implementation, which is then accced through a generic public interface. This provides the necessary level of abstraction so that your application code can be data agnostic.

### Paths

`stow` doesn't implement a __*path*__ object and instead uses strings just as any `os.path` method would. However, `Artefact` objects are [**path-like**](https://docs.python.org/3/glossary.html#term-path-like-object){target=_blank} which means they will be compatible with `os` methods just as a path object from `pathlib` would be.

```python
>>> import os
>>> import stow
>>> stow.join('/workspace', 'stow')
'/workspace/stow'
>>> os.path.join(stow.artefact('/workspace'), 'stow')
'/workspace/stow'

# On windows
>>> os.path.join(stow.artefact(), 'bin')
'c:\\Users\\kieran\\Projects\\Personal\\stow\\bin'
```

!!! Warning
    Remote artefacts will not be _available_ for use by `os` methods (hence this package) so you are encouraged to use `stow` methods. All `os.path` methods are available on the top level of `stow`

Importantly, `stow` handles paths to remote files just as smoothly as any local file, power you cannot get anywhere else.

```python
>>> stow.join('s3://example-bucket/data', 'data.csv')
's3://example-bucket/data/data.csv'
>>> stow.getmtime('s3://example-bucket/data/data.csv')
1617381185.341602
```

### Artefacts

An `Artefact` represents a storage object which is then subclassed into `stow.File` and `stow.Directory`. These objects provides convenient methods for accessing their contents and extracting relevant metadata. `Artefact` objects are created just in time to serve a request and act as pointers to the local/remote objects. File contents is not downloaded until a explicit method to do so is called.

```python
for artefact in stow.ls('~'):
    if isinstance(artefact, stow.Directory):
        for file in artefact.ls():
            print(file)

    else:
        print(artefact)

home = stow.artefact('~')
print(home['file.txt'].content)  # Explicit call to get the file's contents
```

All `Artefact` objects belong to a `Manager` which orchestrates communication between your session and the storage medium. `Artefacts` are not storage implementation aware, and draw on the public interface of the manager object they belong to to provide their functionality. This point will become importantly when considering extending stow to an additional storage implementation.

!!! Important
    `Artefact` objects are not guaranteed to exist! Read below

As you can hold onto references to `Artefacts` after they have been deleted (either via the stow interface or another method), you can end up attempting to access information for items that no longer exist. Any interaction with an `Artefact` will inform you if that is the case.

```python
>>> file = stow.artefact('~/file.txt')
>>> file.delete()
>>> file.content
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "C:\Users\kieran\Projects\Personal\stow\stow\artefacts.py", line 35, in __getattribute__
    raise exceptions.ArtefactNoLongerExists(
stow.exceptions.ArtefactNoLongerExists: Artefact <class 'stow.artefacts.File'> /Users/kieran/file.txt no longer exists
```
### Managers

`Manager` objects represent a specific storage medium, and they will orchestrate communication between your active interpreter and the storage provider. They all adhere to a rich `Manager` interface which includes definitions for all of the `os.path` methods.

`Manager` objects are created behind the scene for many of `stows` stateless methods to process those calls. To avoid multiple definitions for the same storage providers, `Manager` objects are cached. `Managers` initialised directly will not be cached. It is encouraged to make use of the `Manager` cache by initialising `Managers` using  the following methods `stow.find`, `stow.connect`, and `stow.parseURL`.

`Managers` do not expect to process protocols and path params when they are being used directly. `Managers` will internally use the unix style path standard for displaying and creating `artefacts` paths. This means that a valid path is valid for all `Managers`.

```python
>>> manager = stow.connect(manager='s3', bucket='example-bucket')
>>> manager['/directory/file1.txt']
<stow.File: /directory/file1.txt modified(2021-04-07 18:14:11.473302+00:00) size(0 bytes)>
>>> stow.artefact('s3://example-bucket/directory/file1.txt')
<stow.File: /directory/file1.txt modified(2021-04-07 18:14:11.473302+00:00) size(0 bytes)>
```

!!! Note
    You can completely forget about `Managers`! The stateless interface is sufficiently expressive to do everything you would need, without having to create a `Manager` object. From a users perspective, they have a very limited beneficial use case, one such use case is shown below.

Since `Managers` hold information about their storage provider and want to use valid paths, you can define methods to use the `Manager` objects with the simplified path and have that work across multiple backends.

```python
def managerAgnosticMethod(manager: stow.Manager):

    # do stuff

    with manager.open('/specific/file/path') as handle:
        # do more stuff..


s3 = stow.connect(manager='s3', bucket='example-bucket')
ssh = stow.connect(manager='ssh', host='ec2....', username='ubuntu', ...)

managerAgnosticMethod(s3)
managerAgnosticMethod(ssh)
```

In the example, we have specified a path inside our function and given no consideration to what backend we may be using. The `Manager` passed will interpret the path relative to itself. This would be as opposed to simply constructing that path with the stateless interface.

```python
def managerAgnosticMethod(base: str):

    # do stuff

    with stow.open(stow.join(base, 'specific/file/path')) as handle:
        # do more stuff..

managerAgnosticMethod("s3://example-bucket")
managerAgnosticMethod("ssh://ubuntu:***@ec2...../home/ubuntu")
```

As the `Managers` interface is just as extensive and feature full as the stateless interface, either method would be would be appropriate. The `Manager` method as described will likely lead to fewer lines being written in the general case, but, it comes with the cost of having to understand what a `Manager` object is.

## Ways of working

### Ensuring artefacts

Conventionally working with a remote destination, you would create your artefacts and then push/put them on the target using a bespoke package for the storage manager. It would look similar to below but more complicated on the pushing front.

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

!!! Note
    AWS credentials were setup for the user via one of the methods that can be read about <a href="https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html" target="_blank">__*here*__</a>. This allows `stow` to able to communicate with s3 simply be using the qualified url for the artefacts. Otherwise, the IAM secret keys are required to be passed to the manager as keyword arguments which can be looked at in [managers](managers).

### Communication between two remote managers

If moving artefacts between multiple remote file systems, it is extremely likely that the `Manager`s will require the artefacts be pulled to your local machine and pushed to the target destination. As such your local machine will be orchestrating the communication between the remote file systems and may need to hold in memory, or use storage to hold artefact information as it is being pushed up to the target.

```python
import stow

stow.cp("s3://example-bucket/here","s3://different-bucket/here")

for art in stow.ls("ssh://ubuntu@ec2../files/here"):
    if isinstance(art, stow.File):
        stow.put(art, "s3://example-bucket/instance/")
```

### Dealing with added latency

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

### Don't hold onto things you don't need

Words to live by, but especially here. As `Artefact` objects are created to provide a convenient means of accessing filesystem objects, their existence is tied intrinsically with those objects. Holding references to objects that are deleted will result in the `Artefact` objects raising existence errors when you next try to interact with them.

If an object is actively deleted you should try and make sure you aren't holding a reference to it.

That being said, updates to files, overwrites, copies and moves onto operations will not be an issue. The metadata for the the `Artefact` object will be updated accordingly.