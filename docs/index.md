# Welcome to MkDocs

For full documentation visit [mkdocs.org](https://mkdocs.org).

## Commands

* `mkdocs new [dir-name]` - Create a new project.
* `mkdocs serve` - Start the live-reloading docs server.
* `mkdocs build` - Build the documentation site.
* `mkdocs help` - Print this help message.

## Project layout

    mkdocs.yml    # The configuration file.
    docs/
        index.md  # The documentation homepage.
        ...       # Other markdown pages, images and other files.

## Manager Hiearchy

We do not want to have to duplicate any objects more than we we need to

Following this we cannot have the local and remote managers behave in the same way as the remote requires having its contents downloaded to be worked on. This puts a restrain on the local documents to be __downloaded__ from their location locally to another local position.


Manager
    -LocalManager
        -LFS
    -RemoveManager
        -Amazon
        -SSH
        -GoogleDrive

With this setup we can then have artefacts interact with the local and remove managers differently. Namely, the local artefacts would need to interact with the files directly.

Methods on the manager

The manager needs to declare that it's returning artefact objects. These objects can either being a directory or a File.
As such their interfaces are not the same and the use of such an interface doesn't help. What is the shared interface of directories and Files?

Artefact:
    - owner (Manager)
    - path (Manager relative path)
    - download (to a given path)
    - upload (replace the file with the uploaded information)
    - copy (duplicate this information to another location identified)
    -



Managers have a single mapping between an artefact's key (path to file) and the representing file object

* We want to make sure artefacts are are hashable

The user isn't going to be making files an directories as they are products of connecting to a file system. The manager is the one that creates and governs the files and as such the interface to an object shouldn't have to interact with a manager.

This could lead to allowing the user to `create` a new directory/file and then it can immediately default to working as a local file/directory despite it not having a manager, it can be written to act differently assuming that it has been assigned to either which means that we can write the entirity of artefacts before the managers.


Taking this further, we can kill to birds with one stone be not creating a manager for a local file system but rather creating a directory as it would have posess the same code to find it's children.

If it posesses the code to find child elements what difference does it have to a manager? It holds the metadata about the connected party and how to interact with it... its definitely required but it's interface is shared.

Let's write a directory first

d = Directory('/path/to/directory')

d.ls()
d.touch()
with d.open('filename', 'r') as handle:
d.rm()
path = d.download()
d.upload('path/to/directory')











