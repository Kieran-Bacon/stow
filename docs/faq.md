# Frequently asked questions

## Can files and managers be seralised

Yes.

## How to put into things into a directory

The put method takes the source artefact and puts it directly at the location given by destination. If you'd like to move things by bulk into a destination and keep the same name you can write the following

```python
with artefact in stow.ls("local_directory"):
    stow.put(artefact, stow.join(target.path, artefact.basename))
```

## put vs sync for directories
### How do stamp on a directory with another?

You want to use the put command and toggle overwrite. This will remove everything at the target location and upload the source directory to that location

```python
stow.put(directory1, directory2, overwrite=True)
```

### How should I merge two directories together?

This is a complicated question as it depends on what you mean by merge exactly.

Lets imagine that you want all the files in a directory at every level to be pushed into a another directory. This would preserve files in the destination that don't conflict and overwrite any that do (even if the destination file is more up to date than the source file).

We can do this by iterating over the artefacts and putting the `File` artefacts into the destination. `Directory` objects will be created for files if they don't already exist in the destination.

```python
# Loop over the contents in the source
for artefact in stow.ls(source, recursive=True):

    # If the artefact is a file - push it to the target
    if isinstance(artefact, stow.File):

        # Put the artefact into the destination relative to the artefacts relative path to the source directory
        stow.put(artefact, stow.join(destination, source.relpath(artefact)))
```

!!! Important

    The `overwrite` argument wasn't toggled, so we assume that we won't every have a situation where we want to replace a directory in the destination with a file

Lets now imagine that you want only the first level to be merged (anything nested can be overwritten/deleted). Then we can simple loop over the top level of the source directory and push both files and directories.

```python
# Loop over the artefacts in the top level of the source
for artefact in stow.ls(source):

    # Put the artefact in the location with their basename
    stow.put(artefact, stow.join(destination, artefact.basename), overwrite=True)
```

With these tools you can customise your merge behaviour to no end, for any level. However, I'd suggest checking out `sync` before you write you own method for merging as it may already be exactly want you are looking to do.

### How do I merge and update files only if they are newer?

Simply put, you want to `sync` the two directories together.

`sync` will push all file objects (and create their directories if they don't exist) into a destination at every level only if the source file is _newer_ (modified more recently) than the destination file. Any source file that doesn't conflict is also put into the destination.

```python
stow.sync(source, destination)
```

!!! Warning
    `sync` assumes that the directories will have a similar structures, so will throw and error in the event that a source file would overwrite a destination directory.

### How do I get deleted files to be apart of a sync operation?

You want to use toggle the delete argument with calling `sync` to remove any file on the destination that doesn't appear in the source. For files that do conflict, only newer files in the source will be uploaded.

```python
stow.sync(source, destination, delete=True)
```
