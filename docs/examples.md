# Sample package usage examples

## Manipulating artefacts

### Putting artefacts

```python
# Put an artefact in another location with the same name
stow.put(path, stow.join(destination, stow.basename(path)))

# Putting artefacts in a directory
for artefact in stow.ls(path):
    stow.put(artefact, stow.join(destination, artefact.basename))
```

### Iterative over files in a nested hierarchy

```
folder
├── sub-directory-1
│   ├── 1.jpg
│   ├── 2.jpg
│   ├── ...
│   └── n.jgp
├── sub-directory-2
│   ├── a1.jpg
│   ├── a1-annotation.xml
│   ├── a2.jpg
│   ├── ...
│   └── an.jgp
└── sub-directory-3
    ├── manifest.jsonl
    ├── performance.csv
    ├── beach.jpg
    └── party.jpg
```

```python
# Get all files to do some work
for artefact in stow.ls(folder, recursive=True):
    if isinstance(artefact, Directory):
        continue

    # Do something with the files
    pass

# Get only images
for artefact in stow.ls(folder, recursive=True):
    if isinstance(artefact, File) and artefact.extension == 'jpg':
        # Do something with the files
        pass
```

### Splitting directory contents

```python
# Split a directory - preserve the original with copy (cp) delete the original directory with move (mv)
for file in stow.ls(folder):

    if file.modifiedTime > someDatetime:
        stow.cp(file, stow.join(destination, 'recent', file.basename))

    else:
        stow.cp(file, stow.join(destination, 'older', file.basename))
```

### Processing files

```python
import stow

source = stow.artefacts(stow.join(BASE, 'source'))
destination = stow.mkdir(stow.join(BASE, 'destination'))

for art in source.ls(recursive=True):

    df = pd.read_csv(art.path)

    # Do stuff
    ...

    df.to_csv(stow.join(destination, art.basename))
```

## Synchronising directories

### merge

For items in a directory, consider the files only and put them into the destination corresponding to their relative path in the source directory.

```python
for artefact in stow.ls(directory):
    if isinstance(artefact, stow.File):
        stow.put(artefact, stow.join(destination, stow.relpath(artefact, directory)))
```

We can control this behaviour easily by filtering to specific `Files` by adding conditions through use of the artefact interface.

```python
for artefact in stow.ls(directory):
    if isinstance(artefact, stow.File) and artefact.size > 4000 and artefact.extension == 'json':
        stow.put(artefact, stow.join(destination, stow.relpath(artefact, directory)))
```

!!! Note
    When putting an artefact into a location, `stow` will ensure that the location exists. This allows you to ignore moving or initialising directories to hold the artefacts being put.

### sync

To update files in a location that have been modified more recently else where, use the `sync` method. `Files` in the source will update those in the destination, but, the source will not be updated.

```python
stow.sync(directory, destination)
```

To synchronise deletions, you can pass the keyword argument `delete=True` to delete any files from the destination that do not exist in the source.

```python
stow.sync(directory, destination, delete=True)
```