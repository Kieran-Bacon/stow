# Sample package usage examples

## Processing some files from one place to another

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