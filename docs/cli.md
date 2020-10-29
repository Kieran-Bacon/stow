# Stow CLI

Commands like

```python
stow.get(source, destination)
stow.put(source, destination, overwrite=True)
```

becomes

```bash
stow get s3://example-bucket local_directory
stow put local_directory s3://example-bucket --overwrite
stow sync local_directory local_directory --delete
```