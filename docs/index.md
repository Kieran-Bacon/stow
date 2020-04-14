Storage is a package that allows for the seemless communication between vairous different data storage systems via a hemogenous "manager" interface. This allows code to be written for a local filestore and then immediately have a AWS bucket plugged in without having to update code.

## Managers

Managers | import path | connect manager names
--- | --- | ---
Amazon AWS | storage.managers.Amazon | S3, AWS
Filesystem | storage.managers.FS | FS, LFS

```python

fs = storage.connect(manager="FS", path="~/Documents")
s3 = storage.connect(manager="S3", bucket="bucket-name")  # Assuming aws creds are installed - else pass them

aws = storage.managers.Amazon("bucket-name")
aws = storage.managers.Amazon("bucket-name", accesskey="***")
```