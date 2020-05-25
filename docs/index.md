warehouse is a package that allows for the seemless communication between vairous different data warehouse systems via a hemogenous "manager" interface. This allows code to be written for a local filestore and then immediately have a AWS bucket plugged in without having to update code.

## Managers

Managers | import path | connect manager names
--- | --- | ---
Amazon AWS | warehouse.managers.Amazon | S3, AWS
Filesystem | warehouse.managers.FS | FS, LFS

```python

fs = warehouse.connect(manager="FS", path="~/Documents")
s3 = warehouse.connect(manager="S3", bucket="bucket-name")  # Assuming aws creds are installed - else pass them

aws = warehouse.managers.Amazon("bucket-name")
aws = warehouse.managers.Amazon("bucket-name", accesskey="***")
```