# Managers

Every manager presents the same interface for interacts with their files. This allows you to seemingly read and write to a file or directory despite it being in a remote location.

```python

import stow

stow.connect()