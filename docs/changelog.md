# Changelog


## [0.2.0] - 2020-9-15

### Added

- `Manager.sync` added which pushes files in the source directory to the target if and only if they are more recently edited than the artefacts on the remote

### Changes
- `Manager.mkdir` has been extended with two defaulted arguments for ignoreExists and overwrite. Ignore exists allows multiple calls to mkdir on the same directory do nothing and not affect contents of the target. OVerwrite allows the multiple calls to take place but for the content to be removed (makes an empty directory)
- Added `__version__` to the init of stow and made setup reference this version

### Fixes
- Files created by the local filesystem didn't include their timezone which meant comparison with s3 files not possible. They have been set aware and time is in UTC.

---
## [0.1.2] - 2020-09-01

### Fixed
- stateless join function which was enforcing its own protocol handle. This has been passed to the manager
- s3 manager now correctly handles s3 protocols when joining paths

### Added
- tests for the stateless and manager join functions

---
## [0.1.1] - 2020-08-30

### Changed
- __contains__ logic changed to check against manager so that if edits have happened behind the scenes that they are    updated in the manager

### Fixes
- amazon isdir couldn't verify to level directories due to a naming issue which has been resolved

---
## [0.1.0] -  2020-08-28

Added to the stateless interface and corrected issues with artefact interface + manager interface

### Added
- isabs, abspath, relpath, commonprefix, commonpath to stateless interface
- isabs added as abstract method to managers
- isEmpty added to Directories

### Changed
- Put method now requires a strategy for putting an artefact onto a directory
- Get method throws ArtefactNotFound error when get source isn't found instead of ArtefactNotMember
- join doesn't use relpath - it joins and cleans the input

---
## [0.0.3] -  2020-08-07

Added to artefacts properties to extract information about their paths a little easier. This will help massively with cutting down the code a user would have to write to filter/analysis a directory.

### Added
- `basename` and `name` added to artefact - for directories this does the same thing - for files, name doesn't include the extension of the file
- `extension` added to File to give a clean way of getting the file extension.
- `Manager` not raised to init so that it can be accessed easier for extending managers

---