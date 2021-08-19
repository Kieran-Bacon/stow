# Changelog

## [1.1.3] - 2021-08-15

### Fixed

- s3 object key regex meant that files and directories that had spaces were rejected however they are accepted. Update regex and test for files with spaces.

---

## [1.1.2] - 2021-07-15

### Fixed

- Content-type for artefacts uploaded to Amazon via stow were not being inferred from the extension. This change is an immediate fix for the issue of having incorrect content-types. content-type feature now in works

---

## [1.1.1] - 2021-05-13

### Fixed

- Bug on storage class in signature where the value wasn't being read for initialising new managers. Couldn't set storage class using stateless interface.

---

## [1.1.0] - 2021-05-08

### Added

- Metadata to the package deployment (information viable on pypi)
- Add `StorageClass` selection and functionality to the `Amazon` manager. This allows stow to put objects into s3 in not just in the 'STANDARD' type.

### Changed

- Updated documentation

---
## [1.0.1] - 2021-04-30

### Added

- `SSH` manager implementation and tests, giving stow the ability to communicate and connect with remote machines via the ssh protocol. Information about how to use the new `Manager` can be found in the [documentation](/managers)
- `Manager` and `Artefact` seralisation has been introduced
- `Directory` has method `empty` added to its interface
- `ArtefactNotAvailable` exception added as a new possible situation for `SSH`. Occurs when

### Changed

- `Manager` objects not extend a `ManagerInterface` object which can be used by `Artefacts` for syntax highlighting.
- `_get`, `_getBytes`, `_cp`, `_mv`, `_rm` have had their interfaces changed to take the object (that must exist) instead of its manager path.
- Allowed `Manager` implementations to return `Artefact` objects during functions that typically create new objects to avoid subsequent calls to fetch object information already known to the implementation.
- Updated documentation

### Fixed

- Issue with `join` not resolving separators correctly when on windows.
- Fixed a hidden issue with `_updateArtefactObjects` - which was possibly not correctly identifying the right artefact to correct

---
## [1.0.1-alpha] - 2021-03-12

### Added

- Added `name` and `extension` to the stateless interface of the package

---
## [1.0.0-alpha] - 2021-02-25

### Added

- Added all os.path methods to the manager and stateless interface
- Updated and created documentation for the interface
- Expanded test coverage
- Tested system on windows
- Made artefacts compatible with path-like interfaces (made the path-like)

---
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