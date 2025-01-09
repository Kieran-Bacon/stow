# Changelog

## [1.4.2] - 2025-01-09

### Fixed

- Added `py.typed` to the package allowing for type annotations to be used.

## [1.4.1] - 2024-04-25

### Changed

- Added tags to the interface of the abstract implementations
- Added tags to the interface in the manager
- Updated amazon to use the tags field
- Add tests for the setting of tags via mv, put, and putBytes.

### Fixed

- Fixed error with artefact tags where `_tags` was not defined and could not be used to return.

## [1.4.0] - 2024-04-23

### Added

- Implemented `get_tags`, `set_tags`, `get_metadata`, and `set_metadata` public methods on the `Manager` interface + expanded the artefact interface with `tags` and `metadata` properties.

### Changes

- Changed mv behaviour to rely on `delete_source` signal now passed to `_put` implementations. This is to ensure that `mv` continues to work correctly in a threaded environment. Deletes need to happen once puts have been completed.
- Extended the artefact `delete` interface to resemble the options on the `Manager.rm` interface.

### Fixes

- Fixed issue with windows filesystem where stow couldn't process or accept artefacts with filepaths longer than 256 characters
- Fixed issue with `S3` bucket creation not including the default region
- Improved log error and warning messages
- Fixed bug in `Manager` where a mv command between non-compatible managers would do nothing

## [1.3.1] - 2024-01-21

### Added

- Added `--count` to the cli ls command to return the number of items at the listed location rather than the items themselves.

### Changed

- Extended the progress callback to handle progress inside the jupyter notebook environment with the flag `notebook=True`.
- Changed how filepaths are displayed in the progress callback, governed by `description_length`

### Fixed

- Too many calls to delete from `FS`
- Fixed the boto3 libary using threads which when running inside a thread pool executor was causing issues.
- Improved callback calls in `S3`
- `WorkerPoolConfig` no longer prevents more than 100 unfinished tasks at once in the executor.


## [1.3.0] - 2024-01-15

### Added

- Add `set_artefact_time` method to the stateless and manager interface
- Add stow `cli` program
- Added `CommandLineConfig` class to `fs` and `s3` managers to allow for use with the CLI
- Add support for soft/hard symlinks

### Changed

- Extended the save interface on artefacts, to include a callback and file times
- Changed callback interface plus implement filesystem methods to use the callback interface
- Extend `get` interface to include times and callback
- Extend `touch` interface to include times and metadatas
- Update the `__str__` function of `PartialArtefact` to correctly generate the str representation of its target
- Make composeCallback method return an object of Callback not the class.
- Added additional tests and annotate lines to avoid in coverage
- Added profile_name and session token url query params to the signature parser for `Amazon` manager
- Have the `fs._putBytes` method update the time of the file in the same operation to reduce the number method invocations.
- Change `File.content` from property into a function that returns content or sets content if passed
- Changed manager interfaces to take path for the default path of the managers - though not required
- `S3` now supports the creation and deletion of buckets

### Fixed

- Fixed issue with touch when touching an artefact that exists, method doesn't return the expected type
- Fixed issue with `PartialArtefact` where it was not a `os.PathLike` object.

### Removed

- Removed the public `md5` method - it is available through `stow.digest` which defaults to md5 hashing algorithm.

## [1.2.2] - 2023-05-22

### Added

- add `cwd` to stow interface.

### Changed

- `localise` and `open` are now available without the contextmanager.
- Change the separator behaviour to work better with remote managers.
- Update sync behaviour.

### Fixed

- Fixed issue with `Amazon` manager where artefacts moved from one bucket to another were not deleted in the source bucket, but deleted in the second bucket.

## [1.2.1] - 2023-04-03

### Fixed

- Fixed issue with `dirname` where it incorrectly handled artefacts leading to a `urllib` parse error.

## [1.2.0] - 2023-03-20

### Added

- Added support for the setting of motified/accessed time on artefacts via the manager and artefact objects.
- Added `iterls` methods to manager and `Directory` objects for efficient iteration.
- Add `Callbacks` objects for getting upload and download progression.
- Add `manager` method to interface
- Add `root` property to managers
- Add `setmtime`, `setatime` and `mklink` to manager and stateless interface
- Add hashing algorithm support for Amazon objects


### Changed

- Extended `relpath` to take `separator` parameter so that the separator used isn't platform specific.
- Updated `sync` to work for files and directories. Extended the interface to allow the choice between a simply modified time comparison or a more complicated digest comparason function
- Updated the hash of artefacts to be the hash of their `abspath`
- Updated the equality method for artefacts to take type into account
- Refactored `Filesystem` manager to work better between linux and windows

### Fixed

- Fixed inability to delete empty directories without passing overwrite set `True`
- Simplified utils decorator method
- Fixed issue where artefact timestamps were not updated when set
- Fixed `Amazon.cp` issue effecting empty directories leading to new files


## [1.1.6] - 2021-03-04

### Fixed

- `cp` method didn't correctly delete destination objects if the object was an empty directory or empty file.
- `get` in the stateless interface didn't return file bytes when no destination was provided + it was a required field.

## [1.1.4] - 2021-09-07

### Fixed

- mimetype were being created for files put into s3 using the `mimetype` builtin package. The method `guess_type` returns none for extentions that are not apart of the standard set. This caused the CLI to through and error as it requires a valid content-type is set or nothing. Resolved by choosing s3 default if None is returned

---

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