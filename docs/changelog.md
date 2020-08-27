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

## [0.0.3] -  2020-08-07

Added to artefacts properties to extract information about their paths a little easier. This will help massively with cutting down the code a user would have to write to filter/analysis a directory.

### Added
    - `basename` and `name` added to artefact - for directories this does the same thing - for files, name doesn't include the extension of the file
    - `extension` added to File to give a clean way of getting the file extension.
    - `Manager` not raised to init so that it can be accessed easier for extending managers