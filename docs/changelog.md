## [0.0.3] -  2020-08-07

Added to artefacts properties to extract information about their paths a little easier. This will help massively with cutting down the code a user would have to write to filter/analysis a directory.

### Added
    - `basename` and `name` added to artefact - for directories this does the same thing - for files, name doesn't include the extension of the file
    - `extension` added to File to give a clean way of getting the file extension.
    - `Manager` not raised to init so that it can be accessed easier for extending managers