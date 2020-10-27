class ArtefactNotFound(FileNotFoundError):
    """ An artefact cannot be found at a location """
    pass

class ArtefactNotMember(Exception):
    """ An artefact is not a member of a `Manager` or `Directory` """
    pass

class ArtefactTypeError(TypeError):
    """ Expected an artefact of another type """
    pass

class ArtefactNoLongerExists(OSError):
    """ The artefact cannot be accessed because its persistent representation has been deleted or moved. """
    pass

class OperationNotPermitted(Exception):
    """ You do not have permission to perform this action """
    pass

class InvalidPath(ValueError):
    """ Path given is not a valid stow path. See documentation """
    pass