class ArtefactNotFound(Exception):
    pass

class ArtefactNotMember(Exception):
    pass

class ArtefactTypeError(TypeError):
    pass

class OperationNotPermitted(Exception):
    pass

class InvalidPath(ValueError):
    pass