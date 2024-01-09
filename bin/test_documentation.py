import stow
# from stow.managers.amazon import Amazon
from stow.managers.kubernetes import Kubernetes
from stow.artefacts import File

# file: File = None

# a = 'hello' in Amazon('hello')
# a = file in Amazon('hello')

# a = Amazon('hello').put(b'str', 'destination')


# print(a)

# Amazon('hello').exists(File(None, 'path', 3600, None))

# stow.mv('file', 'file')


manager = Kubernetes()

for artefact in manager._ls('/kieran-development-area/exercise-session-processor-67bb7ff5cc-2pkz5/home/esp', recursive=True):
    print(artefact)

# print(manager._ls('/'))


