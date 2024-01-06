import stow
from stow.managers.amazon import Amazon
from stow.artefacts import File

file: File = None

a = 'hello' in Amazon('hello')
a = file in Amazon('hello')

a = Amazon('hello').put(b'str', 'destination')


print(a)

Amazon('hello').exists(File(None, 'path', 3600, None))



