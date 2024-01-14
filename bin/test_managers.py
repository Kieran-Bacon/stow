
import stow
from stow.managers.amazon import Amazon
# from stow.managers.kubernetes import Kubernetes

# m = Kubernetes()

# files = m.ls()

# for file in files:
#     print(file)

# s3 = Amazon('/herosystems-athena/Unsaved')
# s3 = Amazon()

# result = s3.ls(recursive=True)

# for a in result:
#     print(a)

# print(s3.toConfig())

fs = stow.connect('FS')

fs.ls()