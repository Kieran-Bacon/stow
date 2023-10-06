from stow.managers.kubernetes import Kubernetes

m = Kubernetes()

files = m.ls()

for file in files:
    print(file)