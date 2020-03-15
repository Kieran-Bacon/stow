class Locals(LocalManager):

    def __init__(self, name, directories):
        super().__init__(name)

        # Unpack all the directories and keep references to the original managers
        directories = [os.path.expanduser(d) for d in directories]
        self._default = directories[0].split(os.path.sep)[-1]
        self._namesToPaths = {d.split(os.path.sep)[-1]: os.path.abspath(d) for d in directories}
        self._managers = {name: connect(name, manager='FS', path=path) for name, path in self._namesToPaths.items()}

    def refresh(self):
        for manager in self._managers.values():
            manager.refresh()

    def paths(self, artefactType = None):
        # Set up the paths for the manager
        return {
            "{sep}{}{sep}{}".format(name, path.strip(SEP), sep=SEP): art
            for name, manager in self._managers.items()
            for path, art in manager.paths().items()
            if artefactType is None or isinstance(art, artefactType)
        }

    @ staticmethod
    def _splitFilepath(filepath: str) -> (str, str):
        nodes = filepath.strip(SEP).split(SEP)
        return nodes[0], SEP + SEP.join(nodes[1:])

    def __getitem__(self, filepath: str):
        d, path = self._splitFilepath(filepath)
        if d not in self._managers:
            return self._managers[self._default][filepath]
        return self._managers[d][path]

    def __contains__(self, filepath: str):
        if isinstance(filepath, Artefact): return super().__contains__(filepath)
        d, path = self._splitFilepath(filepath)
        if d not in self._managers:
            return filepath in self._managers[self._default]
        return path in self._managers[d]


    def get(self, src_remote: str, dest_local):
        source_path = super().get(src_remote, dest_local)
        d, path = self._splitFilepath(source_path)
        if d not in self._managers:
            return self._managers[self._default].get(source_path, dest_local)
        return self._managers[d].get(path, dest_local)

    def put(self, src_local: str, dest_remote):
        with super().put(src_local, dest_remote) as (source_path, destination_path):
            d, path = self._splitFilepath(destination_path)

            if d not in self._managers:
                return self._managers[self._default].put(source_path, destination_path)
            return self._managers[d].put(source_path, path)

    def rm(self, filename, recursive: bool = False):
        path = super().rm(filename, recursive)
        d, path = self._splitFilepath(path)
        return self._managers[d].rm(path, recursive)