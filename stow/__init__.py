__version__ = "1.1.6"

import os
from .artefacts import Artefact, File, Directory, SubFile, SubDirectory
from .manager import StatelessManager, SubManager
from . import exceptions

env = os.environ

# Expose the util functions
from .utils import (
    find,
    connect,
    parseURL
)

# Create the stateless manager
Manager = StatelessManager()

artefact = Manager.artefact
abspath = Manager.abspath
basename = Manager.basename
name = Manager.name
extension = Manager.extension
commonpath = Manager.commonpath
commonprefix = Manager.commonprefix
dirname = Manager.dirname
expanduser = Manager.expanduser
expandvars = Manager.expandvars
isabs = Manager.isabs
join = Manager.join
normcase = Manager.normcase
normpath = Manager.normpath
realpath = Manager.realpath
relpath = Manager.relpath
samefile = Manager.samefile
sameopenfile = Manager.sameopenfile
samestat = Manager.samestat
split = Manager.split
splitdrive = Manager.splitdrive
splitext = Manager.splitext
md5 = Manager.md5
isfile = Manager.isfile
isdir = Manager.isdir
islink = Manager.islink
ismount = Manager.ismount
getctime = Manager.getctime
getmtime = Manager.getmtime
getatime = Manager.getatime
exists = Manager.exists
lexists = Manager.lexists
touch = Manager.touch
mkdir = Manager.mkdir
localise = Manager.localise
open = Manager.open
ls = Manager.ls
iterls = Manager.iterls
get = Manager.get
put = Manager.put
cp = Manager.cp
mv = Manager.mv
sync = Manager.sync
rm = Manager.rm
supports_unicode_filenames = os.path.supports_unicode_filenames