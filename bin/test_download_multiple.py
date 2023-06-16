import stow

stow.env['AWS_PROFILE'] = 'personal'

# stow.get(
#     "s3://kieran-bacon/archive/datasets/SED/combined/annotations/golden-annotation/",
#     "./annotations",
#     overwrite=True,
#     callback=stow.callbacks.ProgressCallback()
# )
import logging
logging.basicConfig(level=logging.INFO)

stow.touch('dir1/file1.txt')
stow.touch('dir1/file2.txt')

stow.rm('dir1', recursive=True, callback=stow.callbacks.ProgressCallback('dir1'))

stow.touch('file1.txt')

stow.rm('file1.txt', recursive=True, callback=stow.callbacks.ProgressCallback())

# Sync is called with the progress callback
# the callback is instantiated with the sync operation name
# the callback is passed to the put command
# the count is increased when it is a directory
# each file synced gets a start call with the source and destination set
# files to be deleted are then instiantiated
