import stow

stow.get(
    "s3://kieran-bacon/videos/entertainment/series/Another/1",
    "./videos",
    overwrite=True,
    callback=stow.callbacks.ProgressCallback
)


# Sync is called with the progress callback
# the callback is instantiated with the sync operation name
# the callback is passed to the put command
# the count is increased when it is a directory
# each file synced gets a start call with the source and destination set
# files to be deleted are then instiantiated