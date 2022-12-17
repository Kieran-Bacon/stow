import stow

stow.get(
    "s3://kieran-bacon/videos/entertainment/series/Another/1",
    "./videos",
    overwrite=True,
    Callback=stow.callbacks.ProgressCallback
)