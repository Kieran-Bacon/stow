import stow
import tempfile
import time

with tempfile.TemporaryDirectory() as directory:

    local = stow.touch(stow.join(directory, 'file-1.txt'))
    time.sleep(0.5)
    remote = stow.touch(stow.join("s3://herosystems-data-mountain/regression-suite", 'file-2.txt'))

    print(local)
    print(remote)
    print(stow.artefact(local).modifiedTime)
    print(stow.artefact(remote).modifiedTime)

    stow.sync(
        local,
        remote,
        callback=stow.callbacks.ProgressCallback()
    )

    print(stow.exists(local))
    print(stow.exists(remote))

    # print('----')

    stow.sync(
        remote,
        local,
        callback=stow.callbacks.ProgressCallback()
    )

    # print(stow.exists(local))
    # print(stow.exists(remote))

    print(stow.artefact(local).modifiedTime)
    print(stow.artefact(remote).modifiedTime)
