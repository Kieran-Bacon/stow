

import stow
import datetime
import logging

logger = logging.getLogger('stow')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

def modified_time_comparison(s3File: stow.File, localFile: stow.File):
    """ When the data is synced into s3, the modified time is greater than the local source as it cannot be set and
    is set to the time of upload.

    The modified time of the file uploaded is saved as a piece of metadata so that we can see they are equal and know
    that they are the same.

    The source object is an s3 file, if the modified time or the saved time is less than the local file then it is
    historic and is the same file ergo do not update - only if the source modified time is in the future should we do
    anything
    """
    s3ModifiedTime = s3File.metadata.get('modified_time', s3File.modifiedTime)
    if isinstance(s3ModifiedTime, str):
        s3ModifiedTime = datetime.datetime.fromisoformat(s3ModifiedTime)

    synced_file = s3ModifiedTime <= localFile.modifiedTime

    if synced_file:
        # Though they are the same file - the timestamp on the s3 file is larger that the local file (otherwise we
        # would not have compared it in the first place) so we want to update the times of the file so that next time
        # we don't have to waste time comparing
        print('do not download')
        localFile.modifiedTime = s3File.modifiedTime

    return synced_file

# stow.get(
#     "s3://herosystems-data-mountain/test-area/SingleLegSitBacks",
#     'SingleLegSitBacks',
#     overwrite=True
# )

# stow.sync(
#     "s3://herosystems-data-mountain/test-area/SingleLegSitBacks-4",
#     'SingleLegSitBacks',
#     callback=stow.callbacks.ProgressCallback(),
#     artefact_comparator=modified_time_comparison,
#     # metadata={'modified_time': lambda source: source.modifiedTime.isoformat() if isinstance(source, stow.File) else None},
#     # worker_config=stow.WorkerPoolConfig(max_workers=0)
# )

# stow.sync(
#     'SingleLegSitBacks',
#     "s3://herosystems-data-mountain/test-area/SingleLegSitBacks-4",
#     callback=stow.callbacks.ProgressCallback(),
#     metadata={'modified_time': lambda source: source.modifiedTime.isoformat() if isinstance(source, stow.File) else None},
# )


# stow.sync(
#     "s3://herosystems-data-mountain/regression-suite",
#     r"C:\Users\kieran\Projects\hero\exercise-session-processing\bin\regression_suite\data - Copy",
#     callback=stow.callbacks.ProgressCallback(),
#     artefact_comparator=modified_time_comparison,
#     metadata={'modified_time': lambda source: source.modifiedTime.isoformat() if isinstance(source, stow.File) else None},
#     delete=True,
#     worker_config=stow.WorkerPoolConfig(shutdown=True)
# )

import concurrent.futures

stow.sync(
    r"C:\Users\kieran\Projects\hero\exercise-session-processing\bin\regression_suite\data - Copy - Copy",
    "s3://herosystems-data-mountain/regression-suite",
    callback=stow.callbacks.ProgressCallback(),
    metadata={'modified_time': lambda source: source.modifiedTime.isoformat() if isinstance(source, stow.File) else None},
    # delete=True
    overwrite=True,
    # worker_config=stow.WorkerPoolConfig()
)
