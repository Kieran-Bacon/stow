""" House utilities for the finding and creation of Managers """

import os
import dataclasses
import datetime

from typing import Union, Optional

from .types import TimestampLike, TimestampAble

def timestampToFloat(timestampLike: TimestampLike) -> float:
    return (timestampLike.timestamp() if isinstance(timestampLike, TimestampAble) else float(timestampLike))

def timestampToDatetime(timestamp: TimestampLike) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(timestampToFloat(timestamp), tz=datetime.timezone.utc)

def timestampToFloatOrNone(time: Optional[TimestampLike]) -> Union[float, None]:
    return time if time is None else (time.timestamp() if isinstance(time, TimestampAble) else float(time))

@dataclasses.dataclass
class ArtefactModifiedAndAccessedTime:
    modified_time: datetime.datetime
    accessed_time: datetime.datetime

    def __iter__(self):
        yield self.modified_time
        yield self.accessed_time

def utime(
        filepath: str,
        modified_time: Optional[TimestampLike],
        accessed_time: Optional[TimestampLike]
    ) -> ArtefactModifiedAndAccessedTime:
    """ Better utime interface for the updating of file times - defaults to preserving the original time and allowing

    Behaviour:
        1. Pass both times and have them interpreted correctly and set on the file
        2. Pass either a modified or an accessed time to set that one specifically
        3. Pass nothing to update both to now (default utime behaviour)
    """

    if modified_time is None:

        if accessed_time is None:
            # Neither time was set - update the file times to now (default)
            os.utime(filepath)
            stat = os.stat(filepath)

            return ArtefactModifiedAndAccessedTime(
                timestampToDatetime(stat.st_mtime),
                timestampToDatetime(stat.st_atime)
            )

        else:
            # The accessed time was set - the modified time needs to be read in to be preserved
            stat = os.stat(filepath)

            os.utime(filepath, (timestampToFloat(accessed_time), float(stat.st_mtime)))

            return ArtefactModifiedAndAccessedTime(
                timestampToDatetime(stat.st_mtime),
                timestampToDatetime(accessed_time)
            )

    else:
        # The modified time was set

        # Convert the modified time (true in regardless of access time)

        if accessed_time is None:
            # Access time was not set - preserve access time and updated modified time

            stat = os.stat(filepath)
            os.utime(filepath, (float(stat.st_atime), timestampToFloat(modified_time)))
            return ArtefactModifiedAndAccessedTime(
                timestampToDatetime(modified_time),
                timestampToDatetime(stat.st_atime)
            )

        else:
            # Both have been updated - set new times on filepath

            os.utime(filepath, (timestampToFloat(accessed_time), timestampToFloat(modified_time)))

            return ArtefactModifiedAndAccessedTime(
                timestampToDatetime(modified_time),
                timestampToDatetime(accessed_time)
            )