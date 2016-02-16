import logging
from typing import List, TypeVar, Sequence, Optional

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


T = TypeVar('T', Sequence, memoryview)


def grouper(arr: T, group_size: int) -> List[T]:
    # Yield successive n-sized chunks from l.

    return [arr[i:i + group_size] for i in range(0, len(arr), group_size)]


BYTES_PER_MIB = 2 ** 20
BYTES_PER_GIB = 2 ** 30


def humanize_size(size: int) -> str:
    if size >= BYTES_PER_GIB:
        unit = 'GiB'
        factor = BYTES_PER_GIB
    else:
        unit = 'MiB'
        factor = BYTES_PER_MIB
    return '{:.1f} {}'.format(size / factor, unit)


def humanize_speed(speed: int) -> str:
    return '{:.1f} MiB/s'.format(speed / BYTES_PER_MIB)
