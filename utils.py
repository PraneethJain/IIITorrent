import contextlib
import logging
from typing import List, TypeVar, Sequence

import contexttimer


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


T = TypeVar('T', Sequence, memoryview)


def grouper(arr: T, group_size: int) -> List[T]:
    # Yield successive n-sized chunks from l.

    return [arr[i:i + group_size] for i in range(0, len(arr), group_size)]


TIMER_WARNING_THRESHOLD = 0.1


@contextlib.contextmanager
def check_time(check_name: str):
    with contexttimer.Timer() as timer:
        yield
    if timer.elapsed >= TIMER_WARNING_THRESHOLD:
        logger.warning('Too long %s (%.1lf s)', check_name, timer.elapsed)
