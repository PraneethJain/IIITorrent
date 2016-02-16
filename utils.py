import logging
from typing import List, TypeVar, Sequence


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


T = TypeVar('T', Sequence, memoryview)


def grouper(arr: T, group_size: int) -> List[T]:
    # Yield successive n-sized chunks from l.

    return [arr[i:i + group_size] for i in range(0, len(arr), group_size)]
