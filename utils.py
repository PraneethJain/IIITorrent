from math import floor, log
from typing import List, TypeVar, Sequence


T = TypeVar('T', Sequence, memoryview)


def grouper(arr: T, group_size: int) -> List[T]:
    # Yield successive n-sized chunks from l.

    return [arr[i:i + group_size] for i in range(0, len(arr), group_size)]


UNIT_BASE = 2 ** 10
UNIT_PREFIXES = 'KMG'


def humanize_size(size: int) -> str:
    if size < UNIT_BASE:
        return '{} bytes'.format(size)
    unit = floor(log(size, UNIT_BASE))
    unit_name = UNIT_PREFIXES[min(unit, len(UNIT_PREFIXES)) - 1] + 'iB'
    return '{:.1f} {}'.format(size / UNIT_BASE ** unit, unit_name)


def humanize_speed(speed: int) -> str:
    return humanize_size(speed) + '/s'


def floor_to(x: float, ndigits: int) -> float:
    scale = 10 ** ndigits
    return floor(x * scale) / scale
