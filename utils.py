from typing import List, TypeVar, Sequence


T = TypeVar('T', Sequence, memoryview)


def grouper(arr: T, group_size: int) -> List[T]:
    # Yield successive n-sized chunks from l.

    return [arr[i:i + group_size] for i in range(0, len(arr), group_size)]
