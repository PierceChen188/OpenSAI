from enum import Enum, auto


class SubTaskStatus(Enum):
    UN_WORK = auto()
    READY = auto()
    WORK = auto()
    FINISH = auto()
    EARLY_EXIT = auto()
    DROPPED = auto()
