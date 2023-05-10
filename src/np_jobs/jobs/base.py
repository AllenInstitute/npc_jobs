from __future__ import annotations

import dataclasses
import doctest
import time
from typing import NamedTuple, Optional


class JobTuple(NamedTuple):
    """Tuple with session and added required inputs.
    
    >>> from np_jobs import Job
    >>> job = JobTuple('123456789_366122_20230422', added=time.time())
    >>> assert isinstance(job, Job)
    """
    session: str
    added: int | float = 0
    priority: int = 0
    started: Optional[int | float] = None
    hostname: Optional[str] = None
    finished: Optional[int | float] = None
    error: Optional[str] = None


@dataclasses.dataclass
class JobDataclass:
    """Dataclass with only session required.
    
    >>> from np_jobs import Job
    >>> job = JobDataclass('123456789_366122_20230422')
    >>> assert isinstance(job, Job)
    """
    session: str
    added: int | float = dataclasses.field(default_factory=time.time)
    priority: int = 0
    started: Optional[int | float] = None
    hostname: Optional[str] = None
    finished: Optional[int | float] = None
    error: Optional[str] = None

if __name__ == '__main__':
    doctest.testmod()