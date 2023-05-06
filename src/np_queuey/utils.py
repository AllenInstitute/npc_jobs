"""Utilities for np_queuey, should be importable from anywhere in the project
(except `types` module)."""
from __future__ import annotations
import contextlib

import dataclasses
import datetime
import pathlib
import time
from typing import Any, Generator, NamedTuple, Optional, Type

import np_config
import np_logging
import np_session

from np_queuey.types import Job, SessionArgs, JobQueue, JobT, JobQueueT

logger = np_logging.getLogger(__name__)

CONFIG: dict[str, Any] = np_config.fetch('/projects/np_queuey/config')

DEFAULT_HUEY_SQLITE_DB_PATH: str = CONFIG['shared_huey_sqlite_db_path']

DEFAULT_HUEY_DIR: pathlib.Path = pathlib.Path(
    DEFAULT_HUEY_SQLITE_DB_PATH
).parent
"""Directory for shared resources (tasks, sqlite dbs, huey instances...)"""


class JobTuple(NamedTuple):
    """Tuple with session and added required inputs.
    
    >>> job = JobTuple('123456789_366122_20230422', datetime.datetime.now())
    >>> assert isinstance(job, Job)
    """
    session: str
    added: float
    priority = 0
    started: Optional[int | float] = None
    hostname: Optional[str] = None
    finished: Optional[int] = None
    error: Optional[str] = None

@dataclasses.dataclass
class JobDataclass:
    """Dataclass with only session required.
    
    >>> job = JobDataclass('123456789_366122_20230422')
    >>> assert isinstance(job, Job)
    """
    session: str
    added: float = dataclasses.field(default_factory=time.time)
    priority: int = 0
    started: Optional[int | float] = None
    hostname: Optional[str] = None
    finished: Optional[int] = None
    error: Optional[str] = None

def get_session(session_or_job: SessionArgs | Job) -> np_session.Session:
    """Parse a session argument into a Neuropixels Session.
    
    >>> get_session('123456789_366122_20230422')
    PipelineSession('123456789_366122_20230422')
    >>> assert _ == get_session(np_session.Session('123456789_366122_20230422'))
    """
    if isinstance(session_or_job, np_session.Session):
        return session_or_job
    try:
        return np_session.Session(session_or_job)
    except np_session.SessionError as exc:
        raise TypeError(
            f'Unknown type for session_or_job: {session_or_job!r}'
            ) from exc

    
def get_job(session_or_job: SessionArgs | Job, job_type: Type[JobT] = JobDataclass) -> JobT:
    """Get a job with default values and just the `session` attr filled in.
    
    >>> job = get_job('123456789_366122_20230422')
    >>> assert isinstance(job, Job)
    >>> assert job == get_job(job)
    """
    if isinstance(session_or_job, job_type):
        return session_or_job
    return job_type(
        session=get_session(session_or_job).folder,
        )
    
    
@contextlib.contextmanager
def update_status(queue: JobQueueT, job: JobT) -> Generator[Any, None, None]:
    try:
        
        queue.set_started(job)
        logger.debug('Marked job as started: %s %s', queue, job.session)
        yield
    except BaseException as exc:
        if isinstance(exc, Exception):
            queue.set_errored(job, exc)
            logger.exception('Exception during processing %s %s', queue, job.session)
            return
        else: # KeyboardInterrupt, SystemExit etc:
            queue.set_queued(job)
            raise
    else:
        queue.set_finished(job)
        logger.debug('Marked job finished: %s %s', queue, job.session)

if __name__ == '__main__':
    import doctest
    doctest.testmod()
