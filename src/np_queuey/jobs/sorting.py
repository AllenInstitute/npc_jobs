"""
Pipeline sorting queue.

>>> PeeweeJobQueue.db.create_tables([PeeweeJobQueue])
>>> test1 = PeeweeJobQueue.create(folder='t1')
>>> test2 = PeeweeJobQueue.create(folder='t2')
>>> PeeweeJobQueue.next() == test1
True
>>> test1.is_started
False
>>> test1.is_started
False
>>> test1.finished = True
>>> _ = test1.save()
>>> test1.is_started
False
>>> test2 == PeeweeJobQueue.next()
True
>>> PeeweeJobQueue.db.drop_tables([PeeweeJobQueue])
>>> _ = PeeweeJobQueue.db.close()


>>> Sorting.db.create_tables([Sorting])
>>> s = '123456789_366122_20230422'
>>> _ = Sorting.delete().where(Sorting.folder == s).execute()
>>> test = Sorting.add(s, priority=99)
>>> Sorting.next() == test
True
>>> test.session
Session('123456789_366122_20230422')
>>> test.probes
'ABCDEF'
>>> test.set_started()
>>> test.is_started
True
>>> test.set_finished()
>>> Sorting.next() != test
True
>>> _ = test.delete_instance()
>>> _ = Sorting.db.close()
"""
from __future__ import annotations

import abc
import contextlib
import datetime
import pathlib
import time
import typing
from typing import Any, NamedTuple, Optional, Protocol, Union

import np_config
import np_session
import peewee

import np_queuey.utils as utils

DB_PATH = pathlib.Path(utils.DEFAULT_HUEY_SQLITE_DB_PATH).parent / 'sorting.db'

SessionArgs = Union[str, int, pathlib.Path, np_session.Session]

@typing.runtime_checkable
class Job(Protocol):
    """Base class for jobs."""
    
    @property
    @abc.abstractmethod
    def session(self) -> np_session.Session:
        """The Neuropixels Session to process.
        
        - each job must have a Session
        - each queue can only have one job per session (session is unique)
        """
    
    @property
    @abc.abstractmethod
    def priority(self) -> int:
        """
        Priority level for this job.
        Processed in descending order (then ordered by `added`).
        """
        
    @property
    @abc.abstractmethod
    def added(self) -> None | datetime.datetime:
        """
        When the job was added to the queue.
        Jobs processed in ascending order (after ordering by `priority`).
        """
        
    @property
    @abc.abstractmethod
    def hostname(self) -> None | str:
        """The hostname of the machine that is currently processing this session."""
        
    @property
    @abc.abstractmethod
    def finished(self) -> None | bool:
        """Whether the session has been verified as finished."""
    
    
@typing.runtime_checkable
class JobQueue(Protocol):
    """Base class for job queues."""
    
    @abc.abstractmethod
    def add(self, session_or_job: SessionArgs | Job, **kwargs) -> Job:
        """Add an entry to the queue with sensible default values."""
        
    @abc.abstractmethod
    def next(self) -> Job:
        """
        Get the next job to process.
        Sorted by priority (desc), then date added (asc).
        """
    
    @abc.abstractmethod
    def set_finished(self, session_or_job: SessionArgs | Job) -> None:
        """Mark a job as finished. May be irreversible, so be sure."""
        
    @abc.abstractmethod
    def set_started(self, session_or_job: SessionArgs | Job) -> None:
        """Mark a job as being processed. Reversible"""

    @abc.abstractmethod
    def set_queued(self, session_or_job: SessionArgs | Job) -> None:
        """Mark a job as requiring processing, undoing `set_started`."""
    
    @abc.abstractmethod
    def is_started(self, session_or_job: SessionArgs | Job) -> bool:
        """Whether the job has started processing, but not yet finished."""


def parse_session_or_job(session_or_job: SessionArgs | Job) -> np_session.Session:
    """Parse a session argument into a Neuropixels Session."""
    if isinstance(session_or_job, np_session.Session):
        return session_or_job
    if isinstance(session_or_job, Job):
        return session_or_job.session
    try:
        return np_session.Session(session_or_job)
    except np_session.SessionError as exc:
        raise TypeError(
            f'Unknown type for session_or_job: {session_or_job!r}'
            ) from exc


class JobTuple(NamedTuple):
    """Tuple of session, priority, and date added."""
    session: np_session.Session
    added: datetime.datetime
    priority: int = 0
    hostname: Optional[str] = None
    finished: Optional[bool] = None
    
    
def get_job(session_or_job: SessionArgs | Job) -> Job:
    """Get a default job."""
    if isinstance(session_or_job, Job):
        return session_or_job
    return JobTuple(
        session=parse_session_or_job(session_or_job),
        added=datetime.datetime.now(),
        )


class PeeweeJobQueue(peewee.Model):
    """Job queue implementation using `peewee` ORM.
    
    - instances implement the `Job` protocol
    - classmethods implement the `JobQueue` protocol
    """
    
    folder = peewee.TextField(primary_key=True)
    """Session folder name, e.g. `123456789_366122_20230422`"""
    
    priority = peewee.IntegerField(default=0, constraints=[peewee.SQL('DEFAULT 0')])
    """Priority level for processing this session. Higher priority sessions will be processed first."""

    added = peewee.TimestampField(default=time.time, constraints=[peewee.SQL('DEFAULT CURRENT_TIMESTAMP')])
    """When the session was added to the queue."""

    hostname = peewee.TextField(null=True)
    """The hostname of the machine that is currently processing this session."""

    finished = peewee.BooleanField(null=True)
    """Whether the session has been verified as finished."""

    errored = peewee.TextField(null=True)
    """Whether the session has errored during processing."""

    @property
    def session(self) -> np_session.Session:
        """Neuropixels Session the job belongs to."""
        return np_session.Session(self.folder)

    class Meta:
        database = peewee.SqliteDatabase(
                database=DB_PATH,
                pragmas=dict(
                    journal_mode='truncate', # 'wal' not supported on NAS
                    synchronous=2,
                ),
                autoconnect=False,
            )
        
    db = Meta.database

    @classmethod
    def parse_job(cls, job: Job) -> dict[str, Any]:
        """Parse a job into db column values."""
        return dict(
            folder=job.session.folder,
            priority=job.priority,
            added=job.added,
            hostname=job.hostname,
            finished=job.finished,
            )
        
    @classmethod
    def add(cls, session_or_job: SessionArgs | Job, **kwargs) -> Job:
        """
        Add an session or job to the queue, kwargs as
        overwriting fields. Default field values already set in db.
        """
        job_kwargs = cls.parse_job(get_job(session_or_job))
        job_kwargs.update(kwargs)
        with cls.db.connection_context():
            return cls.create(
                **job_kwargs,
                )


    @classmethod
    def next(cls) -> JobQueue | None:
        """Get the next job to process - by priority (desc), then date added (asc)."""
        return cls.select_unprocessed().get_or_none()
            
            
    @classmethod
    def select_unprocessed(cls) -> peewee.ModelSelect:
        """Get the jobs that have not been processed yet.

        Sorted by priority level (desc), then date added (asc).
        """
        return (
            cls.select().where(
                # syntax here is non-standard: don't use `is None` or `not True`
                ((cls.finished == None) | (cls.finished == 0) | (cls.finished == False))
                & 
                ((cls.hostname == None) | (cls.hostname == ''))
                &
                ((cls.errored == None) | (cls.errored == ''))
            ).order_by(cls.priority.desc(), cls.added.asc())
        )

    def self_or_job(self, session_or_job: Optional[SessionArgs | Job] = None) -> PeeweeJobQueue:
        if session_or_job is None:
            instance = self
        else:
            job = parse_session_or_job(session_or_job)
            instance = self.__class__.get(self.__class__.folder == job.folder)
        return instance
            
    def set_finished(self, session_or_job: Optional[SessionArgs | Job] = None) -> None:
        """Mark this session as finished. May be irreversible, so be sure."""
        with self.db.connection_context():
            instance = self.self_or_job(session_or_job)
            instance.finished = True
            instance.save()
        
    def set_started(self, hostname: str = np_config.HOSTNAME, session_or_job: Optional[SessionArgs | Job] = None) -> None:
        """Mark this session as being processed, on `hostname` if provided, defaults to <localhost>."""
        with self.db.connection_context():
            instance = self.self_or_job(session_or_job)
            instance.hostname = hostname
            instance.finished = False
            instance.save()
        
    def set_queued(self, session_or_job: Optional[SessionArgs | Job] = None) -> None:
        """Mark this session as requiring processing, undoing `set_started`."""
        with self.db.connection_context():
            instance = self.self_or_job(session_or_job)
            instance.hostname = None
            instance.finished = False
            instance.save()

    def set_errored(self, exception: Optional[Exception] = None, session_or_job: Optional[SessionArgs | Job] = None) -> None:
        """Mark this session as errored, leaving `hostname` field intact.
        Inserts `exception` into `errored` field, if provided."""
        with self.db.connection_context():
            instance = self.self_or_job(session_or_job)
            instance.errored = f'{exception!r}'.replace('\\','/') if exception else f'{Exception("unknown error on " + instance.hostname)!r}'
            instance.finished = False
            instance.save()
        
    @property
    def is_started(self) -> bool:
        """Whether the job has started processing, but not finished."""
        return bool(self.hostname) and not bool(self.finished)
   
   
class Sorting(PeeweeJobQueue):

    
    probes = peewee.TextField(
        null=False, 
        default='ABCDEF',
        constraints=[peewee.SQL('DEFAULT ABCDEF'),],)
    """Probe letters for sorting, e.g. `ABCDEF`"""

    def update_probes(self, probes: str) -> None:
        """Update the probes to sort."""
        with self.db.connection_context():
            self.probes = probes
            self.save()
            
        
def add_verbose_names_to_peewee_fields(*peewee_cls) -> None:
    """Add the docstring of each `peewee_cls` field to its `verbose_name` attribute."""
    for cls in peewee_cls:
        for field in (_ for _ in cls.__dict__ if isinstance(_, peewee.Field)):
            field.verbose_name = field.__doc__
        
        
add_verbose_names_to_peewee_fields(PeeweeJobQueue, Sorting)


if __name__ == '__main__':
    import doctest
    doctest.testmod()
