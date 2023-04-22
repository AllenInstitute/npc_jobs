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
>>> _ = test.delete_instance()
>>> _ = Sorting.db.close()
"""
from __future__ import annotations

import abc
import datetime
import pathlib
import typing
from typing import Protocol

import np_config
import np_session
import peewee

import np_queuey.utils as utils

DB_PATH = pathlib.Path(utils.DEFAULT_HUEY_SQLITE_DB_PATH).parent / 'sorting.db'

@typing.runtime_checkable
class Job(Protocol):
    """Base class for jobs."""
    
    @property
    @abc.abstractmethod
    def session(self) -> np_session.Session:
        """The Neuropixels Session to process."""
    
    @property
    @abc.abstractmethod
    def priority(self) -> int:
        """
        Priority level for this job.
        Processed in descending order (then ordered by `added`).
        """
        
    @property
    @abc.abstractmethod
    def added(self) -> datetime.datetime: 
        """
        When the job was added to the queue.
        Jobs processed in ascending order (after ordering by `priority`).
        """
        
    @property
    @abc.abstractmethod
    def hostname(self) -> str:
        """The hostname of the machine that is currently processing this session."""
        
    @property
    @abc.abstractmethod
    def finished(self) -> bool:
        """Whether the session has been verified as finished."""
   
    @property
    @abc.abstractmethod
    def is_started(self) -> bool:
        """Whether the job has started processing, but not yet finished."""
    
    
@typing.runtime_checkable
class JobQueue(Protocol):
    """Base class for job queues."""
    
    @abc.abstractmethod
    def add(self, session_or_job: str | int | np_session.Session | Job, **kwargs) -> Job:
        """Add an entry to the queue with sensible default values."""
        
    @abc.abstractmethod
    def next(self) -> Job:
        """
        Get the next job to process.
        Sorted by priority (desc), then date added (asc).
        """
    
    @abc.abstractmethod
    def set_finished(self, job: Job) -> None:
        """Mark a job as finished. Not reversible, so be sure."""
        
    @abc.abstractmethod
    def set_started(self, job: Job) -> None:
        """Mark a job as being processed. Reversible"""

    @abc.abstractmethod
    def set_queued(self, job: Job) -> None:
        """Mark a job as requiring processing, undoing `set_started`."""


class PeeweeJobQueue(peewee.Model):
    """Job queue implementation using `peewee` ORM.
    
    - instances implement the `Job` protocol
    - classmethods implement the `JobQueue` protocol
    """
    
    folder = peewee.CharField(primary_key=True)
    """Session folder name, e.g. `123456789_366122_20230422`"""
    
    priority = peewee.IntegerField(default=0)
    """Priority level for processing this session. Higher priority sessions will be processed first."""

    added = peewee.DateTimeField(default=datetime.datetime.now)
    """When the session was added to the queue."""

    hostname = peewee.CharField(default='')
    """The hostname of the machine that is currently processing this session."""

    finished = peewee.BooleanField(default=False)
    """Whether the session has been verified as finished."""


    @property
    def session(self) -> np_session.Session:
        """Neuropixels Session the job belongs to."""
        return np_session.Session(self.folder)

    class Meta:
        database = peewee.SqliteDatabase(
                database=DB_PATH,
                pragmas=dict(
                    journal_mode='delete', # 'wal' not supported on NAS
                    synchronous=2,
                ),
            )
        
    db = Meta.database

    @classmethod
    def add(cls, session_or_job: str | int | np_session.Session | Job, **kwargs) -> Job:
        """
        Add an entry to the queue with `folder` from `job`, kwargs as
        fields. Default field values already set in db.
        """
        session_or_job = np_session.Session(session_or_job) if isinstance(session_or_job, (str, int)) else session_or_job
        folder = session_or_job.folder if isinstance(session_or_job, np_session.Session) else session_or_job.session.folder
        if isinstance(session_or_job, Job):
            kwargs.setdefault('priority', session_or_job.priority)
        return cls.create(
            folder=folder,
            **kwargs,
            )


    @classmethod
    def next(cls) -> Job:
        """Get the next job to process - by priority (desc), then date added (asc)."""
        return cls.select_unprocessed().get()
            
            
    @classmethod
    def select_unprocessed(cls) -> peewee.ModelSelect:
        """Get the jobs that have not been processed yet.

        Sorted by priority level (desc), then date added (asc).
        """
        return (
            cls.select().where(
                (cls.finished == False) & (cls.hostname == '')
            ).order_by(cls.priority.desc(), cls.added.asc())
        )


    def set_finished(self) -> None:
        """Mark this session as finished. Not reversible, so be sure."""
        self.finished = True
        self.save()
        
    def set_started(self, hostname: str = np_config.HOSTNAME) -> None:
        """Mark this session as being processed on `hostname`, defaults to <localhost>."""
        self.hostname = hostname
        self.finished = False
        self.save()
        
    def set_queued(self) -> None:
        """Mark this session as requiring processing, undoing `set_started`."""
        self.hostname = ''
        self.finished = False
        self.save()

    @property
    def is_started(self) -> bool:
        """Whether the job has started processing, but not finished."""
        return bool(self.hostname) and not bool(self.finished)
   
   
class Sorting(PeeweeJobQueue):

    probes = peewee.CharField(null=False, default='ABCDEF')
    """Probe letters for sorting, e.g. `ABCDEF`"""

            
        
def add_verbose_names_to_peewee_fields(*peewee_cls) -> None:
    """Add the docstring of each `peewee_cls` field to its `verbose_name` attribute."""
    for cls in peewee_cls:
        for field in (_ for _ in cls.__dict__ if isinstance(_, peewee.Field)):
            field.verbose_name = field.__doc__
        
        
add_verbose_names_to_peewee_fields(PeeweeJobQueue, Sorting)


if __name__ == '__main__':
    import doctest
    doctest.testmod()
