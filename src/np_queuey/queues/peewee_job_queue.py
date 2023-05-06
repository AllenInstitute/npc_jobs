"""
Peewee ORM implementation of both the `Job` and `JobQueue` protocols.

Instances of Peewee models are used to represent jobs in the queue, and
Peewee classmethods provide the queue functionality.

We're sharing a sqlite3 db on the Isilon, so we need to be careful about how we
connect - autoconnect is therefore disabled, and we use a context manager for
explicit opening/closing of the db connection for each transaction. As a
result, we only expose a subset of the Peewee API.

>>> with PeeweeJobQueue.db.connection_context():
...     PeeweeJobQueue.db.create_tables([PeeweeJobQueue])
...     test1 = PeeweeJobQueue.create(folder='t1')
...     test2 = PeeweeJobQueue.create(folder='t2')
>>> PeeweeJobQueue.next() == test1
True
>>> test1.is_started
False
>>> test1.is_started
False
>>> test1.finished = True
>>> with PeeweeJobQueue.db.connection_context():
...     _ = test1.save()
>>> test1.is_started
False
>>> test2 == PeeweeJobQueue.next()
True
>>> with PeeweeJobQueue.db.connection_context():
...     PeeweeJobQueue.db.drop_tables([PeeweeJobQueue])

"""

from __future__ import annotations

import abc
import contextlib
import datetime
import pathlib
import time
import typing
from typing import Any, Iterator, NamedTuple, Optional, Protocol, Type, TypeVar, Union

import huey
import huey.api
import huey.consumer
import huey.consumer_options
import np_config
import np_session
import peewee
from typing_extensions import Self

import np_queuey.utils as utils

from np_queuey.utils import (get_job, get_session)
from np_queuey.types import (Job, JobQueue, JobT, SessionArgs)

DB_PATH = pathlib.Path(utils.DEFAULT_HUEY_SQLITE_DB_PATH) 


#.with_name('test.db')

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

    def __getitem__(self, session_or_job: SessionArgs | Job) -> Job:
        """Get a job from the queue."""
        return self.get_job(session_or_job)
    
    def __delitem__(self, session_or_job: SessionArgs | Job) -> None:
        """Remove a job from the queue."""
        self.delete(session_or_job)
            
    def __contains__(self, session_or_job: SessionArgs | Job) -> bool:
        """Whether the session or job is in the queue."""
        return bool(self.get_job(session_or_job))
    
    def __len__(self) -> int:
        """Number of jobs in the queue."""
        with self.db.connection_context():
            return len(self.select())
        
    def __iter__(self) -> Iterator[Job]:
        """Iterate over the jobs in the queue.   
        Sorted by priority (desc), then date added (asc).
        """
        with self.db.connection_context():
            return iter(self.select())        

    @classmethod
    def parse_job(cls, job: Job) -> dict[str, Any]:
        """Parse a job into db column values."""
        return dict(
            folder=job.session,
            priority=job.priority,
            added=job.added,
            hostname=job.hostname,
            finished=job.finished,
            )
        
    @classmethod
    def add(cls, session_or_job: SessionArgs | Job, **kwargs) -> Self:
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
    def delete(cls, session_or_job: SessionArgs | Job) -> None:
        """Delete a session or job from the queue."""
        with cls.db.connection_context():
            instance = cls.get_job(session_or_job)
            instance.delete_instance()
        
    @classmethod
    def update(cls, session_or_job: Optional[SessionArgs | Self] = None, **job_kwargs) -> None:
        """Update the provided fields:values of an existing job."""
        instance = cls.self_or_job(session_or_job)
        if instance.is_started:
            raise ValueError(f'Cannot update started job: {instance}')
        with cls.db.connection_context():
            for key, value in job_kwargs.items():
                setattr(instance, key, value)
            instance.save()

    @classmethod
    def add_or_update(cls, session_or_job: SessionArgs | Job, **job_kwargs) -> None:
        """Add a session or job to the queue, or update if already present."""
        job = get_job(session_or_job)
        with cls.db.connection_context():
            instance = cls.get_or_none(folder=job.session)
        if instance is None:
            cls.add(job, **job_kwargs)
            return
        instance: Self
        instance.update(**job_kwargs)
        instance.set_queued()

    @classmethod
    def next(cls) -> Self | None:
        """Get the next job to process - by priority (desc), then date added (asc)."""
        with cls.db.connection_context():
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
                ((cls.hostname == None) | (cls.hostname == '') | (cls.hostname == 'force' + np_config.HOSTNAME))
                &
                ((cls.errored == None) | (cls.errored == ''))
            ).order_by(cls.priority.desc(), cls.added.asc())
        )
    
    @classmethod
    def get_job(cls, session_or_job: SessionArgs | JobT) -> Self:
        """Get the job instance from the queue that matches the input session."""
        if isinstance(session_or_job, cls):
            return session_or_job
        job = get_session(session_or_job)
        instance = cls.get_or_none(cls.folder == job.folder)
        if instance is None:
            raise ValueError(f'Job not found in queue: {job}')
        return instance
            
    def self_or_job(self, session_or_job: Optional[SessionArgs | JobT] = None) -> Self:
        if session_or_job is None:
            return self
        with self.db.connection_context():
            return self.get_job(session_or_job)
            
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

if __name__ == '__main__':
    import doctest
    doctest.testmod()