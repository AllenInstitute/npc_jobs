from __future__ import annotations

import abc
import os
import subprocess
from typing import Any, Callable, Optional

import huey
import np_config
import np_logging

import np_queuey.tasks as tasks
import np_queuey.utils as utils


class JobQueue(abc.ABC):

    @abc.abstractmethod
    def submit(self, *args, **kwargs) -> None:
        """Submit a task to the job queue in open-loop, with any required args."""
    
    @abc.abstractmethod
    def process(self, *args, **kwargs) -> None:
        """Process one task at a time from the job queue."""
 
    @abc.abstractmethod
    def process_parallel(self, *args, **kwargs) -> None:
        """Process multiple tasks from the job queue in parallel."""


class HueyQueue(JobQueue):

    huey: huey.SqliteStorage
    """`huey` object for submitting tasks"""

    def __init__(self, sqlite_db_path: Optional[str] = None):
        self.huey = huey.SqliteStorage(sqlite_db_path or utils.DEFAULT_HUEY_SQLITE_DB_PATH)

    def submit(self, task: Callable, *args, **kwargs) -> None:
        """Send `task(*args, **kwargs)` to queue in open-loop.
        
        The signature of `task` should be identical when submitted and when
        processed - preferably the function lives in the `tasks` module.
        """
        return self.huey.task()(task)(*args, **kwargs)

    @property
    def consumer_cmd(self) -> list[str]:
        return ['huey_consumer.py', f'{__package__}.{__name__}.{__class__} {self.huey.filename!r}']

    def process(self, *options: str) -> None:
        """Starts a `huey_consumer` in a subprocess on the current machine.
        
        `options` strings are added to the `huey_consumer.py` call.
        """
        subprocess.run(self.consumer_cmd.extend(*options))
    
    def process_parallel(self) -> None:
        """Starts a `huey_consumer` with multiple processes on the current machine."""
        self.process('-k process -w 4')