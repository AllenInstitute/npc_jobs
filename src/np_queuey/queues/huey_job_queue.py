from __future__ import annotations

import abc
import logging
import pathlib
from typing import Callable, Optional

import huey
import huey.api
import huey.consumer
import huey.consumer_options
import peewee

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

    huey: huey.SqliteHuey
    """`huey` object for submitting tasks"""

    db_path: str
    """Path to the sqlite database for `huey`"""

    def __init__(self, sqlite_db_path: Optional[str] = None, **kwargs) -> None:
        self.db_path = sqlite_db_path or utils.DEFAULT_HUEY_SQLITE_DB_PATH
        kwargs.setdefault('journal_mode', 'truncate')
        kwargs.setdefault('fsync', True)
        self.huey = huey.SqliteHuey(filename=self.db_path, **kwargs)
        for task in (
            _ for _ in dir(tasks) if isinstance(getattr(tasks, _), Callable)
        ):
            self.add_task(task)

    def add_task(self, task: str) -> None:
        setattr(self, task, self.huey.task()(getattr(tasks, task)))

    def submit(self, task: str, *args, **kwargs) -> huey.api.Result:
        """Send `task(*args, **kwargs)` to queue.

        The signature of `task` should be identical when submitted and when
        processed - preferably the function lives in the `tasks` module.
        """
        return getattr(self, task)(*args, **kwargs)

    @property
    def consumer_cmd(self) -> list[str]:
        return [
            'huey_consumer.py',
            f'{__name__}.{__class__.__name__} {self.db_path}',
        ]

    def process(self, *options: str) -> None:
        """Starts a `huey_consumer` in a subprocess on the current machine.

        `options` strings are added to the `huey_consumer.py` call.
        """
        # consumer_cmd = self.consumer_cmd.extend(options) if options else self.consumer_cmd
        # subprocess.run(consumer_cmd, check=True)

        # Set up logging for the "huey" namespace.
        logging.getLogger('huey')

        parser_handler = huey.consumer_options.OptionParserHandler()
        parser = parser_handler.get_option_parser()
        options, args = parser.parse_args(list(options))
        options = {k: v for k, v in options.__dict__.items() if v is not None}
        config = huey.consumer_options.ConsumerConfig(**options)
        config.validate()
        consumer: huey.consumer.Consumer = self.huey.create_consumer(
            **config.values
        )
        consumer.run()

    def process_parallel(self) -> None:
        """Starts a `huey_consumer` with multiple processes on the current machine."""
        self.process('-k process -w 4')
