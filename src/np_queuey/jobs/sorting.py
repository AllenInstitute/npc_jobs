"""
Pipeline sorting queue.

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
from typing import Any, NamedTuple, Optional, Protocol, Type, TypeVar, Union

from typing_extensions import Self

import np_config
import np_session
import peewee

import np_queuey.utils as utils
from np_queuey.types import *
from np_queuey.queues import PeeweeJobQueue
   
class Sorting(PeeweeJobQueue):
    
    probes = peewee.TextField(
        null=False, 
        default='ABCDEF',
        constraints=[peewee.SQL('DEFAULT ABCDEF'),],
        )
    """Probe letters for sorting, e.g. `ABCDEF`"""

    def update_probes(self, probes: str, session_or_job: Optional[SessionArgs | Self] = None) -> None:
        """Update the probes to sort."""
        self.update(session_or_job, probes=probes)
        
with Sorting.db.connection_context():
    Sorting.db.create_tables([Sorting])

if __name__ == '__main__':
    with Sorting.db.connection_context():
         Sorting.db.create_tables([Sorting])
    s = '123456789_366122_20230422'
    _ = Sorting.delete().where(Sorting.folder == s).execute()
    test = Sorting.add(s, priority=99)
    Sorting.next() == test
    import doctest
    doctest.testmod()
