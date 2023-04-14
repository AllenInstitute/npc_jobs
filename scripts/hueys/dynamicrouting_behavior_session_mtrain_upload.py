from __future__ import annotations

import contextlib
import sqlite3

import huey as _huey
import np_logging
import np_config
import np_tools

import np_queuey
from np_queuey.jobs import dynamicrouting_behavior_session_mtrain_upload as job

logger = np_logging.get_logger(__name__)

huey = np_queuey.HueyQueue(job.DB_PATH).huey

@huey.periodic_task(_huey.crontab(hour='*/1'))
def upload_outstanding_sessions() -> None:
    sessions: tuple[int] = job.get_outstanding_behavior_session_ids_for_processing()
    if not sessions:
        logger.debug('No outstanding sessions to upload')
        return
    logger.info('Found %d outstanding sessions to upload', len(sessions))
    upload_session_on_hpc.map(sessions)
    logger.debug('Queued upload tasks for sessions: %r', sessions)

@huey.task()
def upload_session_on_hpc(behavior_session_id: int) -> None:
    np_logging.web('np_queuey').info('Uploading behavior session %d to mtrain', behavior_session_id)
    
if __name__ == '__main__':
    import doctest
    doctest.testmod(verbose=True)