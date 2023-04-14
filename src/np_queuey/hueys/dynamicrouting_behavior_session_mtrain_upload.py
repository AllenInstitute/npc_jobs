from __future__ import annotations

import contextlib
import sqlite3

import huey as _huey
import np_config
import np_logging
import np_session
import np_tools

import np_queuey
from np_queuey.jobs import dynamicrouting_behavior_session_mtrain_upload as job

logger = np_logging.getLogger()

huey = np_queuey.HueyQueue(job.DB_PATH).huey

@huey.periodic_task(_huey.crontab(minute='*/1', strict=True))
def upload_outstanding_sessions() -> None:
    sessions: list[tuple[str, str]] = job.get_outstanding_behavior_sessions_for_processing()
    if not sessions:
        logger.info('No outstanding sessions to upload')
        return
    logger.info('Found %d outstanding sessions to upload', len(sessions))
    upload_session_on_hpc.map(sorted(sessions))
    logger.debug('Queued upload tasks for sessions: %r', sessions)

@huey.task()
def upload_session_on_hpc(foraging_id_and_filename: tuple[str, str]) -> None:
    np_logging.web('np_queuey').info('Uploading behavior session %r to mtrain', foraging_id_and_filename)
    
    # todo 
    # write input json
    # write shell script
    # launch on hpc
    
def get_input_json_contents(foraging_id_and_filename: tuple[str, str]) -> dict[str, dict[str, str]]:
    """
    >>> r = get_input_json_contents(('c0721a32377945be947b3bb57ee869f8', 'test_366122_00000_0000.hdf5'))
    >>> r['inc']['foraging_file_name']
    '/allen/programs/braintv/production/neuralcoding/prod0/specimen_657428270/behavior_session_1040290172/test_366122_00000_0000.hdf5'
    """
    foraging_id, filename = foraging_id_and_filename
    task, mouse_id, date, time = job.parse_filename(filename)
    mouse = np_session.Mouse(mouse_id)
    behavior_session = next((_ for _ in mouse.lims['behavior_sessions'] if _['foraging_id'].replace('-', '') == foraging_id.replace('-', '')), None)
    if not behavior_session:
        np_logging.web('np_queuey').error('Could not find behavior session for foraging_id %r in LIMS', foraging_id)
        raise ValueError(f'Could not find behavior session for foraging_id {foraging_id} in LIMS')
    return {
        "inc": {
            "API_BASE": "http://mtrain:5000",
            "foraging_id": foraging_id,
            "foraging_file_name": f"{behavior_session['storage_directory']}{'/' if not behavior_session['storage_directory'].endswith('/') else ''}{filename}",
        }
    }

if __name__ == '__main__':
    import doctest
    doctest.testmod(verbose=True)