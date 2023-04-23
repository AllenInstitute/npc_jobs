from __future__ import annotations

import contextlib
import json
import pathlib
import random
import shutil
import subprocess
import time
from typing import Generator, NoReturn

import huey as _huey
# import np_config
import np_logging
import np_session
import np_tools
from typing_extensions import Literal

import np_queuey
import np_queuey.utils as utils
from np_queuey.jobs.sorting import Sorting

logger = np_logging.getLogger()

huey = _huey.SqliteHuey('sorting.db', immediate=True)


@huey.task()
def sort_outstanding_sessions() -> None:
    with Sorting.db:
        job: Sorting | None = Sorting.next()
        if job is None:
            logger.info('No outstanding sessions to sort')
            return
        if job.is_started:
            logger.info('Sorting already started for %s', job.session)
            return
    run_sorting(job)


@contextlib.contextmanager
def set_flags(job: Sorting) -> Generator[None, None, None]:
    try:
        np_logging.web('np_queuey').info('Starting sorting %s probes %s', job.session, job.probes)
        job.set_started()
        yield
    except BaseException as exc:
        if isinstance(exc, Exception):
            job.set_errored(exc)
            np_logging.web('np_queuey').exception('Exception during sorting %s', job.session)
            return
        else: # KeyboardInterrupt, SystemExit etc:
            job.set_queued()
            raise
    else:
        job.set_finished()
        np_logging.web('np_queuey').info('Sorting completed successfully for %s', job.session)

def run_sorting(job: Sorting) -> None:
    with set_flags(job):
        remove_existing_sorted_folders_on_npexp(job)
        start_sorting(job)
        move_sorted_folders_to_npexp(job)
        remove_raw_data_on_acq_drives(job)

def probe_folders(job: Sorting) -> tuple[str]:
    return tuple(f'{job.session.folder}_probe{probe_letter.upper()}_sorted' for probe_letter in job.probes)

def remove_existing_sorted_folders_on_npexp(job: Sorting) -> None:
    for probe_folder in probe_folders(job):
        logger.info('Checking for existing sorted folder %s', probe_folder)
        path = job.session.npexp_path / probe_folder
        if path.exists():
            logger.info('Removing existing sorted folder %s', probe_folder)
            shutil.rmtree(path.as_posix(), ignore_errors=True)

def start_sorting(job: Sorting) -> None:
    path = pathlib.Path('c:/Users/svc_neuropix/Documents/GitHub/ecephys_spike_sorting/ecephys_spike_sorting/scripts/full_probe3X_from_extraction_nopipenv.bat')
    if not path.exists():
        raise FileNotFoundError(path)
    args = [job.session.folder, ''.join(_ for _ in str(job.probes))]
    subprocess.run([str(path), *args])
 
def move_sorted_folders_to_npexp(job: Sorting) -> None:
    """Move the sorted folders to the npexp drive
    Assumes D: processing drive - might want to move this to rig for
    specific-config.
    Cannot robocopy with * for folders, so we must do each probe separately.
    """
    for probe_folder in probe_folders(job):
        logger.info('Moving D:/%s to npexp', probe_folder)
        subprocess.run(['robocopy', f'D:/{probe_folder}', str(job.session.npexp_path / probe_folder), '/MOVE', '/E' '/R:0', '/W:0', '/MT:32'])       
        

def remove_raw_data_on_acq_drives(job: Sorting) -> None:
    for drive in ('A:', 'B:'):
        for path in pathlib.Path(drive).glob(f'{job.session.folder}*'):
            logger.info('Removing %r', path)
            shutil.rmtree(path, ignore_errors=True)
        
def main() -> NoReturn:
    """Run synchronous task loop."""
    while True:
        sort_outstanding_sessions()
        time.sleep(300)
        
        
if __name__ == '__main__':
    import doctest
    doctest.testmod(verbose=False)
    main()
