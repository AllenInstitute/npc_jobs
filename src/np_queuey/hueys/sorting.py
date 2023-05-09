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
import np_logging
import np_session
import np_tools
from typing_extensions import Literal

from np_queuey.utils import get_job, get_session, update_status
from np_queuey.queues.pipeline_sorting_queue import (
    PipelineSortingQueue, SortingJob
)
from np_queuey.queues.pipeline_qc_queue import PipelineQCQueue
from np_queuey.types import SessionArgs, JobT, Job

logger = np_logging.getLogger()

huey = _huey.SqliteHuey('sorting.db', immediate=True)

Q = PipelineSortingQueue()

@huey.task()
def sort_outstanding_sessions() -> None:
    job: SortingJob | None = Q.next()
    if job is None:
        logger.info('No outstanding sessions to sort')
        return
    if Q.is_started(job):
        logger.info('Sorting already started for %s', job.session)
        return
    run_sorting(job)
    add_job_to_pipeline_qc_queue(job)

def run_sorting(session_or_job: SortingJob | SessionArgs) -> None:
    job = get_job(session_or_job, SortingJob)
    np_logging.web('np_queuey').info('Starting sorting %s probes %s', job.session, job.probes)
    with update_status(Q, job):
        remove_existing_sorted_folders_on_npexp(job)
        start_sorting(job)
        move_sorted_folders_to_npexp(job)
        remove_raw_data_on_acq_drives(job)
    np_logging.web('np_queuey').info('Sorting finished for %s', job.session)

def probe_folders(session_or_job: SortingJob | SessionArgs) -> tuple[str]:
    job = get_job(session_or_job, SortingJob)
    return tuple(f'{job.session}_probe{probe_letter.upper()}_sorted' for probe_letter in job.probes)

def remove_existing_sorted_folders_on_npexp(session_or_job: SortingJob | SessionArgs) -> None:
    job = get_job(session_or_job, SortingJob)
    for probe_folder in probe_folders(job):
        logger.info('Checking for existing sorted folder %s', probe_folder)
        path = np_session.Session(job.session).npexp_path / probe_folder
        if path.exists():
            logger.info('Removing existing sorted folder %s', probe_folder)
            shutil.rmtree(path.as_posix(), ignore_errors=True)

def start_sorting(session_or_job: SortingJob | SessionArgs) -> None:
    job = get_job(session_or_job, SortingJob)
    path = pathlib.Path('c:/Users/svc_neuropix/Documents/GitHub/ecephys_spike_sorting/ecephys_spike_sorting/scripts/full_probe3X_from_extraction_nopipenv.bat')
    if not path.exists():
        raise FileNotFoundError(path)
    args = [job.session, ''.join(_ for _ in str(job.probes))]
    subprocess.run([str(path), *args])
 
def move_sorted_folders_to_npexp(session_or_job: SortingJob | SessionArgs) -> None:
    """Move the sorted folders to the npexp drive
    Assumes D: processing drive - might want to move this to rig for
    specific-config.
    Cannot robocopy with * for folders, so we must do each probe separately.
    """
    job = get_job(session_or_job, SortingJob)
    for probe_folder in probe_folders(job):
        src = pathlib.Path(f'D:/{probe_folder}')
        dest = np_session.Session(job.session).npexp_path / probe_folder
        logger.info(f'Moving {src} to {dest}')
        subprocess.run([
            'robocopy', f'{src}', f'{dest}',
             '/MOVE', '/E', '/COPYALL', '/R:0', '/W:0', '/MT:32'
             ], check=False) # return code from robocopy doesn't signal failure      
        if src.exists():
            np_tools.move(src, dest, ignore_errors=True)
            
def remove_raw_data_on_acq_drives(session_or_job: SortingJob | SessionArgs) -> None:
    session = get_session(session_or_job)
    for drive in ('A:', 'B:'):
        for path in pathlib.Path(drive).glob(f'{session}*'):
            npexp_path = session.npexp_path / path.name
            lims_path = session.lims_path / path.name if hasattr(session, 'lims_path') else None
            if not npexp_path.exists() and (lims_path is None or not lims_path.exists()):
                logger.info('Copying %r to npexp', path)
                np_tools.copy(path, npexp_path)
            logger.info('Removing %r', path)
            shutil.rmtree(path, ignore_errors=True)

def add_job_to_pipeline_qc_queue(session_or_job: Job | SessionArgs) -> None:
    PipelineQCQueue().add_or_update(session_or_job)

def main() -> NoReturn:
    """Run synchronous task loop."""
    while True:
        sort_outstanding_sessions()
        time.sleep(300)
                
if __name__ == '__main__':
    import doctest
    doctest.testmod(verbose=False)
    main()
