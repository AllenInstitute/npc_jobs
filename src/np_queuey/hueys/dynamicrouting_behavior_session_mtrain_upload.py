from __future__ import annotations

import pathlib
import json
from typing_extensions import Literal

import huey as _huey
import np_config
import np_logging
import np_session
import np_tools

import np_queuey
from np_queuey.jobs import dynamicrouting_behavior_session_mtrain_upload as job

logger = np_logging.getLogger()

huey = np_queuey.HueyQueue(job.DB_PATH).huey

UPLOAD_JSON_DIR_CONFIG_KEY = (
    'dynamicrouting_behavior_session_mtrain_upload_json_dir'
)
"""Single leading fwd slash, for unix compatibility on hpc"""


@huey.periodic_task(_huey.crontab(minute='*/30', strict=True))
def upload_outstanding_sessions() -> None:
    sessions: list[
        tuple[str, str]
    ] = job.get_outstanding_behavior_sessions_for_processing()
    if not sessions:
        logger.info('No outstanding sessions to upload')
        return
    logger.info('Found %d outstanding sessions to upload', len(sessions))
    for foraging_id_and_filename in sessions:
        upload_session_on_hpc(foraging_id_and_filename)


@huey.task()
def upload_session_on_hpc(foraging_id_and_filename: tuple[str, str]) -> None:
    if is_behavior_session_in_mtrain(foraging_id_and_filename):
        logger.info(
            'Behavior session %r already in mtrain', foraging_id_and_filename
        )
        job.mark_behavior_session_as_uploaded(foraging_id_and_filename[0])
        return
    np_logging.web('np_queuey').info(
        'Starting behavior session upload to mtrain: %r',
        foraging_id_and_filename,
    )
    write_input_json(foraging_id_and_filename)
    write_shell_script(foraging_id_and_filename)
    with np_tools.ssh('hpc-login') as ssh:
        logger.debug(
            'Launching mtrain_lims on hpc for %s', foraging_id_and_filename
        )
        ssh.run(hpc_cmd(foraging_id_and_filename))
    verify_behavior_session_uploaded(foraging_id_and_filename)


@huey.task(retry_delay=20, retries=3)
def verify_behavior_session_uploaded(
    foraging_id_and_filename: tuple[str, str]
) -> None:
    _, output_json = input_output_jsons(foraging_id_and_filename)

    if not np_config.normalize_path(output_json).exists():
        msg = f'{output_json} does not exist'
        np_logging.web('np_queuey').warning(msg)
        raise FileNotFoundError(msg)

    if not is_behavior_session_in_mtrain(foraging_id_and_filename):
        msg = f'Could not find behavior session {foraging_id_and_filename[0]} in mtrain'
        np_logging.web('np_queuey').warning(msg)
        raise ValueError(msg)

    job.mark_behavior_session_as_uploaded(foraging_id_and_filename[0])
    np_logging.web('np_queuey').info(
        'Behavior session %r verified in mtrain', foraging_id_and_filename
    )


def is_behavior_session_in_mtrain(
    foraging_id_and_filename: tuple[str, str]
) -> bool:
    _, filename = foraging_id_and_filename
    _, mouse_id, date, time = job.parse_filename(filename)
    mtrain = np_session.Mouse(mouse_id).mtrain
    return foraging_id_and_filename[0].replace('-', '') in [
        _['id'].replace('-', '') for _ in mtrain.all_behavior_sessions
    ]


def write_input_json(foraging_id_and_filename: tuple[str, str]) -> None:
    input_json, _ = input_output_jsons(foraging_id_and_filename)
    logger.debug('Writing %s', input_json)
    path = pathlib.Path(np_config.normalize_path(input_json))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(get_input_json_contents(foraging_id_and_filename))
    )


def input_output_jsons(
    foraging_id_and_filename: tuple[str, str]
) -> tuple[str, str]:
    upload_json_dir = np_config.fetch('/projects/np_queuey/config')[
        UPLOAD_JSON_DIR_CONFIG_KEY
    ]
    return tuple(
        f'{upload_json_dir}/UPLOAD_TO_MTRAIN_{foraging_id_and_filename[0]}_{x}.json'
        for x in ('input', 'output')
    )


def hpc_cmd(foraging_id_and_filename) -> str:
    path = shell_script(foraging_id_and_filename).as_posix()
    return f"sbatch {path.replace('//', '/')}"


def get_behavior_session_storage_dir(foraging_id_and_filename) -> pathlib.Path:
    """
    * `storage_directory` isn't being populated in LIMS if upload job fails
    * will need to manually construct path

    >>> d = get_behavior_session_storage_dir(('3b70feba-8572-4cd8-884b-35ff62975d39', 'DynamicRouting1_366122_20230414_120213.hdf5'))
    >>> d.as_posix()
    '//allen/programs/braintv/production/neuralcoding/prod0/specimen_657428270/behavior_session_1264106353'
    """
    foraging_id, filename = foraging_id_and_filename
    _, mouse_id, date, time = job.parse_filename(filename)
    mouse = np_session.Mouse(mouse_id)
    if not mouse.lims:
        raise ValueError(f'Could not find mouse {mouse_id} in LIMS')
    if not mouse.lims.get('behavior_sessions'):
        raise ValueError(
            f'Could not find behavior sessions for mouse {mouse_id} in LIMS'
        )
    behavior_sessions = tuple(
        _
        for _ in mouse.lims['behavior_sessions']
        if _['foraging_id'].replace('-', '') == foraging_id.replace('-', '')
    )
    if not behavior_sessions:
        raise ValueError(
            f'Could not find behavior session for foraging_id {foraging_id} in LIMS'
        )
    return np_config.normalize_path(
        mouse.lims.path / f'behavior_session_{behavior_sessions[0]["id"]}'
    )


def get_input_json_contents(
    foraging_id_and_filename: tuple[str, str]
) -> dict[Literal['inc'], dict[str, str]]:
    """

    >>> r = get_input_json_contents(('3b70feba-8572-4cd8-884b-35ff62975d39', 'DynamicRouting1_366122_20230414_120213.hdf5'))
    >>> r['inc']['foraging_file_name']
    '/allen/programs/braintv/production/neuralcoding/prod0/specimen_657428270/behavior_session_1264106353/DynamicRouting1_366122_20230414_120213.hdf5'
    """
    foraging_id, filename = foraging_id_and_filename
    try:
        storage_directory = get_behavior_session_storage_dir(
            foraging_id_and_filename
        )
    except ValueError as exc:
        np_logging.web('np_queuey').exception(exc)
        raise exc
    else:
        file = storage_directory / filename
        if not file.exists():
            msg = (
                f'Preparing for mtrain upload but file does not exist: {file}'
            )
            np_logging.web('np_queuey').error(msg)
            raise FileNotFoundError(msg)
        return {
            'inc': {
                'API_BASE': 'http://mtrain:5000',
                'foraging_id': foraging_id,
                'foraging_file_name': f'{file.as_posix()[1:] if file.as_posix().startswith("//") else file.as_posix()}',  # TODO remove leading slash,
            }
        }


def shell_script(foraging_id_and_filename: tuple[str, str]) -> pathlib.Path:
    return pathlib.Path(
        f'//allen/scratch/aibstemp/svc_neuropix/mtrain_upload/mtrain_upload_{foraging_id_and_filename[0]}.sh'
    )


def write_shell_script(foraging_id_and_filename: tuple[str, str]) -> None:
    path = shell_script(foraging_id_and_filename)
    logger.debug('Writing %s', path.as_posix())
    with path.open('w', newline='\n') as f:
        f.write(get_shell_script_contents(foraging_id_and_filename))


def get_shell_script_contents(
    foraging_id_and_filename: tuple[str, str]
) -> str:
    foraging_id, filename = foraging_id_and_filename
    input_json, output_json = input_output_jsons(foraging_id_and_filename)
    return f"""#!/bin/bash
#SBATCH --job-name=npexp_to_incoming                        # Job name
#SBATCH --mail-type=FAIL                                    # Mail events (NONE, BEGIN, END, FAIL, ALL)
#SBATCH --mail-user=ben.hardcaslt@alleninstitute.org        # Where to send mail  
#SBATCH --ntasks=1                                          # Run on a single CPU
#SBATCH --mem=1gb                                           # Job memory request (per node)
#SBATCH --time=00:10:00                                     # Time limit hrs:min:sec
#SBATCH --output=mtrain_upload_{foraging_id}.log            # Standard output and error log
#SBATCH --partition braintv                                 # Partition used for processing
#SBATCH --tmp=100M                                          # Request the amount of space your jobs needs on /scratch/fast

pwd; hostname; date

echo 'Running mtrain upload job on a single thread'

source activate /allen/aibs/technology/conda/production/mtrain_upload
mtrain_lims --input_json {input_json} --output_json {output_json}

date
"""


if __name__ == '__main__':
    import doctest

    doctest.testmod(verbose=False)
