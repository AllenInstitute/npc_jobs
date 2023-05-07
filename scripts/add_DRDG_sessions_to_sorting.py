from pathlib import Path
from typing import Generator, Tuple

import np_session
import np_tools
from typing_extensions import Literal

import np_queuey
import np_queuey.utils as utils
from np_queuey.jobs.sorting import Sorting, SessionArgs

def get_sessions_with_missing_metrics() -> Generator[Path, None, None]:
    for session in np_session.sessions('DRDG'):
        if not isinstance(session, np_session.PipelineSession):
            print('skipeed', session)
            continue
        if len(session.metrics_csv) < len(session.probes_inserted):
            yield session
            
def sorted_paths() -> Generator[Path, None, None]:
    for session in np_session.sessions('DR'):
        if not isinstance(session, np_session.PipelineSession):
            print('skipeed', session)
            continue
        for csv in session.metrics_csv:
            yield csv.parent
            
def paths_to_ctimes(parent: Path) -> Tuple[float, ...]:
    return tuple(p.stat().st_ctime for p in parent.iterdir())

def range_hours(parent: Path) -> float:
    ctimes = paths_to_ctimes(parent)
    return (max(ctimes) - min(ctimes)) / 3600


def add_sessions_with_sorted_data_mismatch():

    for p in sorted_paths():
        if range_hours(p) > 7:
            session = p.parent.parent.parent.name
            probe = p.parent.parent.name.split('probe')[-1][0]
            print(f'{range_hours(p):.1f} h  {session} probe{probe}')   
            with Sorting.db:
                job = Sorting.get_or_none(folder=session)
            if job is None:
                Sorting.add(session, probes=probe)
            else:
                job.update_probes(job.probes + probe)

if __name__ == "__main__":
    add_sessions_with_sorted_data_mismatch()