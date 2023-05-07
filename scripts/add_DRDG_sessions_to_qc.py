from pathlib import Path
from typing import Generator, Tuple

import np_session
import np_tools
from typing_extensions import Literal

from np_queuey.hueys.qc import PipelineQCQueue

Q = PipelineQCQueue()

def get_sessions() -> Generator[Path, None, None]:
    for session in np_session.sessions('DRDG'):
        if not isinstance(session, np_session.PipelineSession):
            print('skipeed', session)
            continue
        yield session

def add_sessions():
    for session in get_sessions():
        Q.add_or_update(session)

if __name__ == "__main__":
    add_sessions()