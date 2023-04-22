from __future__ import annotations

import pathlib
from typing import Any

import np_config

CONFIG: dict[str, Any] = np_config.fetch('/projects/np_queuey/config')

DEFAULT_HUEY_SQLITE_DB_PATH: str = CONFIG['shared_huey_sqlite_db_path']

DEFAULT_HUEY_DIR: pathlib.Path = pathlib.Path(
    DEFAULT_HUEY_SQLITE_DB_PATH
).parent
"""Directory for shared resources (tasks, sqlite dbs, huey instances...)"""
