"""
Functions for adding behavior session IDs to the Dynamic Routing Task database,
marking them for upload to mtrain, and checking which sessions have yet to be
uploaded.

- code must be compatible with camstim27 env
- file must be accessible on //allen

Usage from camstim27 env (note: this tests a copy of this file on the network):
>>> import imp
>>> mtrain_uploader = imp.load_source('mtrain_uploader', '//allen/programs/mindscope/workgroups/dynamicrouting/DynamicRoutingTask/dynamicrouting_behavior_session_mtrain_upload.py')
>>> mtrain_uploader.add_behavior_session_to_mtrain_upload_queue('test', 'test.hdf5')
>>> mtrain_uploader.mark_behavior_session_as_processing('test')
"""
import contextlib
import sqlite3

DB_PATH = '//allen/programs/mindscope/workgroups/dynamicrouting/DynamicRoutingTask/.tasks.db'
SESSION_UPLOAD_TABLE = 'behavior_session_mtrain_upload_queue'
COLUMNS_TO_DEFINITIONS = {
    'foraging_id': 'TEXT PRIMARY KEY NOT NULL',
    'filename': 'TEXT NOT NULL',
    'added': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL',  # YYYY-MM-DD HH:MM:SS
    'processing': 'INTEGER',  # [None] 0 or 1
    'uploaded': 'INTEGER',  # [None] 0 or 1
}


def table_sql(column_name_to_definition_mapping):
    """
    >>> table_sql({'col1': 'TEXT PRIMARY KEY NOT NULL', 'col2': 'INTEGER'})
    '(col1 TEXT PRIMARY KEY NOT NULL, col2 INTEGER)'
    """
    return (
        '('
        + ', '.join(
            [
                '{} {}'.format(col, defn)
                for col, defn in column_name_to_definition_mapping.items()
            ]
        )
        + ')'
    )


def initialize_mtrain_upload_queue_in_db():
    """
    >>> initialize_mtrain_upload_queue_in_db()
    """
    with task_db_cursor() as c:
        c.execute(
            'CREATE TABLE IF NOT EXISTS behavior_session_mtrain_upload_queue '
            + table_sql(COLUMNS_TO_DEFINITIONS)
        )


def parse_filename(filename):
    """
    >>> (task, mouse_id, date, time) = parse_filename('DynamicRouting1_664566_20230328_151155.hdf5')
    >>> '_'.join((task, mouse_id, date, time))
    'DynamicRouting1_664566_20230328_151155'
    """
    return filename.split('.')[0].split('_')


def add_behavior_session_to_mtrain_upload_queue(foraging_id, filename):
    """
    >>> add_behavior_session_to_mtrain_upload_queue('test', 'test.hdf5')
    >>> with task_db_cursor() as c:
    ...   sessions = c.execute('SELECT * FROM behavior_session_mtrain_upload_queue').fetchall()
    ...   assert 'test' in (s[0] for s in sessions), 'Test foraging_id = test not added to db'
    >>> add_behavior_session_to_mtrain_upload_queue('test', 'test.hdf5') # accidental repeat should not raise
    """
    initialize_mtrain_upload_queue_in_db()
    with task_db_cursor() as c:
        c.execute(
            'INSERT OR IGNORE INTO behavior_session_mtrain_upload_queue (foraging_id, filename) VALUES (?, ?)',
            (
                foraging_id,
                filename,
            ),
        )


def dynamic_routing_task_db():
    """
    >>> conn = dynamic_routing_task_db()
    >>> _ = conn.cursor()
    """
    conn = sqlite3.connect(DB_PATH, timeout=1)
    conn.isolation_level = None  # autocommit mode
    conn.execute('pragma journal_mode="delete"')
    conn.execute('pragma synchronous=2')
    return conn


@contextlib.contextmanager
def task_db_cursor():
    """
    >>> with task_db_cursor() as c:
    ...    _ = c.execute('SELECT 1').fetchall()
    """
    conn = dynamic_routing_task_db()
    cursor = conn.cursor()
    try:
        cursor.execute('begin exclusive')
        yield cursor
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        cursor.close()


def remove_behavior_session_from_mtrain_upload_queue(foraging_id):
    """
    >>> add_behavior_session_to_mtrain_upload_queue('test', 'test.hdf5')
    >>> remove_behavior_session_from_mtrain_upload_queue('test')
    >>> with task_db_cursor() as c:
    ...   sessions = c.execute('SELECT * FROM behavior_session_mtrain_upload_queue').fetchall()
    ...   assert 'test' not in (s[0] for s in sessions), 'Test foraging_id = "test" not removed from db'
    >>> remove_behavior_session_from_mtrain_upload_queue('test') # accidental repeat should not raise
    """
    initialize_mtrain_upload_queue_in_db()
    with task_db_cursor() as c:
        c.execute(
            'DELETE FROM behavior_session_mtrain_upload_queue WHERE foraging_id = ?',
            (foraging_id,),
        )


def get_outstanding_behavior_sessions_for_processing():
    """
    Returns tuple[(foraging_id, filename), ...] for sessions that have not been processed or
    uploaded.

    >>> remove_behavior_session_from_mtrain_upload_queue('test')
    >>> add_behavior_session_to_mtrain_upload_queue('test', 'test.hdf5')
    >>> sessions = get_outstanding_behavior_sessions_for_processing()
    >>> assert ('test', 'test.hdf5') in sessions, 'Test foraging_id = "test" not returned: sessions = %s' % str(sessions)
    """
    with task_db_cursor() as c:
        sessions = c.execute(
            'SELECT foraging_id, filename FROM behavior_session_mtrain_upload_queue WHERE (processing = 0 OR processing IS NULL) AND (uploaded = 0 OR uploaded IS NULL)'
        ).fetchall()
    return sessions


def mark_behavior_session_as_processing(foraging_id):
    """
    Sets processing = 1.

    >>> remove_behavior_session_from_mtrain_upload_queue('test')
    >>> add_behavior_session_to_mtrain_upload_queue('test', 'test.hdf5')
    >>> mark_behavior_session_as_processing('test')
    >>> with task_db_cursor() as c:
    ...   result = c.execute('SELECT processing FROM behavior_session_mtrain_upload_queue WHERE foraging_id = "test"').fetchall()[0][0]
    ...   assert result == 1, 'Test result (processing, ) returned {}: expected 1 (True)'.format(result)
    """
    with task_db_cursor() as c:
        c.execute(
            'UPDATE behavior_session_mtrain_upload_queue SET processing = 1 WHERE foraging_id = ?',
            (foraging_id,),
        )


def mark_behavior_session_as_uploaded(foraging_id):
    """
    Sets processing to 0 and uploaded to 1.

    >>> remove_behavior_session_from_mtrain_upload_queue('test')
    >>> add_behavior_session_to_mtrain_upload_queue('test', 'test.hdf5')
    >>> mark_behavior_session_as_uploaded('test')
    >>> with task_db_cursor() as c:
    ...   result = c.execute('SELECT processing, uploaded FROM behavior_session_mtrain_upload_queue WHERE foraging_id = "test"').fetchall()[0]
    ...   assert result == (0, 1), 'Test result (processing, uploaded) returned {}: expected (0, 1) (False, True)'.format(result)
    """
    with task_db_cursor() as c:
        c.execute(
            'UPDATE behavior_session_mtrain_upload_queue SET processing = 0, uploaded = 1 WHERE foraging_id = ?',
            (foraging_id,),
        )


if __name__ == '__main__':
    import doctest

    doctest.testmod(verbose=False, raise_on_error=False)
