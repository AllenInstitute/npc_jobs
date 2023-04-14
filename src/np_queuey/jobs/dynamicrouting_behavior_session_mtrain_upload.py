"""
Functions for adding behavior session IDs to the Dynamic Routing Task database,
marking them for upload to mtrain, and checking which sessions have yet to be
uploaded.

- code must be compatible with camstim27 env
- file must be accessible on //allen

Usage from camstim27 env:

>>> import imp
>>> mtrain_uploader = imp.load_source('mtrain_uploader', '//allen/programs/mindscope/workgroups/dynamicrouting/DynamicRoutingTask/dynamicrouting_behavior_session_mtrain_upload.py')
>>> mtrain_uploader.add_behavior_session_to_mtrain_upload_queue(-1)
>>> mtrain_uploader.mark_behavior_session_as_processing(-1)
"""
import contextlib
import sqlite3


DB_PATH = '//allen/programs/mindscope/workgroups/dynamicrouting/DynamicRoutingTask/.tasks.db'
SESSION_UPLOAD_TABLE = 'behavior_session_mtrain_upload_queue'
COLUMNS = (
    'behavior_session_id',  # int
    'added',  # str YYYY-MM-DD HH:MM:SS
    'processing',  # int [None] 0 or 1
    'uploaded',  # int [None] 0 or 1
)


def add_behavior_session_to_mtrain_upload_queue(behavior_session_id):
    """
    >>> add_behavior_session_to_mtrain_upload_queue(-1)
    >>> with task_db_cursor() as c:
    ...   sessions = c.execute('SELECT * FROM behavior_session_mtrain_upload_queue').fetchall()
    ...   assert -1 in (s[0] for s in sessions), 'Test behavior_session_id = -1 not added to db'
    >>> add_behavior_session_to_mtrain_upload_queue(-1) # accidental repeat should not raise
    """
    initialize_mtrain_upload_queue_in_db()
    with task_db_cursor() as c:
        c.execute(
            'INSERT OR IGNORE INTO behavior_session_mtrain_upload_queue (behavior_session_id) VALUES (?)',
            (behavior_session_id,),
        )


def dynamic_routing_task_db():
    """
    >>> conn = dynamic_routing_task_db()
    >>> _ = conn.cursor()
    """
    conn = sqlite3.connect(DB_PATH, timeout=1)
    conn.isolation_level = None  # autocommit mode
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


def initialize_mtrain_upload_queue_in_db():
    """
    >>> initialize_mtrain_upload_queue_in_db()
    """
    with task_db_cursor() as c:
        c.execute(
            'CREATE TABLE IF NOT EXISTS behavior_session_mtrain_upload_queue ('
            'behavior_session_id INTEGER PRIMARY KEY NOT NULL, '
            'added TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, '
            'processing INTEGER, '
            'uploaded INTEGER)'
        )


def remove_behavior_session_from_mtrain_upload_queue(behavior_session_id):
    """
    >>> remove_behavior_session_from_mtrain_upload_queue(-1)
    >>> with task_db_cursor() as c:
    ...   sessions = c.execute('SELECT * FROM behavior_session_mtrain_upload_queue').fetchall()
    ...   assert -1 not in (s[0] for s in sessions), 'Test behavior_session_id = -1 not removed from db'
    >>> remove_behavior_session_from_mtrain_upload_queue(-1) # accidental repeat should not raise
    """
    initialize_mtrain_upload_queue_in_db()
    with task_db_cursor() as c:
        c.execute(
            'DELETE FROM behavior_session_mtrain_upload_queue WHERE behavior_session_id = ?',
            (behavior_session_id,),
        )


def get_outstanding_behavior_session_ids_for_processing():
    """
    Returns tuple[int, ...] of session IDs that have not been processed or
    uploaded.

    >>> remove_behavior_session_from_mtrain_upload_queue(-1)
    >>> add_behavior_session_to_mtrain_upload_queue(-1)
    >>> sessions = get_outstanding_behavior_session_ids_for_processing()
    >>> assert -1 in sessions, 'Test behavior_session_id = -1 not returned: sessions = %s' % str(sessions)
    """
    with task_db_cursor() as c:
        sessions = c.execute(
            'SELECT behavior_session_id FROM behavior_session_mtrain_upload_queue WHERE (processing = 0 OR processing IS NULL) AND (uploaded = 0 OR uploaded IS NULL)'
        ).fetchall()
    return tuple(int(s[0]) for s in sessions)


def mark_behavior_session_as_processing(behavior_session_id):
    """
    Sets processing = 1.

    >>> remove_behavior_session_from_mtrain_upload_queue(-1)
    >>> add_behavior_session_to_mtrain_upload_queue(-1)
    >>> mark_behavior_session_as_processing(-1)
    >>> with task_db_cursor() as c:
    ...   result = c.execute('SELECT processing FROM behavior_session_mtrain_upload_queue WHERE behavior_session_id = -1').fetchall()[0][0]
    ...   assert result == 1, f'Test result (processing, ) returned {result}: expected 1 (True)'
    """
    add_behavior_session_to_mtrain_upload_queue(
        behavior_session_id
    )   # only adds if previously removed
    with task_db_cursor() as c:
        c.execute(
            'UPDATE behavior_session_mtrain_upload_queue SET processing = 1 WHERE behavior_session_id = ?',
            (behavior_session_id,),
        )


def mark_behavior_session_as_uploaded(behavior_session_id):
    """
    Sets processing to 0 and uploaded to 1.

    >>> remove_behavior_session_from_mtrain_upload_queue(-1)
    >>> add_behavior_session_to_mtrain_upload_queue(-1)
    >>> mark_behavior_session_as_uploaded(-1)
    >>> with task_db_cursor() as c:
    ...   result = c.execute('SELECT processing, uploaded FROM behavior_session_mtrain_upload_queue WHERE behavior_session_id = -1').fetchall()[0]
    ...   assert result == (0, 1), f'Test result (processing, uploaded) returned {result}: expected (0, 1) (False, True)'
    """
    add_behavior_session_to_mtrain_upload_queue(
        behavior_session_id
    )   # only adds if previously removed
    with task_db_cursor() as c:
        c.execute(
            'UPDATE behavior_session_mtrain_upload_queue SET processing = 0, uploaded = 1 WHERE behavior_session_id = ?',
            (behavior_session_id,),
        )


if __name__ == '__main__':
    import doctest

    doctest.testmod(verbose=True)
