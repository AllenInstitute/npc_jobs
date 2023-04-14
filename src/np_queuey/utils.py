try:
    import np_config
except:
    DEFAULT_HUEY_SQLITE_DB_PATH = '/tmp/np_queuey_huey.db'
else:
    DEFAULT_HUEY_SQLITE_DB_PATH: str = np_config.fetch(
        '/projects/np_queuey/config'
    )['shared_huey_sqlite_db_path']
