from np_jobs.queues.sqlite_isilon.base import SqliteIsilonJobQueue
from np_jobs.jobs import SortingJob

class PipelineSortingQueue(SqliteIsilonJobQueue):
    
    table_name = 'sorting'
    column_definitions = dict(
        **SqliteIsilonJobQueue.column_definitions,
        probes='TEXT NOT NULL DEFAULT ABCDEF',
    )  
    job_type = SortingJob