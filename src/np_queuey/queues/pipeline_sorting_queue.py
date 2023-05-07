import dataclasses

from np_queuey.queues.sqlite_isilon_queue import SqliteJobQueue
from np_queuey.utils import JobDataclass

@dataclasses.dataclass
class SortingJob(JobDataclass):
    probes: str = 'ABCDEF'
    
class PipelineSortingQueue(SqliteJobQueue):
    
    table_name = 'sorting'
    column_definitions = dict(
        **SqliteJobQueue.column_definitions,
        probes='TEXT NOT NULL DEFAULT ABCDEF',
    )  
    job_type = SortingJob