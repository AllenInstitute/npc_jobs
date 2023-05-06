from np_queuey.queues.sqlite_isilon_queue import SqliteJobQueue
from np_queuey.utils import JobDataclass
   
class PipelineQCQueue(SqliteJobQueue):
    
    table_name = 'qc'