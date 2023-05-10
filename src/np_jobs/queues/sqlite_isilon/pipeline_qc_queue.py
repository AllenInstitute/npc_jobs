from np_jobs.queues.sqlite_isilon.base import SqliteIsilonJobQueue
   
class PipelineQCQueue(SqliteIsilonJobQueue):
    
    table_name = 'qc'