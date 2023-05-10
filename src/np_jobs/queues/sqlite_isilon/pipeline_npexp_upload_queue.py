from np_jobs.queues.sqlite_isilon.base import SqliteIsilonJobQueue
   
class PipelineNpexpUploadQueue(SqliteIsilonJobQueue):
    
    table_name = 'npexp_upload'