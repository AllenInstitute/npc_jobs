from np_jobs.queues.sqlite_isilon.base import SqliteIsilonJobQueue
   
class CodeOceanUploadQueue(SqliteIsilonJobQueue):
    
    table_name = 'codeocean_upload'