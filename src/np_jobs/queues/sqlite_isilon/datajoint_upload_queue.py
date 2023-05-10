from np_jobs.queues.sqlite_isilon.base import SqliteIsilonJobQueue
   
class DataJointUploadQueue(SqliteIsilonJobQueue):
    
    table_name = 'datajoint_upload'