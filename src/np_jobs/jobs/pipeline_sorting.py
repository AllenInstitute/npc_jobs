import dataclasses

from np_jobs.jobs.base import JobDataclass

@dataclasses.dataclass
class SortingJob(JobDataclass):
    probes: str = 'ABCDEF'
    