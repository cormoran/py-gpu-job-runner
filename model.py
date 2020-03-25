from typing import NamedTuple, Optional
from enum import Enum


class JobStatus(Enum):
    Queue = 'QUEUE'
    Running = 'Running'
    Finish = 'Finish'
    Fail = 'Fail'
    Cancel = 'Cancel'
    Stop = 'Stop'

    def translate(self, escape_table):
        return self.value

    def __repr__(self):
        return self.value


class Job(NamedTuple):
    repo_url: str = ''
    commit_hash: str = ''
    status: JobStatus = JobStatus.Queue
    command: str = ''
    message: str = ''
    priority: int = 10
    num_gpu: int = 1
    required_labels: str = ''
    executor: str = ''
    #
    gpu_ids: str = ''
    host: str = ''
    run_id: str = ''
    #
    id: int = None
    created_at: str = None
    updated_at: str = None


class RunnerStatus(Enum):
    Running = 'Running'
    Stop = 'Stop'

    def translate(self, escape_table):
        return self.value

    def __repr__(self):
        return self.value


class Runner(NamedTuple):
    name: str = ''
    gpu_ids: str = ''
    labels: str = ''
    status: RunnerStatus = RunnerStatus.Running
    #
    id: int = None
    created_at: str = None
    updated_at: str = None
