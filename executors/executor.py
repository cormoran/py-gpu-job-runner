from typing import TextIO
from model import Job


class Executor():
    def __init__(
            self,
            job: Job,
            temp_dir: str,
            temp_dir_root: str,
            stdout: TextIO,
            stderr: TextIO,
    ):
        self.job = job
        self.temp_dir = temp_dir
        self.temp_dir_root = temp_dir_root
        self.stdout = stdout
        self.stderr = stderr

    def prepare(self):
        ...

    def execute(self):
        raise NotImplementedError()

    def cleanup(self):
        ...

    def kill(self):
        raise NotImplementedError()
