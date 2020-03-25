from typing import Optional, Sequence
import datetime, threading

from pymysql.connections import Connection
from model import Job, JobStatus, Runner, RunnerStatus

db_lock = threading.Lock()


class JobRepository():
    def __init__(self, db: Connection, tz=datetime.timezone(datetime.timedelta(hours=9), 'JST')):
        self.db = db
        self.tz = tz
        self.create_table()

    def create_table(self):
        with db_lock, self.db.cursor() as cur:
            # yapf: disable
            cur.execute(
                'CREATE TABLE IF NOT EXISTS jobs ('+
                '   id int NOT NULL AUTO_INCREMENT,'+
                '   repo_url varchar(1024),'+
                '   commit_hash varchar(255),'+
                '   status varchar(16),'+
                '   command LONGTEXT,'+
                '   message LONGTEXT,'+
                '   priority int,'+
                '   num_gpu int,'+
                '   required_labels varchar(255),'+
                '   executor varchar(255),'+
                '   gpu_ids varchar(255),'+
                '   host varchar(255),'+
                '   run_id varchar(255),'+
                '   created_at varchar(64),'+
                '   updated_at varchar(64),'+
                '   PRIMARY KEY (id))'
            )
            # yapf: enable

    def create(self, job: Job):
        with db_lock:
            job_dict = job._asdict()
            job_dict['created_at'] = datetime.datetime.now(tz=self.tz).isoformat()
            job_dict['updated_at'] = datetime.datetime.now(tz=self.tz).isoformat()
            del job_dict['id']
            sql = 'INSERT INTO jobs(' + ', '.join(list(job_dict.keys())) + ') VALUES (' + ', '.join(['%s'] * len(job_dict.keys())) + ')'
            with self.db.cursor() as cur:
                cur.execute(sql, list(job_dict.values()))
                cur.execute('SELECT * from jobs WHERE id = LAST_INSERT_ID() LIMIT 1')
                result = cur.fetchone()
            return Job(**result)

    def update(self, id: int, **kwargs):
        with db_lock:
            self._update(id, **kwargs)
        return self.get(id)

    def update_timestamp(self, id: int):
        with db_lock:
            self._update(id, updated_at=datetime.datetime.now(tz=self.tz).isoformat())
        return self.get(id)

    def _update(self, id: int, **kwargs):
        default_job = Job()
        job = dict()
        for key, value in kwargs.items():
            if hasattr(default_job, key) and type(value) == type(getattr(default_job, key)):
                job[key] = value
        job['updated_at'] = datetime.datetime.now(tz=self.tz).isoformat()
        sql = 'UPDATE jobs set ' + ', '.join([key + '= %s' for key in job.keys()]) + ' WHERE id = %s'
        with self.db.cursor() as cur:
            cur.execute(sql, list(job.values()) + [id])

    def get(self, id: int) -> Optional[Job]:
        with db_lock:
            with self.db.cursor() as cur:
                cur.execute('SELECT * from jobs WHERE id = %s LIMIT 1', id)
                row = cur.fetchone()
            return Job(**row)

    def pop_next_job(self, max_gpu_available: int, labels: Sequence[str] = []):
        with db_lock:
            labels = set(labels)
            found = False
            self.db.begin()
            try:
                with self.db.cursor() as cur:
                    sql = 'SELECT * FROM jobs WHERE status = %s ORDER BY priority DESC, num_gpu DESC limit 1'
                    cur.execute(sql, (JobStatus.Queue.value))
                    row = cur.fetchone()
                    if row:
                        job = Job(**row)
                    if row is None or job.num_gpu > max_gpu_available:
                        rows = []
                    else:
                        sql = 'SELECT * FROM jobs WHERE status = %s AND num_gpu <= %s ORDER BY priority DESC, created_at ASC FOR UPDATE'
                        cur.execute(sql, (JobStatus.Queue.value, max_gpu_available))
                        rows = cur.fetchall()
                for row in rows:
                    job = Job(**row)
                    required_labels = set(job.required_labels.split(',') if len(job.required_labels) > 0 else [])
                    if required_labels.intersection(labels) != required_labels:
                        continue
                    self._update(job.id, status=JobStatus.Running)
                    found = True
                    break
                self.db.commit()
            except (Exception, KeyboardInterrupt) as e:
                self.db.rollback()
                raise e
        return job if found else None

    def get_failed_jobs_since(self, since):
        with db_lock:
            with self.db.cursor() as cur:
                cur.execute('SELECT * FROM jobs WHERE status = %s AND updated_at > %s', (JobStatus.Fail.value, since))
                rows = cur.fetchall()
        return [Job(**row) for row in rows]


class RunnerRepository():
    def __init__(self, db: Connection, tz=datetime.timezone(datetime.timedelta(hours=9), 'JST')):
        self.db = db
        self.tz = tz
        self.create_table()

    def create_table(self):
        with db_lock, self.db.cursor() as cur:
            # yapf: disable
            cur.execute(
                'CREATE TABLE IF NOT EXISTS runners ('+
                '   id int NOT NULL AUTO_INCREMENT,'+
                '   name varchar(255),'+
                '   gpu_ids varchar(255),'+
                '   labels varchar(255),'+
                '   status varchar(16),'+
                '   created_at varchar(64),'+
                '   updated_at varchar(64),'+
                '   PRIMARY KEY (id))'
            )
            # yapf: enable

    def create(self, runner: Runner):
        with db_lock:
            runner_dict = runner._asdict()
            runner_dict['created_at'] = datetime.datetime.now(tz=self.tz).isoformat()
            runner_dict['updated_at'] = datetime.datetime.now(tz=self.tz).isoformat()
            del runner_dict['id']
            sql = 'INSERT INTO runners (' + ', '.join(list(runner_dict.keys())) + ') VALUES (' + ', '.join(['%s'] * len(runner_dict.keys())) + ')'
            with self.db.cursor() as cur:
                cur.execute(sql, list(runner_dict.values()))
                cur.execute('SELECT * from runners WHERE id = LAST_INSERT_ID() LIMIT 1')
                result = cur.fetchone()
        return Runner(**result)

    def update(self, id: int, **kwargs):
        with db_lock:
            self._update(id, **kwargs)
        return self.get(id)

    def update_timestamp(self, id: int):
        with db_lock:
            self._update(id, updated_at=datetime.datetime.now(tz=self.tz).isoformat())
        return self.get(id)

    def _update(self, id: int, **kwargs):
        default_runner = Runner()
        runner = dict()
        for key, value in kwargs.items():
            if hasattr(default_runner, key) and type(value) == type(getattr(default_runner, key)):
                runner[key] = value
        runner['updated_at'] = datetime.datetime.now(tz=self.tz).isoformat()
        sql = 'UPDATE runners set ' + ', '.join([key + '= %s' for key in runner.keys()]) + ' WHERE id = %s'
        with self.db.cursor() as cur:
            cur.execute(sql, list(runner.values()) + [id])

    def get(self, id: int) -> Optional[Runner]:
        with db_lock:
            with self.db.cursor() as cur:
                cur.execute('SELECT * from runners WHERE id = %s LIMIT 1', id)
                row = cur.fetchone()
        return Runner(**row)

    def remove(self, id: int):
        with db_lock:
            with self.db.cursor() as cur:
                cur.execute('DELETE from runners WHERE id = %s', id)
