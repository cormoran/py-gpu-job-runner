import threading, importlib, os, uuid, shutil, signal, typing, signal, queue, socket, time
import traceback
import pymysql

from db import JobRepository, RunnerRepository
from model import Job, JobStatus, Runner, RunnerStatus
from executors.executor import Executor
import gitrepo
import util
import gpu
from display import Display


def load_executor(executor):
    if executor is None or len(executor) == 0:
        executor = 'python_venv'
    return importlib.import_module('executors.' + executor).Executor


class WrapExecutor(threading.Thread):
    ''' Execute `job` with `job.executor` '''
    def __init__(self, job_repo: JobRepository, job: Job, finish_que: queue.Queue, temp_dir_root: str, repo_cache_dir: str, trash_dir_root: str):
        super().__init__()
        self.job_repo = job_repo
        self.job = job
        self.temp_dir_root = temp_dir_root
        self.trash_dir_root = trash_dir_root
        self.repo_cache_dir = repo_cache_dir
        self.executor: Executor = None
        self.stdout_path = None
        self.stderr_path = None
        self.finish_que = finish_que
        self.result: str = None
        self.should_resume = False
        self.finished = False

    def render(self) -> str:
        if self.stderr_path is None or self.stdout_path is None or self.finished:
            return ''
        with open(self.stderr_path, 'r') as stderr, open(self.stdout_path, 'r') as stdout:
            result = '[Standard Error]\n' + stderr.read()
            result += '\n\n[Standard Out]\n' + stdout.read()
        return result

    def run(self):
        temp_dir = os.path.join(self.temp_dir_root, str(uuid.uuid4()))
        os.makedirs(temp_dir)
        self.stdout_path = os.path.join(temp_dir, 'stdout.txt')
        self.stderr_path = os.path.join(temp_dir, 'stderr.txt')
        result = None
        execute_error = None
        other_error = None
        with open(self.stdout_path, 'w') as stdout, open(self.stderr_path, 'w') as stderr:
            try:
                self.job_repo.update(self.job.id, run_id=os.path.basename(temp_dir))
                self.executor = load_executor(self.job.executor)(self.job, temp_dir, self.temp_dir_root, stdout, stderr)
                gitrepo.clone_git_repository(self.job.repo_url, self.job.commit_hash, os.path.join(temp_dir, 'src'), self.repo_cache_dir)
                self.executor.prepare()
                try:
                    self.executor.execute()
                except Exception as e:
                    execute_error = e
                finally:
                    self.executor.cleanup()
            except Exception as e:
                other_error = e
            finally:
                if execute_error or other_error:
                    with open(self.stderr_path, 'r') as f:
                        result = '[stderr]\n'
                        result += f.read()
                    if execute_error is not None:
                        result += '\n\n[execute error message]\n' + str(execute_error)
                    if other_error is not None:
                        result += '\n\n[other error message]\n' + str(other_error)
                self.finished = True
                os.makedirs(self.trash_dir_root, exist_ok=True)
                shutil.move(temp_dir, self.trash_dir_root)
        self.result = result
        self.finish_que.put(self.job.id)

    def kill(self, resume=False):
        self.should_resume = resume
        if self.executor:
            self.executor.kill()


class ExecutorManager():
    def __init__(
            self,
            display: Display,
            db: pymysql.Connection,
            available_gpu_ids: typing.List[int],
            temp_dir_root: str,
            repo_cache_dir: str,
            trash_dir_root: str,
            max_parallel: int,
            labels: typing.List[str],
            name: str = socket.gethostname(),
    ):
        self.display = display
        self.db = db
        self.repo = JobRepository(self.db)
        self.runner_repo = RunnerRepository(self.db)
        self.active_executors: typing.Dict[int, WrapExecutor] = {}  # Job.id ->
        self.finished_executors_queue = queue.Queue()
        self.available_gpu_ids = set(available_gpu_ids)
        self.temp_dir_root = temp_dir_root
        self.trash_dir_root = trash_dir_root
        self.repo_cache_dir = repo_cache_dir
        self.max_parallel = max_parallel
        self.name = name
        self.labels = labels
        self.runner = Runner(
            name=self.name,
            gpu_ids=','.join(list(map(str, self.available_gpu_ids))),
            labels=','.join(labels),
            status=RunnerStatus.Running,
        )
        self.finish_flg = False
        self.finished_jobs = []
        self.display.render_toppage = self._render

    def run(self):
        self.runner = self.runner_repo.create(self.runner)
        while not self.finish_flg or len(self.active_executors) > 0:
            try:
                self._loop()
            except KeyboardInterrupt:
                self.finish_flg = True
        self.runner_repo.remove(self.runner.id)

    def _loop(self):
        with util.DelayedKeyboardInterrupt():
            sleep_time = 10
            self._handle_finished_jobs()
            self._check_active_job_status()
            self._sync_runner_status()
            if self.finish_flg or self.runner.status == RunnerStatus.Stop.value:
                self._kill_executors()
                sleep_time = 10
            else:
                job = self._get_next_job()
                if job is not None:
                    sleep_time = 1
                    self._start_job(job)
            self.display.update_toppage()
        for i in range(int(sleep_time / 0.1)):
            self.display.render()
            time.sleep(0.01)

    def _kill_executors(self):
        for executor in self.active_executors.values():
            executor.kill(resume=True)

    def _get_next_job(self) -> typing.Optional[Job]:
        if len(self.active_executors) >= self.max_parallel or self.runner.status != RunnerStatus.Running.value:
            return None
        # acquire all free GPUs and release no-needs after get next job
        available_gpu_ids = gpu.try_get_available_gpu(self.available_gpu_ids, 60 * 60 * 24 * 10)
        required_gpu_ids = []
        try:
            job = self.repo.pop_next_job(max_gpu_available=len(available_gpu_ids), labels=self.labels)
            if job is not None:
                required_gpu_ids = available_gpu_ids[:job.num_gpu]
                job = job._replace(gpu_ids=','.join(list(map(str, required_gpu_ids))), host=self.name)
                self.repo.update(**job._asdict())
        finally:
            no_need_gpu_ids = set(available_gpu_ids) - set(required_gpu_ids)
            gpu.release_gpu(list(no_need_gpu_ids))
        return job

    def _start_job(self, job: Job):
        executor = WrapExecutor(self.repo, job, self.finished_executors_queue, self.temp_dir_root, self.repo_cache_dir, self.trash_dir_root)
        executor.start()
        refresh_func, window_id = self.display.add_window(executor.render)
        executor._window_id = window_id
        executor._window_refresh = refresh_func
        self.active_executors[job.id] = executor

    def _handle_finished_jobs(self):
        for _ in range(100):
            try:
                finished_id = self.finished_executors_queue.get_nowait()
            except queue.Empty:
                break
            executor = self.active_executors[finished_id]
            if len(executor.job.gpu_ids):
                gpu_ids = list(map(int, executor.job.gpu_ids.split(',')))
                gpu.release_gpu(gpu_ids)
            self.display.delete_page(id=executor._window_id)
            del self.active_executors[finished_id]
            job = self.repo.get(executor.job.id)
            if executor.result is None:  # success
                job = job._replace(status=JobStatus.Finish, message='')
            else:  # fail
                if executor.should_resume:
                    job = job._replace(status=JobStatus.Queue, message=executor.result)
                else:
                    job = job._replace(status=JobStatus.Fail, message=executor.result)
            self.repo.update(**job._asdict())
            self.finished_jobs.append(job)
            if len(self.finished_jobs) > 30:
                self.finished_jobs = self.finished_jobs[len(self.finished_jobs) - 30:]

    def _check_active_job_status(self):
        for id, executor in self.active_executors.items():
            executor.job = self.repo.update_timestamp(id)
            if executor.job.status != JobStatus.Running.value:
                executor.kill(resume=False)
            executor._window_refresh()

    def _sync_runner_status(self):
        self.runner = self.runner_repo.update_timestamp(self.runner.id)
        if len(self.runner.gpu_ids) > 0:
            try:
                available_gpu_ids = set(list(map(int, self.runner.gpu_ids.split(','))))
                self.available_gpu_ids = available_gpu_ids
            except Exception:
                ...
        else:
            self.available_gpu_ids = set()
        self.labels = self.runner.labels.split(',')

    def _render(self):
        def format_job(job: Job):
            return '* {} {}'.format(job.status, job.command)

        if self.finish_flg:
            status = 'KeyboardInterrupt detected. Killing all {} executors. Please wait.'.format(len(self.active_executors))
        else:
            status = '{} executors are running.'.format(len(self.active_executors))
        labels = 'lables: ' + ', '.join(self.labels)
        gpus = 'GPUs: ' + ', '.join(list(map(str, list(self.available_gpu_ids))))
        running_jobs = '\n\n'.join(list(map(format_job, map(lambda executor: executor.job, self.active_executors.values()))))
        finished_jobs = '\n\n'.join(list(map(format_job, self.finished_jobs)))
        return '''

:::GPU Job Runner:::

{}
{}
{}

[Running Jobs]

{}


[Finished Jobs]

{}

'''.format(status, labels, gpus, running_jobs, finished_jobs)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--user', default='jobmanager')
    parser.add_argument('--password', default='jobmanager')
    parser.add_argument('--database', default='jobmanage_py')
    parser.add_argument('--gpus', type=str, default=None)
    parser.add_argument('--max-gpu-memory-used', type=float, default=0.001)
    parser.add_argument('--temp-dir-root', type=str, default='~/.py-job-runner/tmp')
    parser.add_argument('--trash-dir-root', type=str, default='~/Trash')
    parser.add_argument('--repo-cache-dir', type=str, default='~/.py-job-runner/repo')
    parser.add_argument('--max-parallel', type=int, default=10)
    parser.add_argument('--labels', type=str, nargs='+', default=[])
    args = parser.parse_args()

    args.temp_dir_root = os.path.expanduser(args.temp_dir_root)
    args.repo_cache_dir = os.path.expanduser(args.repo_cache_dir)
    args.trash_dir_root = os.path.expanduser(args.trash_dir_root)

    db = connection = pymysql.connect(
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.database,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

    available_gpu_ids = ''
    if args.gpus:
        available_gpu_ids = list(map(int, args.gpus.split(',')))
    with Display() as display:
        ExecutorManager(
            display,
            db,
            available_gpu_ids,
            args.temp_dir_root,
            args.repo_cache_dir,
            args.trash_dir_root,
            args.max_parallel,
            args.labels,
        ).run()
