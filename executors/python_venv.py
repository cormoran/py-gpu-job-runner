import threading
import os, subprocess, time
from executors.executor import Executor as Base


def repo_url_to_dir(repo_url):
    # git@github.com:user/foo.git
    # https://github.com/user/foo.git
    # ssh://git@host:port/user/foo.git
    repo_path = None
    for prefix in ['git@', 'ssh://git@', 'http://', 'https://']:
        if repo_url.startswith(prefix):
            repo_path = repo_url[len(prefix):]
            repo_path = repo_path.replace(':', '/').replace('..', '__')
    if repo_path is None:
        raise ValueError('unknown repo_url format: {}'.format(repo_url))
    return repo_path


locks_lock = threading.Lock()
locks = {}


class Executor(Base):
    def prepare(self):
        key = repo_url_to_dir(self.job.repo_url)
        with locks_lock:
            lock = locks[key] if key in locks else threading.Lock()
            locks[key] = lock
        self.kill_flg = False
        self.venv_dir = os.path.join(self.temp_dir_root, 'python_venv', repo_url_to_dir(self.job.repo_url))
        with lock:
            os.makedirs(self.venv_dir, exist_ok=True)
            subprocess.check_call(
                'python -m venv venv',
                cwd=self.venv_dir,
                shell=True,
                stdout=self.stdout,
                stderr=self.stderr,
            )
            subprocess.check_call(
                '. ./venv/bin/activate; pip install -r {}/src/requirements.txt'.format(self.temp_dir),
                cwd=self.venv_dir,
                shell=True,
                stdout=self.stdout,
                stderr=self.stderr,
            )
            os.makedirs(os.path.join(self.temp_dir, 'OUTPUT_SSHFS'))

    def execute(self):
        command = '. {}/venv/bin/activate;'.format(self.venv_dir) + self.job.command
        with subprocess.Popen(
                command,
                shell=True,
                cwd=os.path.join(self.temp_dir, 'src'),
                stdout=self.stdout,
                stderr=self.stderr,
                env={
                    **os.environ,
                    'CUDA_VISIBLE_DEVICES': self.job.gpu_ids,
                    'LOGDIR_ROOT': '/home/ukai/OUTPUT_SSHFS/transferclustering',
                },
                preexec_fn=os.setsid,
        ) as proc:
            while proc.poll() is None:
                time.sleep(10)
                if self.kill_flg:
                    os.killpg(os.getpgid(proc.pid), subprocess.signal.SIGINT)
                    time.sleep(20)
            proc.wait()
            if proc.returncode != 0:
                raise subprocess.CalledProcessError(proc.returncode, command)

    def cleanup(self):
        ...

    def kill(self):
        self.kill_flg = True