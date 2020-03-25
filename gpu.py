#!/usr/bin/env python
import sys, os, json, fcntl, time
import GPUtil


class Lock():
    def __init__(self, filename):
        self.filename = filename

    def __enter__(self):
        self.lock = open(self.filename, 'a+')
        fcntl.flock(self.lock, fcntl.LOCK_EX)

    def __exit__(self, exec_type, exec_value, traceback):
        fcntl.flock(self.lock, fcntl.LOCK_UN)
        self.lock.close()


def try_get_available_gpu(
        candidate_gpu_ids,
        assign_interval_s,
        max_memory_used=0.001,
        lock_file='~/.gpu_wait.lock',
        history_file='~/.gpu_history.json',
        ngpu=None,
):
    '''
    if ngpu == None: get all avaiable GPUs
    elif isinstance(ngpu, int):
        try to get specified number of gpus
        if number of available gpus < ngpu: get no gpus
    '''
    history_file = os.path.expanduser(history_file)
    lock_file = os.path.expanduser(lock_file)
    os.makedirs(os.path.dirname(history_file), exist_ok=True)
    os.makedirs(os.path.dirname(lock_file), exist_ok=True)

    with Lock(lock_file):
        if os.path.exists(history_file):
            with open(history_file, 'r') as f:
                history = json.load(f)
        else:
            history = {}
        del_ids = set()
        for gpu_id, last_time in history.items():
            if time.time() - last_time > assign_interval_s:
                del_ids.add(gpu_id)
        for del_id in del_ids:
            del history[del_id]

        used_gpu_ids = set(map(int, history.keys()))

        available_gpu_ids = set(GPUtil.getAvailable(limit=1000, maxMemory=max_memory_used))
        if len(candidate_gpu_ids) > 0:
            available_gpu_ids = available_gpu_ids.intersection(candidate_gpu_ids)
        available_gpu_ids = available_gpu_ids - used_gpu_ids

        if ngpu is None or len(available_gpu_ids) >= ngpu:
            gpu_ids = sorted(available_gpu_ids)
            if ngpu is not None:
                gpu_ids = gpu_ids[:ngpu]
            for gpu_id in gpu_ids:
                history[gpu_id] = time.time()
            with open(history_file, 'w') as f:
                json.dump(history, f)
        else:
            gpu_ids = []
        return gpu_ids


def release_gpu(
        gpu_ids,
        lock_file='~/.gpu_wait.lock',
        history_file='~/.gpu_history.json',
):
    gpu_ids = list(map(str, gpu_ids))
    history_file = os.path.expanduser(history_file)
    lock_file = os.path.expanduser(lock_file)
    os.makedirs(os.path.dirname(history_file), exist_ok=True)
    os.makedirs(os.path.dirname(lock_file), exist_ok=True)

    with Lock(lock_file):
        if os.path.exists(history_file):
            with open(history_file, 'r') as f:
                history = json.load(f)
        else:
            history = {}
        for gpu_id in gpu_ids:
            if gpu_id in history:
                del history[gpu_id]
        with open(history_file, 'w') as f:
            json.dump(history, f)


def get_available_gpu(candidate_gpu_ids=set(),
                      assign_interval_s=60,
                      max_memory_used=0.001,
                      lock_file='~/.gpu_wait.lock',
                      history_file='~/.gpu_history.json',
                      sleep_interval_s=30,
                      wait=True,
                      ngpu=1):
    if len(GPUtil.getGPUs()) == 0:
        return []
    gpu_ids = []
    flg = False
    while len(gpu_ids) < ngpu and wait:
        if flg:
            time.sleep(sleep_interval_s)
        gpu_ids = try_get_available_gpu(candidate_gpu_ids, assign_interval_s, max_memory_used, lock_file, history_file, ngpu=ngpu)
        flg = True
    return gpu_ids


class GPUContext():
    def __init__(self, candidate_gpu_ids=set(), max_memory_used=0.001, ngpu=1):
        self.candidate_gpu_ids = candidate_gpu_ids
        self.gpu_ids = []
        self.max_memory_used = max_memory_used
        self.ngpu = ngpu

    def __enter__(self):
        self.gpu_ids = get_available_gpu(candidate_gpu_ids=self.candidate_gpu_ids,
                                         assign_interval_s=60 * 10,
                                         max_memory_used=self.max_memory_used,
                                         ngpu=self.ngpu)
        return self

    def __exit__(self, exc_type, exc_value, tb):
        release_gpu(self.gpu_ids)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser('空きGPU が出るまで待機するスクリプト')
    parser.add_argument('--gpus', type=str, default='', help='使いたいGPU番号一覧 ex) 0,1,2,3,4 空白ですべて')
    parser.add_argument('--history-file', default='~/.gpu_history.json')
    parser.add_argument('--lock-file', default='~/.gpu_wait.lock')
    parser.add_argument('--sleep-interval', type=int, default=10)
    parser.add_argument('--assign-interval', type=int, default=60, help='同じGPUを再割り当てするまでの間隔（プログラムのスタートアップ待ち時間）')
    parser.add_argument('--no-wait', action='store_true', default=False, help='')
    parser.add_argument('--max-memory-used', type=float, default=0.001, help='指定%以上メモリを使用している GPU は使わない')
    parser.add_argument('--n-gpu', type=int, default=1)

    args = parser.parse_args()

    sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

    candidate_gpu_ids = set(map(int, args.gpus.split(','))) if len(args.gpus) > 0 else set()

    gpu_ids = get_available_gpu(
        candidate_gpu_ids,
        args.assign_interval,
        args.max_memory_used,
        args.lock_file,
        args.history_file,
        args.sleep_interval,
        not args.no_wait,
        args.n_gpu,
    )
    print(','.join(list(map(str, gpu_ids))))
