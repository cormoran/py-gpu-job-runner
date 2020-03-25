import pymysql
from db import JobRepository
from model import Job, JobStatus

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(prefix_chars='+')
    parser.add_argument('++command', type=str, nargs='+', required=True)
    parser.add_argument('++repo-url', type=str, required=True)
    parser.add_argument('++commit-hash', type=str, required=True)
    parser.add_argument('++priority', type=int, default=5)
    parser.add_argument('++labels', type=str, nargs='+')
    parser.add_argument('++num-gpu', type=int, default=1)
    parser.add_argument('++host', default='localhost')
    parser.add_argument('++user', default='jobmanager')
    parser.add_argument('++password', default='jobmanager')
    parser.add_argument('++database', default='jobmanage_py')
    parser.add_argument('+n', '++no-push', action='store_true')
    args = parser.parse_args()

    if args.no_push:
        print(' '.join(args.command))
        exit(0)

    db = connection = pymysql.connect(
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.database,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )
    repo = JobRepository(db)
    res = repo.create(
        Job(
            repo_url=args.repo_url,
            commit_hash=args.commit_hash,
            status=JobStatus.Queue,
            command=' '.join(args.command),
            required_labels=','.join(args.labels) if args.labels else '',
            priority=args.priority,
            executor='python_venv',
            num_gpu=args.num_gpu,
        ))
    print(res)