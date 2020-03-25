import time, datetime, json, os
import requests
import pymysql
from db import JobRepository
from model import Job, JobStatus


def send_to_slack(url, job: Job):
    data = {
        'text':
        'ジョブが失敗しました :ghost:',
        'attachments': [
            {
                "title": 'host',
                "text": job.host,
            },
            {
                "title": 'command',
                "text": '```\n{}\n```'.format(job.command),
                "mrkdwn_in": ["text"],
            },
            {
                "title": 'error',
                "text": '```\n{}\n```'.format(job.message),
                "mrkdwn_in": ["text"],
                "color": 'danger',
            },
        ],
    }
    requests.post(url, data=json.dumps(data))


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--user', default='jobmanager')
    parser.add_argument('--password', default='jobmanager')
    parser.add_argument('--database', default='jobmanage_py')
    parser.add_argument('--slack-api-url', default=os.environ.get('SLACK_WEBHOOK_URL'))
    args = parser.parse_args()

    db = connection = pymysql.connect(
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.database,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )
    repo = JobRepository(db)
    last_time = datetime.datetime.now(tz=repo.tz).isoformat()

    while True:
        now = datetime.datetime.now(tz=repo.tz).isoformat()
        failed_jobs = repo.get_failed_jobs_since(last_time)
        for job in failed_jobs:
            send_to_slack(args.slack_api_url, job)
        last_time = now
        time.sleep(30)
