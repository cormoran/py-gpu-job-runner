# GPU Job Runner

## Requirements

- python3
- mysql

## Install

```
git clone https://github.com/cormoran/py-gpu-job-runner.git
cd py-gpu-job-runner
python -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt
```

## Usage

```
# runner (on GPU server)
python runner.py --help
# example: Run on 4 GPU server
python runner.py --host MYSQL_HOST --user MYSQL_USER --password MYSQL_PASSWORD --database DATABASE --gpus 0,1,2,3

# client (enqueue jobs)
python push.py ++help
# example:
python push.py  ++command echo Hello world

# fail watcher
python fail-watcher.py --help
```

## Note

- ジョブの停止等は DataGrip などの mysql クライアントを使ってデータを直接修正する

## Bugs

- runner.py は実行中のプログラムのログを表示するはずだが高い確率でうまく行かない
