# GPU Job Runner

```
git clone https://github.com/cormoran/py-gpu-job-runner.git
cd py-gpu-job-runner
python -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt

# runner (on GPU server)
python runner.py --help

# client (enqueue jobs)
python push.py ++help
```
