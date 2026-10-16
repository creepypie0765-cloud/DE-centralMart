"""Optional live integration check: run with Airflow started on localhost:8082."""
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from orchestration import airflow_request, DAG_ID, trigger_run
from urllib.parse import quote

run = trigger_run(10000)
run_id = run['dag_run_id']
print(f'Triggered {run_id}', flush=True)
path = '/dags/' + DAG_ID + '/dagRuns/' + quote(run_id, safe='')
deadline = time.monotonic() + 180
while time.monotonic() < deadline:
    current = airflow_request(path)
    if current['state'] in ('success', 'failed'):
        tasks = airflow_request(path + '/taskInstances?limit=100')['task_instances']
        for task in tasks:
            print(task['task_id'], task['state'], task.get('duration'), flush=True)
        if current['state'] != 'success' or len(tasks) != 11 or any(t['state'] != 'success' for t in tasks):
            raise SystemExit('Airflow integration check failed')
        print('All 11 Airflow tasks succeeded.', flush=True)
        break
    time.sleep(2)
else:
    raise SystemExit('Timed out waiting for the Airflow demo run')
