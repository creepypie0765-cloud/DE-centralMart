"""Small loopback-only bridge from the retail dashboard to local Airflow."""
from datetime import datetime, timezone
import json
from urllib.parse import quote
from urllib.request import Request, build_opener, ProxyHandler
from urllib.error import HTTPError
import time
import os

AIRFLOW_URL = os.environ.get('CENTRAL_MART_AIRFLOW_URL', 'http://127.0.0.1:8082')
_token = None
_token_time = 0
DAG_ID = 'central_mart_retail'
STAGES = [
    ('create_workspace', 'Create workspace', 'Run folders and configuration', 'run.json'),
    ('create_reference_data', 'Create reference data', 'Four stores and six products', 'stores.csv · products.csv'),
    ('generate_orders', 'Generate orders', 'Synthetic retail order export', 'sales.csv'),
    ('ingest_csv_to_duckdb', 'Ingest CSV', 'Load three sources into raw warehouse tables', 'warehouse.duckdb'),
    ('validate_ingested_data', 'Check input quality', 'Counts, unique IDs, reference and field checks', 'Quality checks in task log'),
    ('convert_to_parquet', 'Convert to Parquet', 'Zstandard compression and columnar storage', 'sales.parquet'),
    ('join_filter_and_enrich', 'Join, filter & enrich', 'Catalog joins, discounts and completed orders', 'enriched_sales · completed_sales'),
    ('aggregate_sales', 'Aggregate sales', 'Store, month, product and status metrics', 'Four report tables'),
    ('benchmark_formats_and_threads', 'Benchmark queries', 'CSV / Parquet and CPU worker comparison', 'benchmark.json · query_plan.txt'),
    ('export_reports', 'Export reports', 'CSV exports, business summary and manifest', 'results/ · manifest.json'),
    ('validate_final_reports', 'Reconcile results', 'Independent raw order revenue validation', 'validated.json'),
]


def airflow_request(path, method='GET', payload=None):
    global _token, _token_time
    opener = build_opener(ProxyHandler({}))
    if not _token or time.monotonic() - _token_time > 300:
        with opener.open(AIRFLOW_URL + '/auth/token', timeout=5) as response:
            _token = json.load(response)['access_token']
            _token_time = time.monotonic()
    request = Request(AIRFLOW_URL + '/api/v2' + path, method=method,
                      data=json.dumps(payload).encode() if payload is not None else None,
                      headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + _token})
    try:
        with opener.open(request, timeout=5) as response:
            return json.load(response)
    except HTTPError as exc:
        if exc.code == 401:
            _token = None
        raise


def airflow_status():
    base = '/dags/' + DAG_ID
    stages = [dict(id=s[0], name=s[1], description=s[2], artifact=s[3], state='not_run') for s in STAGES]
    try:
        dag = airflow_request(base)
        runs = airflow_request(base + '/dagRuns?limit=8&order_by=-start_date')['dag_runs']
        latest = runs[0] if runs else None
        if latest:
            tasks = airflow_request(base + '/dagRuns/' + quote(latest['dag_run_id'], safe='') + '/taskInstances?limit=100')
            by_id = {t['task_id']: t for t in tasks['task_instances']}
            for stage in stages:
                task = by_id.get(stage['id'], {})
                stage.update(state=task.get('state') or 'pending',
                             duration=task.get('duration'), attempts=task.get('try_number', 0),
                             start_date=task.get('start_date'), end_date=task.get('end_date'))
        return dict(connected=True, url=AIRFLOW_URL, dag_id=DAG_ID, paused=dag['is_paused'],
                    latest=latest, runs=runs, stages=stages)
    except (OSError, ValueError, KeyError) as exc:
        return dict(connected=False, url=AIRFLOW_URL, dag_id=DAG_ID, stages=stages,
                    runs=[], latest=None, error=f'Airflow unavailable: {exc}')


def trigger_run(rows):
    base = '/dags/' + DAG_ID
    airflow_request(base, 'PATCH', {'is_paused': False})
    now = datetime.now(timezone.utc).isoformat()
    return airflow_request(base + '/dagRuns', 'POST', {
        'dag_run_id': 'dashboard__' + now, 'logical_date': now,
        'conf': {'rows': rows, 'threads': 4, 'repeats': 3},
    })
