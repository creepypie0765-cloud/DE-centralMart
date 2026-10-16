"""Run inside the Airflow container: python tests/check_airflow.py."""
from airflow.models import DagBag

bag = DagBag('/opt/project/dags', include_examples=False)
assert not bag.import_errors, bag.import_errors
dag = bag.dags['central_mart_retail']
stages = ['create_workspace', 'create_reference_data', 'generate_orders',
          'ingest_csv_to_duckdb', 'validate_ingested_data', 'convert_to_parquet',
          'join_filter_and_enrich', 'aggregate_sales',
          'benchmark_formats_and_threads', 'export_reports', 'validate_final_reports']
assert set(dag.task_ids) == set(stages)
for parent, child in zip(stages, stages[1:]):
    assert dag.get_task(parent).downstream_task_ids == {child}
assert dag.catchup is False
assert dag.max_active_runs == 1
print('Airflow DAG imports and dependencies passed.')
