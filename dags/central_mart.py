"""Every retail batch stage is visible and independently retryable in Airflow."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path

from airflow.sdk import DAG, Param, get_current_context, task


def parameters():
    return get_current_context()['params']


with DAG(
    dag_id='central_mart_retail',
    description='Create → generate → ingest → check → convert → enrich → aggregate → benchmark → export → validate',
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule='0 6 * * *', catchup=False, max_active_runs=1,
    default_args={'retries': 2, 'retry_delay': timedelta(seconds=30),
                  'execution_timeout': timedelta(minutes=15)},
    params={
        'rows': Param(100000, type='integer', minimum=1, maximum=10000000),
        'threads': Param(4, type='integer', minimum=1, maximum=16),
        'repeats': Param(3, type='integer', minimum=1, maximum=10),
    },
    tags=['duckdb', 'retail', 'demo'],
    doc_md='''## Central Mart: full batch lifecycle
Every node performs an actual stage and saves durable artifacts. Open **Graph**
to follow creation, generation, ingestion, quality checks, conversion, enrichment,
aggregation, benchmarks, exports and independent validation. Each node has its
own logs, duration and retries. Input CSV is synthetic, not a live external feed.

Runs daily at **06:00 UTC / 11:30 India time** when unpaused. Trigger manually to
choose order count, threads and repeats. Each run has an isolated directory under
`airflow-output/runs/`; the final log prints its path and manager summary.
Only directory paths pass through XCom. The shared DuckDB warehouse is closed at
each stage boundary. Benchmark queries repeat calculations to compare CSV/Parquet
and worker counts; exports use stored aggregation tables.
''',
) as dag:

    @task(doc_md='Create isolated data/results directories and persist run configuration.')
    def create_workspace():
        context = get_current_context()
        key = hashlib.sha256(context['run_id'].encode()).hexdigest()[:20]
        output = Path('/opt/project/airflow-output/runs') / key
        for name in ('data', 'results', 'benchmark-work'):
            (output / name).mkdir(parents=True, exist_ok=True)
        (output / 'run.json').write_text(json.dumps(
            {'run_id': context['run_id'], 'params': dict(parameters())}, indent=2))
        print(f'Created run workspace: {output}')
        return str(output)

    @task(doc_md='Create the four-store and six-product reference CSV files.')
    def create_reference_data(output):
        from main import generate_references
        generate_references(Path(output) / 'data', parameters()['threads'])
        print('Created stores.csv and products.csv')
        return output

    @task(doc_md='Generate synthetic order IDs, dates, quantities, discounts and final statuses as CSV.')
    def generate_orders(output):
        from main import generate_orders as generate
        generate(Path(output) / 'data', parameters()['rows'], parameters()['threads'])
        return output

    @task(doc_md='Ingest all three CSV sources into persistent raw DuckDB warehouse tables.')
    def ingest_csv_to_duckdb(output):
        from airflow_pipeline import ingest
        ingest(output, parameters()['threads'])
        return output

    @task(doc_md='Reject incorrect row counts, duplicate IDs, missing references and invalid field values.')
    def validate_ingested_data(output):
        from airflow_pipeline import check_input
        check_input(output, parameters()['rows'])
        return output

    @task(doc_md='Convert the ingested raw order table to Zstandard-compressed Parquet.')
    def convert_to_parquet(output):
        from airflow_pipeline import convert
        convert(output, parameters()['threads'])
        return output

    @task(doc_md='Read Parquet, join reference data, calculate discounted revenue and filter completed orders.')
    def join_filter_and_enrich(output):
        from airflow_pipeline import enrich
        enrich(output, parameters()['threads'])
        return output

    @task(doc_md='Materialize store, monthly, product and order-status aggregations in DuckDB.')
    def aggregate_sales(output):
        from airflow_pipeline import aggregate
        aggregate(output, parameters()['threads'])
        return output

    @task(doc_md='Measure CSV/Parquet queries with one and configured worker counts; verify equivalent results and capture execution plan.')
    def benchmark_formats_and_threads(output):
        from main import analyze_data
        p = parameters()
        analyze_data(Path(output) / 'data', Path(output) / 'benchmark-work',
                     p['rows'], p['threads'], p['repeats'])
        return output

    @task(doc_md='Export stored aggregations to CSV, publish benchmark/summary artifacts and write a file manifest.')
    def export_reports(output):
        from airflow_pipeline import export_reports as export
        export(output, parameters()['threads'])
        return output

    @task(doc_md='Independently recompute revenue from raw CSV using integer arithmetic and reconcile exported reports.')
    def validate_final_reports(output):
        from validation import validate_output
        validate_output(Path(output), parameters()['rows'])
        (Path(output) / 'validated.json').write_text(json.dumps({'validated': True}))
        print(f'Validated reports: {output}/results')
        print((Path(output) / 'results/manager_summary.md').read_text())

    workspace = create_workspace()
    reference = create_reference_data(workspace)
    orders = generate_orders(reference)
    ingested = ingest_csv_to_duckdb(orders)
    checked = validate_ingested_data(ingested)
    parquet = convert_to_parquet(checked)
    enriched = join_filter_and_enrich(parquet)
    aggregated = aggregate_sales(enriched)
    benchmarked = benchmark_formats_and_threads(aggregated)
    exported = export_reports(benchmarked)
    validate_final_reports(exported)
