"""Durable stage artifacts for the Airflow demonstration."""
import json
from pathlib import Path
import shutil

import duckdb
from main import QUERIES, sql_path


def connect(output, threads=4):
    return duckdb.connect(str(Path(output) / 'warehouse.duckdb'),
                          config={'threads': threads, 'memory_limit': '1GB'})


def ingest(output, threads):
    with connect(output, threads) as con:
        for table in ('stores', 'products', 'sales'):
            source = Path(output) / 'data' / f'{table}.csv'
            con.execute(f'CREATE OR REPLACE TABLE raw_{table} AS SELECT * FROM read_csv_auto({sql_path(source)})')
            print(f'Ingested {table}: {con.execute(f"SELECT count(*) FROM raw_{table}").fetchone()[0]:,} rows')


def check_input(output, rows):
    with connect(output) as con:
        count, unique = con.execute('SELECT count(*), count(DISTINCT order_id) FROM raw_sales').fetchone()
        invalid = con.execute("""SELECT count(*) FROM raw_sales s
            LEFT JOIN raw_stores st USING(store_id)
            LEFT JOIN raw_products p USING(product_id)
            WHERE st.store_id IS NULL OR p.product_id IS NULL OR
                quantity IS NULL OR quantity NOT BETWEEN 1 AND 5 OR
                discount_pct IS NULL OR discount_pct NOT BETWEEN 0 AND 100 OR
                status IS NULL OR status NOT IN ('Completed', 'Returned', 'Cancelled') OR
                order_date IS NULL""").fetchone()[0]
        if count != rows or unique != rows or invalid:
            raise ValueError(f'Input quality failed: rows={count}, unique={unique}, invalid={invalid}')
        print(f'Input checks passed: {count:,} unique orders; valid references, quantities, discounts and statuses')


def convert(output, threads):
    with connect(output, threads) as con:
        target = Path(output) / 'data/sales.parquet'
        con.execute(f'COPY raw_sales TO {sql_path(target)} (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)')
        print(f'Converted ingested orders to {target} ({target.stat().st_size:,} bytes)')


def enrich(output, threads):
    with connect(output, threads) as con:
        source = sql_path(Path(output) / 'data/sales.parquet')
        con.execute(f"""CREATE OR REPLACE TABLE enriched_sales AS
            SELECT s.*, st.store_name, st.city, p.product_name, p.category,
                CAST(round(s.quantity * p.unit_price_paise * (100-s.discount_pct)/100.0) AS BIGINT) AS net_paise
            FROM read_parquet({source}) s JOIN raw_stores st USING(store_id)
            JOIN raw_products p USING(product_id)""")
        con.execute("CREATE OR REPLACE TABLE completed_sales AS SELECT * FROM enriched_sales WHERE status='Completed'")
        print('Joined stores and products, calculated discounted revenue, filtered completed orders')


def aggregate(output, threads):
    with connect(output, threads) as con:
        for name, query in QUERIES.items():
            con.execute(f'CREATE OR REPLACE TABLE report_{name} AS {query}')
            print(f'Aggregated {name}: {con.execute(f"SELECT count(*) FROM report_{name}").fetchone()[0]} report rows')


def export_reports(output, threads):
    output = Path(output)
    results = output / 'results'
    results.mkdir(exist_ok=True)
    with connect(output, threads) as con:
        for name, query in QUERIES.items():
            # Apply the same ordering as the source query when exporting stored aggregates.
            order = query.split('ORDER BY', 1)[1]
            con.execute(f'COPY (SELECT * FROM report_{name} ORDER BY {order}) TO {sql_path(results / (name + ".csv"))} (HEADER, FORMAT CSV)')
    for name in ('benchmark.json', 'query_plan.txt', 'manager_summary.md'):
        # Windows-mounted folders may reject Unix metadata changes (copystat).
        shutil.copyfile(output / 'benchmark-work' / name, results / name)
    manifest = {'reports': [p.name for p in sorted(results.iterdir())], 'output': str(output)}
    (results / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    print(f'Exported CSV reports, summary, benchmark and manifest to {results}')
