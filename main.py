"""Daily retail operations: synthetic orders -> Parquet -> manager reports."""
import argparse
import csv
import json
import os
from pathlib import Path
from statistics import median
from time import perf_counter

import duckdb

ROOT = Path(__file__).resolve().parent
QUERIES = {
    "store_performance": """
        SELECT store_name, city, count(*) AS completed_orders,
               sum(quantity) AS units_sold, sum(net_paise) AS net_sales_paise
        FROM completed_sales GROUP BY store_name, city
        ORDER BY net_sales_paise DESC, store_name
    """,
    "monthly_revenue": """
        SELECT strftime(order_date, '%Y-%m') AS month,
               sum(net_paise) AS net_sales_paise
        FROM completed_sales GROUP BY month ORDER BY month
    """,
    "top_products": """
        SELECT product_name, sum(quantity) AS units_sold,
               sum(net_paise) AS net_sales_paise
        FROM completed_sales GROUP BY product_name
        ORDER BY net_sales_paise DESC, product_name LIMIT 5
    """,
    "order_status": """
        SELECT status, count(*) AS orders, sum(net_paise) AS order_value_paise
        FROM enriched_sales GROUP BY status ORDER BY status
    """,
}


def sql_path(path):
    return "'" + path.as_posix().replace("'", "''") + "'"


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def generate_data(data, rows, threads):
    generate_references(data, threads)
    generate_orders(data, rows, threads)


def generate_references(data, threads):
    data = Path(data)
    data.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(config={"threads": threads, "memory_limit": "1GB"}) as con:
        con.execute("""CREATE TABLE stores AS SELECT * FROM (VALUES
            (1, 'Central Mart Delhi', 'Delhi'),
            (2, 'Central Mart Mumbai', 'Mumbai'),
            (3, 'Central Mart Bengaluru', 'Bengaluru'),
            (4, 'Central Mart Pune', 'Pune')
        ) AS t(store_id, store_name, city)""")
        con.execute("""CREATE TABLE products AS SELECT * FROM (VALUES
            (1, 'Rice 5kg', 'Grocery', 35000),
            (2, 'Cooking Oil 1L', 'Grocery', 16000),
            (3, 'Cotton T-shirt', 'Clothing', 49900),
            (4, 'Wireless Mouse', 'Electronics', 79900),
            (5, 'Notebook Pack', 'Stationery', 12000),
            (6, 'Water Bottle', 'Home', 29900)
        ) AS t(product_id, product_name, category, unit_price_paise)""")
        for table in ('stores', 'products'):
            con.execute(f"COPY {table} TO {sql_path(data / (table + '.csv'))} (HEADER, FORMAT CSV)")


def generate_orders(data, rows, threads):
    data = Path(data)
    data.mkdir(parents=True, exist_ok=True)
    csv_file = data / "sales.csv"
    with duckdb.connect(config={"threads": threads, "memory_limit": "1GB"}) as con:
        # SQL range generates rows inside the engine, without a huge Python list.
        # Integer arithmetic makes the synthetic dataset reproducible.
        print(f"Generating {rows:,} synthetic orders...")
        con.execute(f"""
            COPY (
                SELECT i + 1 AS order_id,
                    DATE '2025-01-01' + CAST((i * 17) % 365 AS INTEGER) AS order_date,
                    1 + (hash(i + 11) % 4)::INTEGER AS store_id,
                    1 + (hash(i + 29) % 6)::INTEGER AS product_id,
                    1 + (hash(i + 47) % 5)::INTEGER AS quantity,
                    list_extract([0, 0, 0, 5, 10],
                                 1 + (hash(i + 83) % 5)::INTEGER) AS discount_pct,
                    CASE WHEN hash(i + 101) % 100 < 5 THEN 'Cancelled'
                         WHEN hash(i + 101) % 100 < 12 THEN 'Returned'
                         ELSE 'Completed' END AS status
                FROM range({rows}) AS t(i)
            ) TO {sql_path(csv_file)} (FORMAT CSV, HEADER)
        """)


def convert_data(data, threads):
    data = Path(data)
    csv_file, parquet_file = data / "sales.csv", data / "sales.parquet"
    with duckdb.connect(config={"threads": threads, "memory_limit": "1GB"}) as con:
        csv_source = f"""read_csv({sql_path(csv_file)}, header=true, columns={{
            'order_id':'BIGINT','order_date':'DATE','store_id':'INTEGER',
            'product_id':'INTEGER','quantity':'INTEGER',
            'discount_pct':'INTEGER','status':'VARCHAR'}})"""
        print("Converting CSV to compressed columnar Parquet...")
        con.execute(f"""COPY (SELECT * FROM {csv_source})
            TO {sql_path(parquet_file)}
            (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)""")


def analyze_data(data, results, rows, threads, repeats):
    data, results = Path(data), Path(results)
    results.mkdir(parents=True, exist_ok=True)
    csv_file, parquet_file = data / "sales.csv", data / "sales.parquet"
    with duckdb.connect(config={"threads": threads, "memory_limit": "1GB"}) as con:
        for table in ('stores', 'products'):
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM read_csv_auto({sql_path(data / (table + '.csv'))})")
        csv_source = f"""read_csv({sql_path(csv_file)}, header=true, columns={{
            'order_id':'BIGINT','order_date':'DATE','store_id':'INTEGER',
            'product_id':'INTEGER','quantity':'INTEGER',
            'discount_pct':'INTEGER','status':'VARCHAR'}})"""
        sources = {"CSV": csv_source, "Parquet": f"read_parquet({sql_path(parquet_file)})"}
        benchmarks, expected = [], {}
        for format_name, source in sources.items():
            con.execute(f"CREATE OR REPLACE VIEW sales AS SELECT * FROM {source}")
            assert con.execute("SELECT count(*) FROM sales").fetchone()[0] == rows
            con.execute("""CREATE OR REPLACE VIEW enriched_sales AS
                SELECT s.*, st.store_name, st.city, p.product_name, p.category,
                    CAST(round(s.quantity * p.unit_price_paise *
                         (100 - s.discount_pct) / 100.0) AS BIGINT) AS net_paise
                FROM sales s JOIN stores st USING (store_id)
                JOIN products p USING (product_id)""")
            assert con.execute("SELECT count(*) FROM enriched_sales").fetchone()[0] == rows
            con.execute("""CREATE OR REPLACE VIEW completed_sales AS
                SELECT * FROM enriched_sales WHERE status = 'Completed'""")
            for workers in sorted({1, threads}):
                con.execute(f"SET threads = {workers}")
                for name, query in QUERIES.items():
                    con.execute(query).fetchall()  # Warm-up, excluded from timings.
                    durations = []
                    for _ in range(repeats):
                        start = perf_counter()
                        cursor = con.execute(query)
                        values = cursor.fetchall()
                        durations.append(perf_counter() - start)
                    if name in expected:
                        assert values == expected[name], f"Result mismatch: {name}"
                    else:
                        expected[name] = values
                    record = {"format": format_name, "threads": workers,
                              "query": name, "median_seconds": round(median(durations), 6)}
                    benchmarks.append(record)
                    print(f"{format_name:7} | {workers:2} threads | {name:24} | "
                          f"{record['median_seconds']:.6f}s")
                    if format_name == "Parquet" and workers == threads:
                        with (results / f"{name}.csv").open("w", newline="", encoding="utf-8") as handle:
                            writer = csv.writer(handle)
                            writer.writerow([column[0] for column in cursor.description])
                            writer.writerows(values)
            if format_name == "Parquet":
                plan = con.execute("EXPLAIN ANALYZE " + QUERIES["top_products"]).fetchall()
                (results / "query_plan.txt").write_text(
                    "\n".join(str(row[-1]) for row in plan), encoding="utf-8")
        report = {"rows": rows, "configured_threads": threads, "repeats": repeats,
                  "duckdb_version": duckdb.__version__,
                  "csv_bytes": csv_file.stat().st_size,
                  "parquet_bytes": parquet_file.stat().st_size,
                  "validation": "Row counts and all query results match across formats and thread counts.",
                  "benchmarks": benchmarks}
        (results / "benchmark.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        totals = {status: (count, value) for status, count, value in expected['order_status']}
        assert sum(item[0] for item in totals.values()) == rows
        completed_count, completed_value = totals.get('Completed', (0, 0))
        assert sum(row[4] for row in expected['store_performance']) == completed_value
        assert sum(row[1] for row in expected['monthly_revenue']) == completed_value
        leader = expected['store_performance'][0][0] if completed_count else 'No completed sales'
        best_product = expected['top_products'][0][0] if completed_count else 'No completed sales'
        summary = f"""# Central Mart: retail operations summary

Synthetic demonstration data; all prices are in INR. Reporting period: calendar year 2025.

- Orders received: {rows:,}
- Completed orders: {completed_count:,}
- Net sales after discounts and full returns: INR {completed_value / 100:,.2f}
- Cancelled orders: {totals.get('Cancelled', (0, 0))[0]:,}
- Returned orders: {totals.get('Returned', (0, 0))[0]:,}
- Highest-sales store: {leader}
- Highest-sales product: {best_product}

Use store_performance.csv to compare branches, monthly_revenue.csv to plan
seasonal staffing, and top_products.csv as an input to replenishment discussions.
order_status.csv separates cancellations and returns from completed revenue.
Inventory, costs and customer demand forecasts are outside this demo's scope.

Revenue rule: quantity x catalog price less discount, rounded to the nearest paise
per order. A row represents one single-product order. Returned orders are fully
refunded; cancelled orders never generate revenue. Status is the final status,
so this is a final-status sales report, not an accounting cash-flow ledger.
"""
        (results / 'manager_summary.md').write_text(summary, encoding='utf-8')
        ratio = report['csv_bytes'] / report['parquet_bytes']
        print(f"\nValidation passed. CSV / Parquet size ratio: {ratio:.2f}x")
        print(f"Reports saved to {results}")


def run(rows, threads, repeats, output_dir=None):
    root = Path(output_dir) if output_dir else ROOT
    generate_data(root / "data", rows, threads)
    convert_data(root / "data", threads)
    analyze_data(root / "data", root / "results", rows, threads, repeats)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=positive, default=1_000_000)
    parser.add_argument("--threads", type=positive, default=min(4, os.cpu_count() or 1))
    parser.add_argument("--repeats", type=positive, default=3)
    parser.add_argument("--output-dir", type=Path, help="Separate data/results directory")
    args = parser.parse_args()
    run(args.rows, args.threads, args.repeats, args.output_dir)
