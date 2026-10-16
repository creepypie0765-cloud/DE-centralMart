"""Independent integer-arithmetic checks for completed retail batch outputs."""
import csv
import json
from pathlib import Path


def validate_output(output, expected_rows):
    output = Path(output)

    def records(folder, name):
        with (output / folder / name).open(newline='', encoding='utf-8') as handle:
            yield from csv.DictReader(handle)

    prices = {r['product_id']: int(r['unit_price_paise'])
              for r in records('data', 'products.csv')}
    count, completed, total = 0, 0, 0
    for row in records('data', 'sales.csv'):
        count += 1
        if row['status'] == 'Completed':
            completed += 1
            total += (int(row['quantity']) * prices[row['product_id']] *
                      (100 - int(row['discount_pct'])) + 50) // 100
    if count != expected_rows:
        raise ValueError(f'Expected {expected_rows} orders, found {count}')
    for name in ('store_performance.csv', 'monthly_revenue.csv'):
        actual = sum(int(r['net_sales_paise']) for r in records('results', name))
        if actual != total:
            raise ValueError(f'{name}: sales {actual} differ from raw orders {total}')
    stores = list(records('results', 'store_performance.csv'))
    if sum(int(r['completed_orders']) for r in stores) != completed:
        raise ValueError('Completed order count differs from raw orders')
    if sum(int(r['orders']) for r in records('results', 'order_status.csv')) != count:
        raise ValueError('Status report order count differs from raw orders')
    benchmark = json.loads((output / 'results/benchmark.json').read_text())
    if benchmark['rows'] != count or not benchmark['benchmarks']:
        raise ValueError('Missing or inconsistent benchmark results')
    print(f'Independent validation passed: {count:,} orders; net sales INR {total / 100:,.2f}')
