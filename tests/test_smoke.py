"""End-to-end checks in a disposable checkout; never overwrite a user's reports."""
import csv
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


class SmokeTest(unittest.TestCase):
    def test_fresh_checkout_dashboard_and_batch(self):
        with tempfile.TemporaryDirectory(prefix='retail demo ') as folder:
            work = Path(folder)
            for name in ('main.py', 'dashboard.py', 'orchestration.py'):
                shutil.copy2(ROOT / name, work / name)
            shutil.copytree(ROOT / 'ui', work / 'ui')
            server = subprocess.Popen([sys.executable, '-u', str(work / 'dashboard.py'), '--port', '0'],
                                      stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
            try:
                url = server.stdout.readline().strip().split()[-1]
                self.assertTrue(url.startswith('http://127.0.0.1:'))
                def get(path):
                    with urllib.request.urlopen(url + path, timeout=10) as response:
                        return response.read()
                self.assertIn(b'Central Mart', get('/'))
                self.assertTrue(json.loads(get('/api/reports'))['empty'])
                request = urllib.request.Request(url + '/api/run', data=b'{"rows":10000}',
                                                 headers={'Content-Type': 'application/json'}, method='POST')
                with urllib.request.urlopen(request, timeout=10) as response:
                    self.assertEqual(response.status, 202)
                deadline = time.monotonic() + 90
                while time.monotonic() < deadline:
                    result = json.loads(get('/api/reports'))
                    if not result['running']:
                        break
                    time.sleep(0.2)
                self.assertFalse(result['running'], 'Batch did not finish')
                self.assertIsNone(result['error'])
                self.assertEqual(result['benchmark']['rows'], 10000)
                self.assertIn(b'net_sales_paise', get('/download/store_performance.csv'))
                # Independently calculate completed sales from source CSV using integer arithmetic.
                def read(name):
                    with (work / 'data' / name).open(newline='', encoding='utf-8') as handle:
                        return list(csv.DictReader(handle))
                prices = {r['product_id']: int(r['unit_price_paise']) for r in read('products.csv')}
                total = sum((int(r['quantity']) * prices[r['product_id']] *
                             (100-int(r['discount_pct'])) + 50) // 100
                            for r in read('sales.csv') if r['status'] == 'Completed')
                actual = sum(int(r['net_sales_paise']) for r in result['reports']['store_performance'])
                self.assertEqual(total, actual)
                for payload, expected in [(b'{"rows":-1}', 400), (b'{"rows":true}', 400)]:
                    request = urllib.request.Request(url + '/api/run', data=payload,
                        headers={'Content-Type': 'application/json'}, method='POST')
                    with self.assertRaises(urllib.error.HTTPError) as error:
                        urllib.request.urlopen(request, timeout=10)
                    self.assertEqual(error.exception.code, expected)
            finally:
                server.terminate()
                server.wait(timeout=10)
                server.stdout.close()
