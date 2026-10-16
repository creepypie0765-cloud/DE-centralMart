"""Local dashboard. Run: python dashboard.py, then visit http://127.0.0.1:8000."""
import argparse
import csv
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import subprocess
import sys
import threading
from urllib.parse import urlsplit, parse_qs
from orchestration import airflow_status, trigger_run

ROOT = Path(__file__).resolve().parent
REPORTS = ('store_performance', 'monthly_revenue', 'top_products', 'order_status')
lock = threading.Lock()
state = {'running': False, 'error': None}


def report_root(airflow=False):
    if airflow:
        candidates = list((ROOT / 'airflow-output/runs').glob('*/validated.json'))
        if not candidates:
            return ROOT / 'airflow-output/no-successful-run'
        return max(candidates, key=lambda p: p.stat().st_mtime).parent
    return ROOT


def run_batch(rows):
    try:
        result = subprocess.run(
            [sys.executable, str(ROOT / 'main.py'), '--rows', str(rows),
             '--threads', '4', '--repeats', '3'], cwd=ROOT,
            capture_output=True, text=True, timeout=600)
        with lock:
            state['error'] = result.stderr[-2000:] if result.returncode else None
    except Exception as exc:
        with lock:
            state['error'] = str(exc)
    finally:
        with lock:
            state['running'] = False


class Handler(BaseHTTPRequestHandler):
    def send(self, code, body, content_type='application/json'):
        payload = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(payload)))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        parsed = urlsplit(self.path)
        source_airflow = parse_qs(parsed.query).get('source') == ['airflow']
        path = parsed.path
        if self.path == '/':
            return self.send(200, (ROOT / 'ui' / 'index.html').read_bytes(), 'text/html; charset=utf-8')
        if path == '/api/airflow':
            return self.send(200, airflow_status())
        if path == '/api/reports':
            with lock:
                if state['running']:
                    return self.send(200, dict(state))
                try:
                    reports = {}
                    root = report_root(source_airflow)
                    for name in REPORTS:
                        with (root / 'results' / f'{name}.csv').open(newline='', encoding='utf-8') as handle:
                            reports[name] = list(csv.DictReader(handle))
                    benchmark = json.loads((root / 'results' / 'benchmark.json').read_text())
                    return self.send(200, {**state, 'reports': reports, 'benchmark': benchmark})
                except FileNotFoundError:
                    return self.send(200, {**state, 'empty': True})
                except (ValueError, OSError) as exc:
                    return self.send(500, {'error': str(exc)})
        if path.startswith('/download/'):
            name = path.removeprefix('/download/')
            if name not in {f'{r}.csv' for r in REPORTS} | {'benchmark.json', 'manager_summary.md', 'query_plan.txt', 'manifest.json'}:
                return self.send(404, {'error': 'Unknown report'})
            with lock:
                if state['running']:
                    return self.send(409, {'error': 'Reports are being regenerated; try again shortly.'})
                report_path = report_root(source_airflow) / 'results' / name
                if not report_path.exists():
                    return self.send(404, {'error': 'Run a batch first.'})
                mime = 'application/json' if name.endswith('.json') else ('text/csv; charset=utf-8' if name.endswith('.csv') else 'text/plain; charset=utf-8')
                return self.send(200, report_path.read_bytes(), mime)
        self.send(404, {'error': 'Not found'})

    def do_POST(self):
        if self.path not in ('/api/run', '/api/airflow/run'):
            return self.send(404, {'error': 'Not found'})
        # Only the dashboard's own browser origin may start a local process.
        origin = self.headers.get('Origin')
        if origin and origin != f'http://127.0.0.1:{self.server.server_port}':
            return self.send(403, {'error': 'Use the local dashboard address.'})
        if self.headers.get('Content-Type') != 'application/json':
            return self.send(415, {'error': 'Expected JSON'})
        try:
            size = int(self.headers.get('Content-Length', '0'))
            if not 0 < size < 1000:
                raise ValueError()
            rows = json.loads(self.rfile.read(size))['rows']
            if type(rows) is not int or rows not in (10000, 100000, 1000000):
                raise ValueError()
        except (ValueError, KeyError, TypeError):
            return self.send(400, {'error': 'Choose 10,000, 100,000 or 1,000,000 orders.'})
        if self.path == '/api/airflow/run':
            try:
                return self.send(202, trigger_run(rows))
            except (OSError, ValueError) as exc:
                return self.send(503, {'error': f'Could not trigger Airflow: {exc}'})
        with lock:
            if state['running']:
                return self.send(409, {'error': 'A batch is already running.'})
            state.update(running=True, error=None)
        threading.Thread(target=run_batch, args=(rows,), daemon=True).start()
        self.send(202, {'running': True})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8000)
    args = parser.parse_args()
    server = ThreadingHTTPServer(('127.0.0.1', args.port), Handler)
    print(f'Central Mart dashboard: http://127.0.0.1:{server.server_port}', flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()


if __name__ == '__main__':
    main()
