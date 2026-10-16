"""Exercise stage boundaries and reject reports with corrupted totals."""
from pathlib import Path
import tempfile
import unittest

from main import generate_data, convert_data, analyze_data
from validation import validate_output
from airflow_pipeline import ingest, check_input, convert, enrich, aggregate, export_reports


class PipelineTest(unittest.TestCase):
    def test_airflow_stage_artifacts(self):
        with tempfile.TemporaryDirectory(prefix='retail stages ') as folder:
            root = Path(folder)
            generate_data(root / 'data', 1000, 2)
            ingest(root, 2)
            check_input(root, 1000)
            with self.assertRaises(ValueError):
                check_input(root, 1001)
            convert(root, 2)
            enrich(root, 2)
            aggregate(root, 2)
            analyze_data(root / 'data', root / 'benchmark-work', 1000, 2, 1)
            export_reports(root, 2)
            validate_output(root, 1000)
            self.assertTrue((root / 'results/manifest.json').exists())

    def test_stages_and_corrupt_report(self):
        with tempfile.TemporaryDirectory(prefix='airflow retail ') as folder:
            root = Path(folder)
            generate_data(root / 'data', 1000, 2)
            self.assertFalse((root / 'data/sales.parquet').exists())
            convert_data(root / 'data', 2)
            analyze_data(root / 'data', root / 'results', 1000, 2, 1)
            validate_output(root, 1000)
            # Retrying stages must overwrite their own outputs safely.
            convert_data(root / 'data', 2)
            analyze_data(root / 'data', root / 'results', 1000, 2, 1)
            validate_output(root, 1000)
            report = root / 'results/monthly_revenue.csv'
            report.write_text('month,net_sales_paise\n2025-01,0\n')
            with self.assertRaisesRegex(ValueError, 'differ from raw orders'):
                validate_output(root, 1000)
