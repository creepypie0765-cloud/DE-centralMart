import unittest
from unittest.mock import patch

from orchestration import airflow_status, trigger_run, STAGES


class OrchestrationTest(unittest.TestCase):
    @patch('orchestration.airflow_request')
    def test_live_task_status_mapping(self, request):
        request.side_effect = [
            {'is_paused': False},
            {'dag_runs': [{'dag_run_id': 'manual run', 'state': 'running'}]},
            {'task_instances': [{'task_id': STAGES[0][0], 'state': 'success',
                                 'duration': 1.25, 'try_number': 1}]},
        ]
        result = airflow_status()
        self.assertTrue(result['connected'])
        self.assertEqual(len(result['stages']), 11)
        self.assertEqual(result['stages'][0]['state'], 'success')
        self.assertEqual(result['stages'][1]['state'], 'pending')
        self.assertIn('manual%20run', request.call_args.args[0])

    @patch('orchestration.airflow_request', side_effect=OSError('offline'))
    def test_offline_does_not_invent_success(self, request):
        result = airflow_status()
        self.assertFalse(result['connected'])
        self.assertTrue(all(s['state'] == 'not_run' for s in result['stages']))

    @patch('orchestration.airflow_request')
    def test_trigger_uses_selected_rows(self, request):
        trigger_run(10000)
        self.assertEqual(request.call_args_list[0].args[2], {'is_paused': False})
        self.assertEqual(request.call_args.args[2]['conf']['rows'], 10000)
