"""Boundary and censoring checks for the manual batch evaluator.

Run: python3 -m unittest discover -s examples/consensus -p test_consensus_statistics.py
"""
import csv
import json
from pathlib import Path
import tempfile
import unittest

from consensus_single_run import (background_metrics, evaluate_run, path_metrics,
                                  recovery_rounds, throughput_samples)


def write_rows(path, fields, rows):
    with Path(path).open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


class StatisticsTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.prefix = Path(self.directory.name) / 'run'

    def artifact(self, suffix):
        return str(self.prefix) + '-' + suffix

    def test_trailing_window_boundaries_and_weights(self):
        events = [(0, 100), (1, 1), (250000, 2), (250001, 3)]
        samples = list(throughput_samples(events, 260000))
        self.assertEqual(samples, [(250000, 3, 12), (260000, 5, 20)])
        self.assertEqual(list(throughput_samples(events, 249999)), [])

    def test_recovery_requires_all_honest_nodes_in_same_view(self):
        def phase(node, kind, time, view):
            return dict(protocol='zyzzyva', node_id=str(node), phase=str(kind),
                        time_us=str(time), view=str(view), instance_id=str(view))
        rows = [phase(0, 19, 100, 0)]
        # Node zero is malicious; a later-view event cannot fill a missing view-1 event.
        rows += [phase(0, 17, 1000, 1), phase(1, 17, 150, 1),
                 phase(2, 17, 160, 1), phase(3, 17, 170, 2)]
        config = dict(replica_count=4, zyzzyva_malicious_replica_ids='0')
        item = recovery_rounds(rows, config)[0]
        self.assertIsNone(item['all_correct_recovered_us'])
        rows += [phase(3, 17, 180, 1)]
        item = recovery_rounds(rows, config)[0]
        self.assertEqual(item['all_correct_recovered_us'], 180)
        self.assertEqual(item['all_correct_recovered_latency_us'], 80)

    def test_legacy_recovery_is_not_falsely_certified(self):
        rows = [dict(protocol='zyzzyva', node_id='0', phase='19', time_us='100')]
        item = recovery_rounds(rows, dict(replica_count=4))[0]
        self.assertEqual(item['status'], 'legacy-missing-view')

    def test_background_unfinished_flows_are_counted(self):
        fields = ['event','time_ns','sender_id','receiver_id','flow_id','bytes','fct_ns']
        write_rows(self.artifact('background-events.csv'), fields, [
            dict(zip(fields, ['start',0,1,2,1,1000,''])),
            dict(zip(fields, ['start',0,1,2,2,20000000,''])),
            dict(zip(fields, ['complete',100000,1,2,1,1000,100000])),
        ])
        all_flows = background_metrics(self.prefix)[0]
        self.assertEqual(all_flows['started'], 2)
        self.assertEqual(all_flows['completed'], 1)
        self.assertEqual(all_flows['completion_fraction'], .5)
        self.assertEqual(all_flows['p99_us'], 100)

    def test_background_unmatched_completion_is_rejected(self):
        fields = ['event','time_ns','sender_id','receiver_id','flow_id','bytes','fct_ns']
        write_rows(self.artifact('background-events.csv'), fields,
                   [dict(zip(fields, ['complete',100,1,2,1,1000,100]))])
        with self.assertRaises(ValueError):
            background_metrics(self.prefix)

    def test_paths_match_message_identity_not_only_sequence(self):
        fields = ['direction','sender_id','receiver_id','message_type','sequence_number',
                  'bytes','time_us','message_id','view','client_id','request_id','digest','time_ns']
        common = dict(sender_id='200', receiver_id='100', message_type='30',
                      sequence_number='1', bytes='100', time_us='0', view='0',
                      client_id='1000', request_id='1', digest='123')
        rows = []
        for direction, message_id, time in [('tx','1',0), ('tx','2',1000),
                                             ('rx','2',2000), ('rx','1',4000)]:
            rows.append(dict(common, direction=direction, message_id=message_id, time_ns=time))
        write_rows(self.artifact('transaction-path.csv'), fields, rows)
        item = path_metrics(self.prefix, dict(replica_count=10))[0]
        self.assertEqual(item['samples'], 2)
        self.assertEqual(item['mean_us'], 2.5)
        self.assertEqual(item['max_us'], 4)

    def fixture(self, protocol='bidl'):
        config = dict(architecture=protocol, replica_count=2, simulation_time=1,
                      topology_file='topology.txt', flow_file='', request_rate=3,
                      rdma_cc_mode=1, seed=1, run=1)
        manifest = dict(expected_requests=3, measurement_start=0, measurement_stop=1,
                        status='incomplete')
        Path(self.artifact('config.json')).write_text(json.dumps(config))
        Path(self.artifact('manifest.json')).write_text(json.dumps(manifest))
        write_rows(self.artifact('summary.csv'), ['metric','value','unit'], [])
        fields = ['client_id','request_id','submitted_us','completed_us','status','transaction_count','completion_path']
        write_rows(self.artifact('request-events.csv'), fields, [
            dict(zip(fields, [1000,1,0,100000,'complete',1,'fast'])),
            dict(zip(fields, [1000,2,250000,800000,'complete',1,'slow'])),
            dict(zip(fields, [1000,3,300000,'','incomplete',1,''])),
        ])

    def test_commit_dedup_cohort_and_incomplete_overall(self):
        self.fixture()
        fields = ['time_ns','node_id','sequence_number','block_id','client_id','request_id','transaction_count']
        write_rows(self.artifact('transaction-finalized.csv'), fields, [
            dict(zip(fields, [90000000,100,1,1,1000,1,1])),
            dict(zip(fields, [95000000,101,1,1,1000,1,1])),
            dict(zip(fields, [96000000,100,1,1,1000,1,1])),
            dict(zip(fields, [700000000,100,2,2,1000,2,1])),
        ])
        item, events, _, _ = evaluate_run(self.prefix, 'fixture', .25, .75)
        self.assertEqual(item['unique_requests_first_executor_committed'], 2)
        self.assertEqual(item['unique_requests_all_executors_committed'], 1)
        self.assertEqual(item['window_first_executor_commit_tps'], 2)
        self.assertEqual(item['window_client_completion_tps'], 0)
        self.assertEqual(item['client_to_first_executor_commit_p50_us'], 450000)
        self.assertEqual(item['latency_cohort_incomplete'], 1)
        self.assertIsNone(item['full_workload_client_tps'])
        self.assertEqual(len(events['first_executor_commit']), 2)

    def test_fast_and_slow_completion_are_separate_from_reexecution(self):
        self.fixture('zyzzyva')
        item, _, _, _ = evaluate_run(self.prefix, 'fixture')
        self.assertEqual(item['fast_path_success_fraction_completed'], .5)
        self.assertEqual(item['fast_path_success_fraction_offered'], 1/3)
        self.assertIsNone(item['zyzzyva_correct_reexecution_events_per_execution_event'])



class SlurmMatrixTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import importlib.util
        script = Path(__file__).parent/'scripts/slurm/consensus-slurm.py'
        spec = importlib.util.spec_from_file_location('consensus_slurm', script)
        cls.slurm = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.slurm)
        runner_spec = importlib.util.spec_from_file_location('consensus_launcher', Path(__file__).parent/'run-experiment.py')
        cls.runner = importlib.util.module_from_spec(runner_spec)
        runner_spec.loader.exec_module(cls.runner)

    def matrix(self):
        return json.loads(self.slurm.DEFAULT_SPEC.read_text())

    def test_default_matrix_is_ninety_paired_fixed_seed_bidl_tasks(self):
        tasks = list(self.slurm.expand(self.matrix()))
        self.assertEqual(len(tasks), 90)
        groups = {}
        for name, parameters, axes, seed in tasks:
            self.assertEqual(seed, 1)
            self.assertEqual(parameters['run'], 1)
            self.assertEqual(parameters['preset'], 'bidl-rdma')
            self.assertEqual(int(parameters['replicaCount']), 10)
            self.assertEqual(int(parameters['requestRate']), 30000)
            self.assertEqual(int(parameters['requestCount']), 150000)
            self.assertEqual(float(parameters['measurementTime']), 5)
            self.assertEqual(int(parameters['requestCount']) / int(parameters['requestRate']), 5)
            self.assertEqual(float(parameters['simulationTime']), 20)
            groups.setdefault(name, []).append(axes['flowFile'])
        self.assertEqual(len(groups), 9)
        self.assertTrue(all(len(values) == 10 for values in groups.values()))
        self.assertEqual(len({tuple(values) for values in groups.values()}), 1)

    def test_cc_arms_change_only_algorithm_and_label(self):
        tasks = list(self.slurm.expand(self.matrix()))
        arms = {name: parameters for name, parameters, _, _ in tasks}
        reference = arms['bidl-attack-dcqcn']
        for name, mode in (('bidl-attack-hpcc', 3), ('bidl-attack-timely', 7), ('bidl-attack-dctcp', 8)):
            candidate = arms[name]
            changes = {key for key in reference.keys() | candidate.keys() if reference.get(key) != candidate.get(key)}
            self.assertEqual(changes, {'scenario', 'rdmaCcMode'})
            self.assertEqual(candidate['rdmaCcMode'], mode)
            self.assertEqual(candidate['iorsMode'], '0')
        iors = arms['bidl-attack-iors-dcqcn']
        self.assertEqual(iors['iorsMode'], '1')
        self.assertEqual(iors['iorsTerminalGate'], 'true')
        self.assertEqual(iors['iorsGateTimeoutUs'], '1000')
        self.assertEqual(iors['bidlAttackProbability'], '1')
        self.assertEqual(iors['bidlAttackTargetExecutors'], '0')
        for label, mode in (('dcqcn', 1), ('hpcc', 3), ('timely', 7), ('dctcp', 8)):
            plain = arms['bidl-attack-' + label]
            protected = arms['bidl-attack-iors-' + label]
            self.assertEqual(plain['rdmaCcMode'], mode)
            self.assertEqual(protected['rdmaCcMode'], mode)
            differences = {key for key in plain.keys() | protected.keys()
                           if plain.get(key) != protected.get(key)}
            self.assertEqual(differences, {'scenario', 'outputPrefix', 'iorsMode', 'iorsTerminalGate', 'iorsGateTimeoutUs'})
            self.assertEqual(protected['iorsTerminalGate'], 'true')
            self.assertEqual(protected['iorsGateTimeoutUs'], '1000')
        self.assertEqual(arms['bidl-normal-dcqcn']['bidlAttackEnabled'], 'false')

    def test_cli_filters_preserve_pairing_and_source_matrix(self):
        source = self.matrix()
        selected = self.slurm.select_matrix(source, ['bidl-attack-dcqcn', 'bidl-attack-iors-dcqcn'], [0, 5, 25, 50])
        tasks = list(self.slurm.expand(selected))
        self.assertEqual(len(tasks), 8)
        self.assertEqual(len(source['axes']['flowFile']), 10)
        self.assertEqual(len(source['cases']), 9)
        self.assertEqual(tasks[0][1]['flowFile'], '')
        with self.assertRaises(ValueError):
            self.slurm.select_matrix(source, hadoop_loads=[100])
        with self.assertRaises(ValueError):
            self.slurm.select_matrix(source, case_names=['unknown'])

    def test_duplicate_case_labels_are_rejected(self):
        matrix = self.matrix()
        matrix['cases'].append(matrix['cases'][0])
        with self.assertRaises(ValueError):
            list(self.slurm.expand(matrix))

    def test_abnormal_exit_is_not_misclassified_as_incomplete(self):
        incomplete = {'status': 'incomplete'}
        self.assertEqual(self.runner.classify_exit(1, incomplete), 3)
        self.assertEqual(self.runner.classify_exit(0, {'status': 'complete'}), 0)
        for code in (0, 2, 127, -6, -9, -15):
            self.assertEqual(self.runner.classify_exit(code, incomplete), 1)
        self.assertEqual(self.runner.classify_exit(0, {'status': 'complete', 'confirmed_safety_error': True}), 1)

    def test_runtime_fingerprint_covers_iors_and_rdma_dependencies(self):
        fingerprints = self.slurm.runtime_fingerprints()
        self.assertTrue(any('point-to-point' in path for path in fingerprints))
        self.assertTrue(any('core-optimized' in path for path in fingerprints))
        self.assertTrue(all(len(value) == 64 for value in fingerprints.values()))

    def test_changed_trace_is_rejected_before_launch(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            trace = root/'flow.txt'
            trace.write_text('1\n0 1 3 100 1000 2000000000 0\n')
            manifest = {'input_files': {'flow.txt': {'bytes': trace.stat().st_size, 'sha256': self.slurm.digest(trace)}}}
            task = {'input_files': {'flowFile': 'flow.txt'}}
            with patch.object(self.slurm, 'NS3', root):
                self.slurm.verify_inputs(manifest, [task])
                trace.write_text('1\n0 1 3 100 1000 3000000000 0\n')
                with self.assertRaises(ValueError):
                    self.slurm.verify_inputs(manifest, [task])

if __name__ == '__main__':
    unittest.main()
