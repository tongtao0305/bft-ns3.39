#!/usr/bin/env python3
"""Prepare, submit, execute and collect 128-host consensus experiments.

Run from the ns-3 root. Workers need identical local project and campaign
copies; preparation and submission do not synchronize files automatically.
"""
import argparse
import csv
import copy
import hashlib
import itertools
import json
import os
from pathlib import Path
import re
import shlex
import signal
import socket
import subprocess
import sys

NS3 = Path(__file__).resolve().parents[4]
SCRIPT = Path(__file__).resolve()
RUNTIME = [
    'build/examples/consensus/ns3.39-bidl-experiment-optimized',
    'build/lib/libns3.39-consensus-optimized.so',
    'examples/consensus/run-experiment.py',
    str(SCRIPT.relative_to(NS3)),
]

# One maintained matrix file; no duplicated embedded experiment defaults.
DEFAULT_SPEC = SCRIPT.with_name('bidl-hadoop-matrix.json')
DEFAULT_RESOURCES = {'tasks_per_node': 1, 'memory': '16G', 'time': '04:00:00'}


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()



def runtime_fingerprints(binary=None):
    binary = (binary or NS3 / RUNTIME[0]).resolve()
    paths = set(RUNTIME[2:])
    paths.add(str(binary.relative_to(NS3)))
    result = subprocess.run(['ldd', str(binary)], capture_output=True, text=True, check=True)
    if 'not found' in result.stdout:
        raise ValueError('simulator has missing shared libraries: ' + result.stdout.strip())
    for dependency in re.findall(r'=>\s+(/\S+)', result.stdout):
        path = Path(dependency).resolve()
        try:
            relative = path.relative_to(NS3)
        except ValueError:
            continue
        if path.name.startswith('libns3'):
            paths.add(str(relative))
    return {name: digest(NS3 / name) for name in sorted(paths)}


def read_config(path):
    result = {}
    for number, raw in enumerate(path.read_text().splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        key, separator, value = line.partition('=')
        key, value = key.strip(), value.strip()
        if not separator or not key or key in result:
            raise ValueError(f'{path}:{number}: invalid/duplicate configuration entry')
        result[key] = value
    return result


def expand(spec):
    seeds = spec.get('seeds', [1])
    if not seeds or len(set(seeds)) != len(seeds) or any(type(seed) is not int or not 1 <= seed <= 2**32-1 for seed in seeds):
        raise ValueError('seeds must be unique integers in 1..4294967295')
    axes = spec.get('axes', {})
    if any(not isinstance(values, list) or not values for values in axes.values()):
        raise ValueError('each axis must have a nonempty list of values')
    if not spec.get('cases'):
        raise ValueError('at least one case is required')
    names = [case['name'] for case in spec['cases']]
    if len(set(names)) != len(names) or any(not re.fullmatch(r'[A-Za-z0-9_.-]+', name) for name in names):
        raise ValueError('case names must be unique simple labels')
    for case in spec['cases']:
        base = read_config(NS3 / case['base_config']) if case.get('base_config') else {}
        for values in itertools.product(*axes.values()):
            selected = dict(zip(axes, values))
            parameters = {**base, **spec.get('defaults', {}), **case.get('parameters', {}), **selected}
            for key, value in parameters.items():
                if (not isinstance(key, str) or not key or any(c in key for c in '=\n\r')
                        or value is None or '\n' in str(value) or '\r' in str(value)):
                    raise ValueError('invalid configuration key or value')
            if parameters.get('topology') != 'qbb-file' or not parameters.get('topologyFile'):
                raise ValueError('each task must specify topology=qbb-file and a 128-host topologyFile')
            for key in ('topologyFile', 'flowFile'):
                if parameters.get(key) and not (NS3 / parameters[key]).is_file():
                    raise ValueError(f'missing {key}: {parameters[key]}')
            with (NS3 / parameters['topologyFile']).open() as stream:
                nodes, switches, _ = map(int, stream.readline().split())
            if nodes - switches != 128:
                raise ValueError(f'expected 128 hosts: {parameters["topologyFile"]}')
            for seed in seeds:
                yield case['name'], parameters, selected, seed



def select_matrix(spec, case_names=None, hadoop_loads=None):
    spec = copy.deepcopy(spec)
    if case_names is not None:
        available = {case['name']: case for case in spec.get('cases', [])}
        if len(set(case_names)) != len(case_names) or set(case_names) - available.keys():
            raise ValueError('--cases contains duplicate or unknown case names')
        spec['cases'] = [available[name] for name in case_names]
    if hadoop_loads is not None:
        if len(set(hadoop_loads)) != len(hadoop_loads):
            raise ValueError('--hadoop-loads must not contain duplicates')
        available = {0: ''}
        flows = spec.get('axes', {}).get('flowFile', [])
        if not flows:
            raise ValueError('--hadoop-loads requires a flowFile axis')
        for flow in flows:
            if not flow:
                continue
            match = re.fullmatch(r'bg-hadoop-128host-(\d+)G-(?:0t1|2t3)\.txt', Path(flow).name)
            if not match or int(match.group(1)) in available:
                raise ValueError('--hadoop-loads requires one unambiguous Hadoop file per rate')
            available[int(match.group(1))] = flow
        if set(hadoop_loads) - available.keys():
            raise ValueError('--hadoop-loads rate is absent from the source matrix; 0 disables background')
        spec['axes']['flowFile'] = [available[rate] for rate in hadoop_loads]
    return spec


def prepare(args):
    output = args.output.resolve()
    output.relative_to(NS3)
    if any(character.isspace() for character in str(output)):
        raise ValueError('campaign paths cannot contain whitespace (simulator outputPrefix limitation)')
    if output.exists():
        raise ValueError('campaign directory already exists; choose a new name')
    spec_path = args.spec or DEFAULT_SPEC
    spec = select_matrix(json.loads(spec_path.read_text()), getattr(args, 'cases', None),
                         getattr(args, 'hadoop_loads', None))
    tasks = list(expand(spec))
    if not tasks:
        raise ValueError('empty task list')
    for _, parameters, _, _ in tasks:
        if not 0 <= int(parameters.get('run', 1)) <= 2**64-1:
            raise ValueError('run must be in 0..18446744073709551615')
    binary = (args.binary or NS3 / RUNTIME[0]).resolve()
    runtime = runtime_fingerprints(binary)
    # Hash each unique input once, even when many cases replay the same large trace.
    inputs = {}
    for _, parameters, _, _ in tasks:
        for key in ('topologyFile', 'flowFile'):
            if parameters.get(key) and parameters[key] not in inputs:
                path = (NS3 / parameters[key]).resolve()
                path.relative_to(NS3)
                inputs[parameters[key]] = {'sha256': digest(path), 'bytes': path.stat().st_size}
    resources = {**DEFAULT_RESOURCES, **spec.get('slurm', {})}
    if set(resources) != set(DEFAULT_RESOURCES) or type(resources['tasks_per_node']) is not int or resources['tasks_per_node'] < 1:
        raise ValueError('invalid slurm resources; supported keys: tasks_per_node, memory, time')
    if any(not isinstance(resources[key], str) or not resources[key] or any(c in resources[key] for c in '\n\r\0') for key in ('memory', 'time')):
        raise ValueError('slurm memory/time must be nonempty single-line strings')
    output.mkdir(parents=True)
    for name in ('configs', 'logs', 'results'):
        (output / name).mkdir()
    (output / 'spec.json').write_text(json.dumps(spec, indent=2) + '\n')
    manifest = {'schema_version': 1, 'binary': str(binary.relative_to(NS3)),
                'runtime_sha256': runtime, 'input_files': inputs,
                'resources': resources, 'source_spec': str(spec_path.resolve()), 'tasks': []}
    for index, (name, parameters, selected, seed) in enumerate(tasks):
        config = output / 'configs' / f'task-{index:05d}.conf'
        keys = sorted(parameters, key=lambda key: (key != 'preset', key))
        config.write_text(''.join(
            f'{key}={str(parameters[key]).lower() if isinstance(parameters[key], bool) else parameters[key]}\n'
            for key in keys))
        manifest['tasks'].append({
            'task_id': index, 'name': name, 'seed': seed, 'run': int(parameters.get('run', 1)), 'axes': selected,
            'input_files': {key: parameters[key] for key in ('topologyFile', 'flowFile') if parameters.get(key)},
            'config': str(config.relative_to(output)), 'config_sha256': digest(config),
        })
    with (output / 'tasks.csv').open('w', newline='') as stream:
        fields = ['task_id', 'case', 'seed', 'run', 'preset', 'topologyFile', 'flowFile', 'hadoop_Gbps_per_host',
                  'rdmaCcMode', 'bidlAttackEnabled', 'iorsMode', 'iorsTerminalGate', 'requestRate', 'requestCount',
                  'replicaCount', 'measurementTime', 'simulationTime']
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for index, (name, parameters, selected, seed) in enumerate(tasks):
            row = {key: parameters.get(key, '') for key in fields}
            load = re.search(r'128host-(\d+)G-', parameters.get('flowFile', ''))
            row.update(task_id=index, case=name, seed=seed, run=int(parameters.get('run', 1)),
                       hadoop_Gbps_per_host=int(load.group(1)) if load else '')
            writer.writerow(row)
    (output / 'tasks.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(f'Prepared {len(tasks)} tasks in {output}; no experiments started. Review tasks.csv before submission.')


def read_campaign(campaign):
    manifest = json.loads((campaign / 'tasks.json').read_text())
    if manifest['schema_version'] != 1:
        raise ValueError('unsupported task manifest schema')
    if not manifest['tasks']:
        raise ValueError('empty task list')
    if any(task['task_id'] != index for index, task in enumerate(manifest['tasks'])):
        raise ValueError('manifest task index mismatch')
    return manifest


def verify_runtime(manifest):
    for relative, expected in manifest['runtime_sha256'].items():
        path = (NS3 / relative).resolve()
        path.relative_to(NS3)
        if digest(path) != expected:
            raise ValueError(f'runtime differs from prepared campaign: {relative}; synchronize and prepare again')


def verify_inputs(manifest, tasks):
    checked = set()
    for task in tasks:
        for relative in task.get('input_files', {}).values():
            if relative in checked:
                continue
            checked.add(relative)
            path = (NS3 / relative).resolve()
            path.relative_to(NS3)
            expected = manifest['input_files'][relative]
            if path.stat().st_size != expected['bytes'] or digest(path) != expected['sha256']:
                raise ValueError(f'input differs from prepared campaign: {relative}; synchronize and prepare again')


def task_config(campaign, task):
    config = (campaign / task['config']).resolve()
    config.relative_to(campaign)
    if digest(config) != task['config_sha256']:
        raise ValueError('configuration fingerprint differs from the prepared copy')
    return config


def workers(args):
    names = args.workers
    if not names or len(names) != len(set(names)):
        raise ValueError('workers must be nonempty and unique')
    if any(not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', name) for name in names):
        raise ValueError('workers must be hostnames or SSH aliases')
    return names


def submit(args):
    if not args.partition:
        raise ValueError('supply --partition or CONSENSUS_SLURM_PARTITION')
    campaign = args.campaign.resolve()
    manifest = read_campaign(campaign)
    verify_runtime(manifest)
    for task in manifest['tasks']:
        task_config(campaign, task)
    verify_inputs(manifest, manifest['tasks'])
    resources = {**DEFAULT_RESOURCES, **manifest.get('resources', {})}
    for field in DEFAULT_RESOURCES:
        value = getattr(args, field)
        if value is not None:
            resources[field] = value
    if resources['tasks_per_node'] < 1:
        raise ValueError('--tasks-per-node must be positive')
    names = workers(args)
    count = len(manifest['tasks'])
    # sbatch reads this script from stdin; no separate .sbatch source file is needed.
    script = ('#!/usr/bin/env bash\nset -euo pipefail\n'
              'export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1\n'
              + shlex.join(['srun', '--ntasks=1', '--cpus-per-task=1',
                            'python3', str(SCRIPT), 'run-task', str(campaign)]) + '\n')
    for index, worker in enumerate(names):
        if index >= count:
            break
        command = [
            'sbatch', '--parsable', '--job-name=consensus', f'--partition={args.partition}',
            f'--nodelist={worker}', '--nodes=1', '--ntasks=1', '--cpus-per-task=1',
            f"--mem={resources['memory']}", f"--time={resources['time']}",
            f"--array={index}-{count - 1}:{len(names)}%{resources['tasks_per_node']}",
            f'--chdir={NS3}', f'--output={campaign}/logs/%A_%a.out',
            f'--error={campaign}/logs/%A_%a.err', '--export=ALL',
        ]
        if args.account:
            command.append(f'--account={args.account}')
        if args.dry_run:
            print(shlex.join(command))
            print(script, end='')
        else:
            (campaign / 'logs').mkdir(exist_ok=True)
            job = subprocess.run(command, input=script, text=True, capture_output=True, check=True).stdout.strip()
            with (campaign / 'submissions.jsonl').open('a') as stream:
                stream.write(json.dumps({'worker': worker, 'job_id': job, 'command': command}) + '\n')
            print(f'{worker}: {job}')


def run_task(args):
    if args.task_id is None or args.task_id < 0:
        raise ValueError('a nonnegative --task-id or SLURM_ARRAY_TASK_ID is required')
    campaign = args.campaign.resolve()
    manifest = read_campaign(campaign)
    if args.task_id >= len(manifest['tasks']):
        raise ValueError('task ID is out of range')
    task = manifest['tasks'][args.task_id]
    config = task_config(campaign, task)
    verify_runtime(manifest)
    verify_inputs(manifest, [task])
    job = os.environ.get('SLURM_ARRAY_JOB_ID', os.environ.get('SLURM_JOB_ID', 'local'))
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', job) or job in ('.', '..'):
        raise ValueError('invalid Slurm job ID')
    attempt = campaign / 'results' / f'task-{args.task_id:05d}' / f'job-{job}'
    if attempt.exists():
        raise ValueError(f'attempt already exists: {attempt}; use a new array job')
    attempt.mkdir(parents=True, exist_ok=False)
    command = [sys.executable, str(NS3 / 'examples/consensus/run-experiment.py'),
               '--config', str(config), '--seed', str(task['seed']), '--run', str(task.get('run', 1)), '--output', str(attempt)]
    if manifest.get('binary'):
        command.extend(['--binary', str(NS3 / manifest['binary'])])
    print(f'host={socket.gethostname()} task={args.task_id} case={task["name"]} output={attempt}', flush=True)
    child = subprocess.Popen(command, cwd=NS3)

    def forward(signum, _frame):
        try:
            child.send_signal(signum)
        except ProcessLookupError:
            pass

    signal.signal(signal.SIGTERM, forward)
    signal.signal(signal.SIGINT, forward)
    code = child.wait()
    metadata = {'hostname': socket.gethostname(), 'slurm_job_id': os.environ.get('SLURM_JOB_ID'),
                'slurm_array_job_id': job, 'task': task, 'runner_exit_code': code}
    (attempt / 'slurm-task.json').write_text(json.dumps(metadata, indent=2) + '\n')
    if code == 0:
        try:
            status = json.loads((attempt / 'run-status.json').read_text())
            if status.get('launcher_exit_code') != 0:
                return 1
        except (OSError, ValueError) as exc:
            print(f'Missing or invalid run-status.json: {exc}', file=sys.stderr)
            return 1
    return code if code >= 0 else 128 - code


def collect(args):
    campaign = args.campaign.resolve()
    read_campaign(campaign)
    for worker in workers(args):
        for name in ('results', 'logs'):
            destination = campaign / 'collected' / worker / name
            command = ['rsync', '-a', '--protect-args', f'{worker}:{campaign}/{name}/', str(destination) + '/']
            if args.dry_run:
                print(shlex.join(command))
            else:
                destination.mkdir(parents=True, exist_ok=True)
                subprocess.run(command, check=True)
    if not args.dry_run:
        print(f'Results copied to {campaign}/collected; retain worker copies until validation.')


def example(args):
    content = DEFAULT_SPEC.read_text()
    if args.output:
        with args.output.open('x') as stream:
            stream.write(content)
    else:
        print(content, end='')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    p = commands.add_parser('example', help='Print or export the maintained 90-task BIDL/Hadoop matrix')
    p.add_argument('--output', type=Path, help='New JSON file; existing files are not overwritten')
    p.set_defaults(action=example)
    p = commands.add_parser('prepare', help='Expand a matrix into task configurations; no jobs are started')
    p.add_argument('--spec', type=Path, help='JSON matrix; defaults to bidl-hadoop-matrix.json (90 tasks)')
    p.add_argument('--output', type=Path, required=True, help='New campaign directory under the ns-3 root')
    p.add_argument('--binary', type=Path, help='Simulator under the ns-3 root; recorded and verified for every task')
    p.add_argument('--cases', nargs='+', help='Select named comparison groups from the matrix')
    p.add_argument('--hadoop-loads', nargs='+', type=int, help='Select Hadoop Gbit/s per host, e.g. 5 25 50; 0 disables background')
    p.set_defaults(action=prepare)
    p = commands.add_parser('run-task', help='Execute one task locally or inside a Slurm array')
    p.add_argument('campaign', type=Path)
    p.add_argument('--task-id', type=int, default=os.environ.get('SLURM_ARRAY_TASK_ID'))
    p.set_defaults(action=run_task)
    default_workers = os.environ.get('CONSENSUS_SLURM_WORKERS', 'ubuntu-236 ubuntu-237').replace(',', ' ').split()
    p = commands.add_parser('submit', help='Submit prepared tasks after synchronizing all worker copies')
    p.add_argument('campaign', type=Path)
    p.add_argument('--partition', default=os.environ.get('CONSENSUS_SLURM_PARTITION'))
    p.add_argument('--account', default=os.environ.get('CONSENSUS_SLURM_ACCOUNT'))
    p.add_argument('--workers', nargs='+', default=default_workers)
    p.add_argument('--tasks-per-node', type=int, default=os.environ.get('CONSENSUS_TASKS_PER_NODE'), help='CLI/environment overrides matrix resource budget')
    p.add_argument('--memory', default=os.environ.get('CONSENSUS_SLURM_MEMORY'), help='Overrides matrix memory (default 16G)')
    p.add_argument('--time', default=os.environ.get('CONSENSUS_SLURM_TIME'), help='Overrides matrix time (default 04:00:00)')
    p.add_argument('--dry-run', action='store_true', help='Print submission commands and job script only')
    p.set_defaults(action=submit)
    p = commands.add_parser('collect', help='Copy worker-local results and logs back with rsync')
    p.add_argument('campaign', type=Path)
    p.add_argument('--workers', nargs='+', default=default_workers)
    p.add_argument('--dry-run', action='store_true')
    p.set_defaults(action=collect)
    args = parser.parse_args()
    try:
        return args.action(args) or 0
    except subprocess.CalledProcessError as exc:
        print(exc.stderr or str(exc), file=sys.stderr)
        return 1
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.error(str(exc))


if __name__ == '__main__':
    sys.exit(main())
