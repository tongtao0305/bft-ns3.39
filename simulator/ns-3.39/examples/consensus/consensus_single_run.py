"""Explicit batch evaluation of raw consensus simulation artifacts."""
import csv
import json
import math
from pathlib import Path


def read_summary(path):
    with Path(path).open(newline="", encoding="utf-8") as stream:
        rows = csv.DictReader(stream)
        if rows.fieldnames != ["metric", "value", "unit"]:
            raise ValueError("summary CSV has an unsupported schema")
        result = {}
        for row in rows:
            value = row["value"]
            try:
                value = float(value)
                if value.is_integer():
                    value = int(value)
            except (TypeError, ValueError):
                pass
            result[row["metric"]] = {"value": value, "unit": row["unit"]}
    return result


# Batch evaluation is deliberately separate from the launcher. No third-party
# dependencies and no analysis files are written into individual run folders.
def csv_rows(path):
    path = Path(path)
    if not path.exists():
        return
    with path.open(newline="", encoding="utf-8") as stream:
        yield from csv.DictReader(stream)


def distribution(values):
    values = sorted(values)
    result = {"samples": len(values), "mean_us": None, "p50_us": None,
              "p95_us": None, "p99_us": None, "max_us": None}
    if values:
        result.update(mean_us=sum(values) / len(values), max_us=values[-1])
        for percentile in (50, 95, 99):
            result[f"p{percentile}_us"] = values[math.ceil(percentile / 100 * len(values)) - 1]
    return result


def quantile_fields(target, name, values):
    target.update({f"{name}_{k}": v for k, v in distribution(values).items()})


def recovery_rounds(phases, config):
    """Only certify all-correct completion when every required replica is present."""
    correct = set(range(int(config["replica_count"]))) - {
        int(x) for x in str(config.get("zyzzyva_malicious_replica_ids", "0")).split(',') if x.strip()}
    rows = [r for r in phases if r["protocol"] == "zyzzyva"]
    attacks = [r for r in rows if r["phase"] == "19"]
    result = []
    for attack in attacks:
        begin = float(attack["time_us"])
        # Legacy phase traces do not associate an injection with its view.
        view = int(attack["view"]) if attack.get("view") else None
        item = {"attack_us": begin, "attack_view": view,
                "target_view": view + 1 if view is not None else None,
                "correct_replicas": len(correct), "status": "legacy-missing-view"}
        if view is None:
            result.append(item)
            continue
        next_attack = min((float(r["time_us"]) for r in attacks
                           if float(r["time_us"]) > begin), default=math.inf)
        target = view + 1
        for phase, label in (("5", "first_evidence"), ("18", "first_view_change"),
                             ("14", "all_correct_installed"), ("17", "all_correct_recovered")):
            selected = {}
            for r in rows:
                node, time = int(r["node_id"]), float(r["time_us"])
                event_view = int(r.get("view") or -1)
                if (r["phase"] != phase or node not in correct or not begin <= time < next_attack
                        or event_view != (view if phase == "5" else target)):
                    continue
                selected[node] = min(selected.get(node, math.inf), time)
            all_required = label.startswith("all_correct")
            available = selected and (not all_required or set(selected) == correct)
            value = (max(selected.values()) if all_required else min(selected.values())) if available else None
            item[label + "_us"] = value
            item[label + "_latency_us"] = value - begin if value is not None else None
            item[label + "_replicas_observed"] = len(selected)
        item["status"] = "recovered" if item["all_correct_recovered_us"] is not None else "incomplete-before-stop-or-next-attack"
        installed, started = item["all_correct_installed_us"], item["first_view_change_us"]
        item["view_change_duration_us"] = installed - started if installed is not None and started is not None else None
        recovered = item["all_correct_recovered_us"]
        item["recovery_after_install_us"] = recovered - installed if recovered is not None and installed is not None else None
        result.append(item)
    return result


def throughput_samples(events, stop_us, sample_us=10000, window_us=250000):
    """Trailing interval is (t-window,t]; only full observed windows are emitted."""
    events = sorted(events)
    left = right = total = 0
    for time in range(((window_us + sample_us - 1) // sample_us) * sample_us,
                      int(stop_us) + 1, sample_us):
        while right < len(events) and events[right][0] <= time:
            total += events[right][1]
            right += 1
        while left < right and events[left][0] <= time - window_us:
            total -= events[left][1]
            left += 1
        yield time, total, total * 1e6 / window_us


def write_csv(path, rows):
    rows = list(rows)
    fields = list(dict.fromkeys(key for row in rows for key in row)) or ["run_id", "status"]
    with Path(path).open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def background_metrics(prefix):
    """Disk-backed flow matching avoids retaining millions of Hadoop flows in RAM."""
    import sqlite3
    import tempfile
    event_path = Path(str(prefix) + '-background-events.csv')
    aggregate_path = Path(str(prefix) + '-background-summary.csv')
    if aggregate_path.exists() and next(iter(csv_rows(event_path)), None) is None:
        result = []
        for row in csv_rows(aggregate_path):
            item = dict(row)
            for key in ('started', 'completed', 'incomplete'):
                item[key] = int(item[key])
            for key in ('completion_fraction', 'mean_us', 'max_us', 'p50_us', 'p95_us', 'p99_us'):
                item[key] = float(item[key])
            item['samples'] = item['completed']
            item['status'] = 'aggregate-only' if item['started'] else 'no-started-flows'
            result.append(item)
        return result
    if not event_path.exists():
        values = [float(r['fct_us']) for r in csv_rows(str(prefix) + '-background.csv')]
        return [{"size_class": "all", "status": "legacy-completions-only",
                 "started": None, "completed": len(values), "completion_fraction": None,
                 **distribution(values)}]
    with tempfile.TemporaryDirectory(prefix='consensus-flows-') as directory:
        with sqlite3.connect(str(Path(directory) / 'flows.sqlite')) as db:
            db.execute('PRAGMA journal_mode=OFF')
            db.execute('CREATE TABLE flows(sender TEXT,receiver TEXT,id TEXT,bytes INTEGER,start INTEGER,finish INTEGER,fct REAL,PRIMARY KEY(sender,receiver,id)) WITHOUT ROWID')
            for row in csv_rows(event_path):
                key = (row['sender_id'], row['receiver_id'], row['flow_id'])
                time = int(row['time_ns'])
                if row['event'] == 'start':
                    db.execute('INSERT INTO flows(sender,receiver,id,bytes,start) VALUES(?,?,?,?,?)',
                               (*key, int(row['bytes']), time))
                elif row['event'] == 'complete':
                    if db.execute('UPDATE flows SET finish=?,fct=? WHERE sender=? AND receiver=? AND id=? AND finish IS NULL',
                                  (time, int(row['fct_ns']) / 1000, *key)).rowcount != 1:
                        raise ValueError('background completion without a unique start')
                else:
                    raise ValueError('unknown background event')
            if db.execute('SELECT COUNT(*) FROM flows WHERE finish<start OR fct<0').fetchone()[0]:
                raise ValueError('negative background FCT')
            db.execute('CREATE INDEX fct_index ON flows(fct)')
            result = []
            for label, condition in (('all', '1'), ('small_le_100KiB', 'bytes<=102400'),
                                     ('medium_100KiB_to_10MiB', 'bytes>102400 AND bytes<=10485760'),
                                     ('large_gt_10MiB', 'bytes>10485760')):
                count, complete, mean, maximum = db.execute(
                    f'SELECT COUNT(*),COUNT(fct),AVG(fct),MAX(fct) FROM flows WHERE {condition}').fetchone()
                item = {"size_class": label, "status": "available" if count else "no-started-flows",
                        "started": count, "completed": complete, "incomplete": count-complete,
                        "completion_fraction": complete/count if count else None,
                        "mean_us": mean, "max_us": maximum}
                for percentile in (50, 95, 99):
                    item[f'p{percentile}_us'] = db.execute(
                        f'SELECT fct FROM flows WHERE {condition} AND fct IS NOT NULL ORDER BY fct LIMIT 1 OFFSET ?',
                        (max(0, math.ceil(percentile/100*complete)-1),)).fetchone()[0] if complete else None
                result.append(item)
            return result


def path_metrics(prefix, config):
    if config.get('transaction_path_events') is False and not config.get('diagnostic_events', False):
        return [{'status': 'not-recorded', 'enable_option': 'transactionPathEvents'}]
    from array import array
    from collections import deque
    pending, groups = {}, {}
    for row in csv_rows(str(prefix) + '-transaction-path.csv'):
        if not row.get('message_id') or not row.get('time_ns'):
            return [{"status": "legacy-missing-message-identity"}]
        if row['message_type'] != '30':
            continue
        sender, receiver = int(row['sender_id']), int(row['receiver_id'])
        n = int(config['replica_count'])
        ingress = {200} | (set(range(n)) if config.get('bidl_native_sequencing') else set())
        malicious = set(range(300, 300+int(config.get('bidl_malicious_node_count', 0))))
        executors = set(range(100, 100+n))
        route = ('sequencer_to_executor' if sender in ingress and receiver in executors else
                 'sequencer_to_attacker' if sender in ingress and receiver in malicious else
                 'attacker_to_executor' if sender in malicious and receiver in executors else None)
        if route is None:
            continue
        group = groups.setdefault((route, sender, receiver), {'tx': 0, 'rx': 0, 'unmatched_rx': 0, 'values': array('d')})
        key = tuple(row[k] for k in ('sender_id','receiver_id','message_type','message_id','view','sequence_number','client_id','request_id','digest'))
        time = int(row['time_ns'])
        if row['direction'] == 'tx':
            group['tx'] += 1
            pending.setdefault(key, deque()).append(time)
        elif row['direction'] == 'rx':
            group['rx'] += 1
            queue = pending.get(key)
            if not queue:
                group['unmatched_rx'] += 1
                continue
            start = queue.popleft()
            if not queue:
                del pending[key]
            if time < start:
                raise ValueError('message receive precedes send')
            group['values'].append((time-start)/1000)
    return [{"route": route, "sender_id": sender, "receiver_id": receiver,
             "status": "available", "sent": g['tx'], "received": g['rx'],
             "unmatched_receives": g['unmatched_rx'], "unmatched_sends": g['tx']-len(g['values']),
             **distribution(g['values'])} for (route,sender,receiver),g in groups.items()]


def backlog_metrics(prefix, config):
    from collections import defaultdict
    protocol = config['architecture']
    path = str(prefix) + ('-executor-backlog.csv' if protocol == 'bidl' else '-replica-backlog.csv')
    nodes = {}
    for row in csv_rows(path):
        node = int(row['node_id'])
        time = int(row['time_ns'])/1e9 if row.get('time_ns') else float(row['time_us'])/1e6
        fields = ('verification_backlog','execution_backlog','total_backlog') if protocol == 'bidl' else ('verification_messages','execution_transactions','buffered_orders')
        sample = {key:int(row[key]) for key in fields}
        state = nodes.setdefault(node, {"previous": None, "areas": defaultdict(float), "maxima": defaultdict(int), "samples": 0})
        if state['previous']:
            previous_time, previous = state['previous']
            if time < previous_time:
                raise ValueError('nonmonotone backlog samples')
            for key,value in previous.items():
                state['areas'][key] += value*(time-previous_time)
        else:
            state['start'] = time
        for key,value in sample.items():
            state['maxima'][key] = max(state['maxima'][key],value)
        state['previous'] = (time,sample)
        state['samples'] += 1
    result = []
    for node,state in nodes.items():
        stop,last = state['previous']
        duration = stop-state['start']
        item = {"node_id": node, "samples": state['samples'], "observed_start_s":state['start'], "observed_stop_s":stop}
        for key in last:
            item[key+'_mean'] = state['areas'][key]/duration if duration else None
            item[key+'_max'] = state['maxima'][key]
            item[key+'_last'] = last[key]
        result.append(item)
    return result


def evaluate_run(prefix, run_id, window_start=None, window_stop=None):
    prefix = Path(prefix)
    config = json.loads(Path(str(prefix)+'-config.json').read_text())
    manifest = json.loads(Path(str(prefix)+'-manifest.json').read_text())
    summary = read_summary(str(prefix)+'-summary.csv')
    requests = {}
    for request in csv_rows(str(prefix)+'-request-events.csv'):
        key = (int(request['client_id']), int(request['request_id']))
        if key in requests:
            raise ValueError('duplicate client/request identity in request events')
        if request['status'] not in ('complete', 'incomplete', 'aborted') or int(request['transaction_count']) < 1:
            raise ValueError('invalid request status or transaction weight')
        if request['status'] == 'complete' and not request.get('completed_us'):
            raise ValueError('completed request has no completion timestamp')
        requests[key] = request
    expected = int(manifest.get('expected_requests',0))
    stop = float(config['simulation_time'])*1e6
    start_window = float(window_start if window_start is not None else manifest['measurement_start'])*1e6
    stop_window = float(window_stop if window_stop is not None else manifest['measurement_stop'])*1e6
    if not 0 <= start_window < stop_window <= stop:
        raise ValueError('analysis window must lie inside the observation interval')
    complete = {key:r for key,r in requests.items() if r['status']=='complete'}
    submissions = [float(r['submitted_us']) for r in requests.values()]
    completions = [(float(r['completed_us']),int(r['transaction_count'])) for r in complete.values()]
    if any(float(r['completed_us'])<float(r['submitted_us']) for r in complete.values()):
        raise ValueError('negative client completion latency')
    result = {"run_id":run_id, "architecture":config['architecture'], "scenario":config.get('scenario'),
              "topology":Path(config.get('topology_file','')).name,
              "flow_file":Path(config.get('flow_file','')).name,
              "cc_mode":config.get('rdma_cc_mode'), "request_rate":config.get('request_rate'),
              "iors_mode":config.get('iors_mode'), "iors_terminal_gate":config.get('iors_terminal_gate'),
              "seed":config.get('seed'), "run":config.get('run'),
              "protocol_status":manifest.get('status'), "confirmed_safety_error":manifest.get('confirmed_safety_error'),
              "expected_requests":expected, "submitted_requests":len(requests), "completed_requests":len(complete),
              "incomplete_or_aborted_requests":max(0,expected-len(complete)),
              "completion_fraction":len(complete)/expected if expected else None,
              "observation_truncated":len(complete)!=expected,
              "window_start_s":start_window/1e6,"window_stop_s":stop_window/1e6}
    total = sum(weight for _,weight in completions)
    duration = max((t for t,_ in completions),default=0)-min(submissions,default=0)
    result['client_completed_transactions'] = total
    result['observed_client_completion_tps'] = total*1e6/duration if duration>0 else None
    result['full_workload_client_tps'] = result['observed_client_completion_tps'] if len(complete)==expected and expected else None
    result['window_client_completion_tps'] = sum(weight for t,weight in completions if start_window<=t<stop_window)*1e6/(stop_window-start_window)
    cohort = [r for r in requests.values() if start_window<=float(r['submitted_us'])<stop_window]
    quantile_fields(result,'client_latency',[float(r['completed_us'])-float(r['submitted_us']) for r in cohort if r['status']=='complete'])
    result['latency_cohort_submitted'] = len(cohort)
    result['latency_cohort_incomplete'] = sum(r['status']!='complete' for r in cohort)
    route_known = all(r.get('completion_path') for r in complete.values()) and bool(complete)
    fast = sum(r.get('completion_path')=='fast' for r in complete.values())
    slow = sum(r.get('completion_path')=='slow' for r in complete.values())
    result['fast_path_success_fraction_completed'] = fast/len(complete) if route_known and config['architecture']=='zyzzyva' else None
    result['fast_path_success_fraction_offered'] = fast/expected if route_known and config['architecture']=='zyzzyva' and expected else None
    result['slow_path_completed_requests'] = slow if route_known and config['architecture']=='zyzzyva' else None
    finalized = {}
    finalpath=Path(str(prefix)+'-transaction-finalized.csv')
    for row in csv_rows(finalpath):
        if int(row['node_id']) not in range(100,100+int(config['replica_count'])):
            raise ValueError('finalization from an unconfigured executor')
        key=(int(row['client_id']),int(row['request_id']))
        state=finalized.setdefault(key,{"times":{},"transactions":int(row['transaction_count'])})
        if state['transactions'] != int(row['transaction_count']):
            raise ValueError('replicas disagree on finalized request weight')
        node=int(row['node_id']);time=int(row['time_ns'])/1000
        state['times'][node]=min(state['times'].get(node,math.inf),time)
    committed, all_committed, commit_latency, all_latency = [], [], [], []
    required=set(range(100,100+int(config['replica_count'])))
    for key,state in finalized.items():
        first=min(state['times'].values()); weight=state['transactions']
        committed.append((first,weight))
        request=requests.get(key)
        if request and start_window<=float(request['submitted_us'])<stop_window:
            latency=first-float(request['submitted_us'])
            if latency<0: raise ValueError('commit precedes client submission')
            commit_latency.append(latency)
        if required <= state['times'].keys():
            last=max(state['times'][node] for node in required)
            all_committed.append((last,weight))
            if request and start_window<=float(request['submitted_us'])<stop_window:
                all_latency.append(last-float(request['submitted_us']))
    result['executor_commit_status']='available' if finalpath.exists() and config['architecture']=='bidl' else 'not-recorded-or-not-applicable'
    result['unique_requests_first_executor_committed']=len(committed) if config['architecture']=='bidl' and finalpath.exists() else None
    result['unique_requests_all_executors_committed']=len(all_committed) if config['architecture']=='bidl' and finalpath.exists() else None
    for label,events in (('first_executor_commit',committed),('all_executor_commit',all_committed)):
        result['window_'+label+'_tps']=sum(w for t,w in events if start_window<=t<stop_window)*1e6/(stop_window-start_window) if config['architecture']=='bidl' and finalpath.exists() else None
    quantile_fields(result,'client_to_first_executor_commit',commit_latency)
    quantile_fields(result,'client_to_all_executor_commit',all_latency)
    metrics={k:v['value'] for k,v in summary.items()}
    for key in ('bidl_reexecution_events','bidl_reexecuted_transactions','bidl_affected_blocks',
                'iors_bidl_marked_waits','iors_bidl_marked_replacements','iors_bidl_marked_timeout_fallbacks',
                'iors_bidl_marked_branches_avoided_execution','retransmissions_full_cycle','timed_out_requests_full_cycle'):
        result[key]=metrics.get(key)
    phases=list(csv_rows(str(prefix)+'-phases.csv'))
    bidl_commits={(r['node_id'],r['instance_id']) for r in phases if r['protocol']=='bidl' and r['phase']=='4'}
    reexec_pairs={(r['node_id'],r['block_id']) for r in csv_rows(str(prefix)+'-bidl-reexecution-events.csv')}
    result['bidl_reexecuted_fraction_committed_executor_blocks']=len(bidl_commits & reexec_pairs)/len(bidl_commits) if bidl_commits else None
    result['bidl_reexecution_fraction_status']='available' if bidl_commits else 'no-committed-executor-blocks'
    for key in ('zyzzyva_rollbacks','zyzzyva_reexecuted_requests','zyzzyva_speculative_executions','zyzzyva_cancelled_execution_tasks',
                'iors_observed','iors_marked','iors_capacity_bypass','iors_collision_bypass','iors_expired'):
        result[key]=manifest.get(key)
    correct=set(range(int(config['replica_count'])))-{int(x) for x in str(config.get('zyzzyva_malicious_replica_ids','0')).split(',') if x.strip()}
    executions=[r for r in phases if r['protocol']=='zyzzyva' and r['phase']=='1' and int(r['node_id']) in correct]
    reexecutions=[r for r in phases if r['protocol']=='zyzzyva' and r['phase']=='11' and int(r['node_id']) in correct]
    result['zyzzyva_correct_reexecution_events_per_execution_event']=len(reexecutions)/len(executions) if executions else None
    for phase,label in (('4','rollback'),('11','reexecution'),('12','cancelled_execution')):
        result['zyzzyva_correct_'+label+'_events']=sum(r['protocol']=='zyzzyva' and r['phase']==phase and int(r['node_id']) in correct for r in phases)
    # These are modeled CPU service demands summed over honest replicas, not wall time.
    single_transaction = int(config.get('transactions_per_request', 1)) == 1
    execution_us = config.get('execution_time_per_transaction_us')
    for event, label in (('rollback', 'invalidated_speculation'), ('reexecution', 'reexecution'),
                         ('cancelled_execution', 'cancelled_execution')):
        result['zyzzyva_correct_'+label+'_modeled_cpu_service_us'] = (
            result['zyzzyva_correct_'+event+'_events']*float(execution_us)
            if config['architecture']=='zyzzyva' and single_transaction and execution_us is not None else None)
    message_rows=list(csv_rows(str(prefix)+'-messages.csv'))
    result['consensus_app_tx_messages']=sum(int(r['full_cycle_count']) for r in message_rows)
    result['consensus_app_tx_bytes']=sum(int(r['full_cycle_bytes']) for r in message_rows)
    result['consensus_app_tx_bytes_per_completed_transaction']=result['consensus_app_tx_bytes']/total if total else None
    result['recovery_control_tx_bytes']=sum(int(r['full_cycle_bytes']) for r in message_rows if int(r['message_type']) in (13,14,42,43,44,45,46,47,60,62,63,64,66))
    network=list(csv_rows(str(prefix)+'-network.csv'))
    for source,dest in (('full_cycle_drops','network_device_drop_events'),
                        ('full_cycle_pfc_pause','network_pfc_pause_events'),
                        ('full_cycle_pfc_resume','network_pfc_resume_events')):
        result[dest]=sum(int(r[source]) for r in network if r.get(source)) if network else None
    result['maximum_measured_device_utilization']=max((float(r['utilization']) for r in network if r.get('utilization')),default=None)
    rounds=recovery_rounds(phases,config) if config['architecture']=='zyzzyva' else []
    recovery_messages=list(csv_rows(str(prefix)+'-transaction-path.csv')) if rounds else []
    switch_events=list(csv_rows(str(prefix)+'-iors-events.csv')) if rounds else []
    for item in rounds:
        begin=item['attack_us']
        end=min((r['attack_us'] for r in rounds if r['attack_us']>begin),default=stop)
        view=item['attack_view']
        for name,times in (
            ('first_client_pom_tx', [float(r['time_us']) for r in recovery_messages
                if r['direction']=='tx' and r['message_type']=='64' and r.get('view')==str(view) and begin<=float(r['time_us'])<end]),
            ('first_switch_alert', [float(r['time_us']) for r in switch_events
                if r.get('protocol')=='3' and r.get('epoch')==str(view) and begin<=float(r['time_us'])<end])):
            value=min(times,default=None)
            item[name+'_us']=value
            item[name+'_latency_us']=value-begin if value is not None else None
        finish=item.get('all_correct_recovered_us')
        item['first_client_completion_after_recovery_us']=min((t for t,_ in completions if t>=finish),default=None) if finish is not None else None
    result['expected_attack_injected']=(int(manifest.get('zyzzyva_attacks_injected',0))>0 if config.get('zyzzyva_equivocation_enabled') else
                                        metrics.get('bidl_forged_transactions',0)>0 if config.get('bidl_attack_enabled') else None)
    result['evaluation_status'] = ('safety-error' if result['confirmed_safety_error'] else
                                   'attack-not-injected' if result['expected_attack_injected'] is False else
                                   'censored' if result['observation_truncated'] else 'complete')
    status_path = prefix.parent/'run-status.json'
    if status_path.exists():
        status = json.loads(status_path.read_text())
        for field in ('process_exit_code', 'host_elapsed_seconds', 'host_child_cpu_user_seconds',
                      'host_child_cpu_system_seconds', 'host_peak_rss_kib'):
            result[field] = status.get(field)
    return result, {'client_completion':completions, **({'first_executor_commit':committed,'all_executor_commit':all_committed} if config['architecture']=='bidl' and finalpath.exists() else {})}, rounds, config


def main():
    import argparse
    import hashlib
    import sqlite3
    parser=argparse.ArgumentParser(description='Evaluate a completed consensus batch without launching simulations.')
    parser.add_argument('inputs',nargs='+',type=Path,help='Run or campaign directories; discovered recursively')
    parser.add_argument('--output',required=True,type=Path,help='New analysis directory')
    parser.add_argument('--window-start',type=float,help='Absolute seconds; independent of request injection')
    parser.add_argument('--window-stop',type=float,help='Absolute seconds, exclusive')
    parser.add_argument('--sample-ms',type=float,default=10)
    parser.add_argument('--rolling-ms',type=float,default=250)
    args=parser.parse_args()
    if (args.window_start is None)!=(args.window_stop is None):
        parser.error('supply both --window-start and --window-stop')
    if not math.isfinite(args.sample_ms) or not math.isfinite(args.rolling_ms) or args.sample_ms<.001 or args.rolling_ms<.001:
        parser.error('sampling and rolling intervals must be finite and at least 0.001ms')
    manifests=sorted({p.resolve() for root in args.inputs for p in root.rglob('*-manifest.json')} |
                     {p.parent.resolve()/'run-manifest.json' for root in args.inputs for p in root.rglob('run-status.json')})
    if not manifests:
        parser.error('no native manifests or launcher run-status.json files found')
    output=args.output.resolve()
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        parser.error('output directory is nonempty; choose a new analysis directory')
    output.mkdir(parents=True,exist_ok=True)
    summary, recoveries, paths, backgrounds, backlogs, errors, provenance=[],[],[],[],[],[],[]
    # When a campaign manifest is available, a never-started task must not disappear.
    for root in args.inputs:
        campaign = root/'tasks.json'
        if not campaign.exists() and root.name == 'collected':
            campaign = root.parent/'tasks.json'
        if not campaign.exists():
            continue
        planned = json.loads(campaign.read_text())
        observed = {part for manifest in manifests for part in manifest.parts if part.startswith('task-')}
        for task in planned.get('tasks', []):
            name = f"task-{int(task['task_id']):05d}"
            if name not in observed:
                errors.append({'run_id': str(campaign.parent/name), 'error': 'planned task has no run artifacts'})
    with (output/'throughput.csv').open('w',newline='') as stream:
        writer=csv.writer(stream)
        writer.writerow(['run_id','basis','time_s','window_s','committed_or_completed_transactions','transactions_per_second'])
        for manifest in manifests:
            prefix=Path(str(manifest)[:-len('-manifest.json')])
            run_id=str(prefix.parent)
            print(f'Analyzing {run_id}',flush=True)
            try:
                row,curves,rounds,config=evaluate_run(prefix,run_id,args.window_start,args.window_stop)
                # Finish fallible readers before including the run in output tables.
                run_paths=path_metrics(prefix,config) if config['architecture']=='bidl' else []
                run_background=background_metrics(prefix)
                run_backlogs=backlog_metrics(prefix,config)
                for basis,events in curves.items():
                    for time,count,rate in throughput_samples(events,float(config['simulation_time'])*1e6,
                                                               round(args.sample_ms*1000),round(args.rolling_ms*1000)):
                        writer.writerow([run_id,basis,time/1e6,args.rolling_ms/1000,count,rate])
                summary.append(row)
                for destination,items in ((recoveries,rounds),(paths,run_paths),(backgrounds,run_background),(backlogs,run_backlogs)):
                    destination.extend({'run_id':run_id,**item} for item in items)
                provenance.append({'run_id':run_id,'config_sha256':hashlib.sha256(Path(str(prefix)+'-config.json').read_bytes()).hexdigest(),
                                   'manifest_sha256':hashlib.sha256(manifest.read_bytes()).hexdigest()})
            except (OSError,ValueError,KeyError,TypeError,sqlite3.Error) as exc:
                errors.append({'run_id':run_id,'error':str(exc)})
    for name,rows in (('summary',summary),('recovery',recoveries),('paths',paths),('background',backgrounds),('backlog',backlogs)):
        write_csv(output/(name+'.csv'),rows)
    metadata={'schema_version':2,'sample_ms':args.sample_ms,'rolling_ms':args.rolling_ms,
              'rolling_interval':'(t-window,t], full windows only', 'fixed_interval':'[start,stop)',
              'latency_cohort':'submitted in fixed interval; incomplete requests excluded from quantiles and counted separately',
              'client_completion':'protocol client quorum; Zyzzyva fast and slow paths distinguished',
              'executor_commit':'BIDL first/all configured executors; unique client/request identities',
              'background_fct':'application submission to full receive; completed-only quantiles plus incomplete counts',
              'backlog_mean':'time-weighted over recorded sample coverage; verification messages and execution transactions are distinct units',
              'missing_values':'empty CSV fields mean unavailable, never zero',
              'analyzer_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'runs':provenance,'errors':errors,
              'references':['https://www.cs.cornell.edu/lorenzo/papers/a7-kotla.pdf',
                            'https://www.usenix.org/conference/nsdi-08/bft-protocols-under-fire',
                            'https://www.microsoft.com/en-us/research/wp-content/uploads/2023/03/RDMA_Experience_Paper_TR-1.pdf']}
    (output/'analysis.json').write_text(json.dumps(metadata,indent=2,allow_nan=False)+'\n')
    print(f'Analyzed {len(summary)} runs; {len(errors)} errors. Output: {output}')
    return 1 if errors else 0


if __name__=='__main__':
    raise SystemExit(main())
