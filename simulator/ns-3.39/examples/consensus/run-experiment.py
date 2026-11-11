#!/usr/bin/env python3
"""Run exactly one consensus experiment and save its artifacts."""
import argparse
import json
import os
import resource
import re
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time



def read_config(path):
    values = {}
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if not separator or not key:
            raise ValueError(f"invalid .conf entry at line {number}: expected key=value")
        if key in values:
            raise ValueError(f"duplicate .conf key at line {number}: {key}")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key] = value
    return values


def read_overrides(tokens):
    values = {}
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if not token.startswith("--") or token == "--":
            raise ValueError(f"expected --parameter=value, got {token!r}")
        key, separator, value = token[2:].partition("=")
        if not separator:
            index += 1
            if index >= len(tokens) or tokens[index].startswith("--"):
                raise ValueError(f"--{key} requires an explicit value (use true/false for booleans)")
            value = tokens[index]
        if key in values:
            raise ValueError(f"duplicate override: --{key}")
        if not key or any(c in key + value for c in "\n\r\0"):
            raise ValueError("invalid parameter key or multiline value")
        values[key] = value
        index += 1
    return values



def classify_exit(exit_code, manifest):
    """Native code 1 denotes an incomplete workload; other failures stay failures."""
    if manifest.get("confirmed_safety_error", False):
        return 1
    if exit_code == 0 and manifest.get("status") == "complete":
        return 0
    if exit_code == 1 and manifest.get("status") == "incomplete":
        return 3
    return 1


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, allow_abbrev=False,
        epilog="Append ns-3 experiment overrides, e.g. --rdmaCcMode=3 --requestRate=20000. "
               "Use --flowFile= to disable file replay. Original configuration is not modified.")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--seed", required=True, type=int)
    parser.add_argument("--run", type=int, help="ns-3 RNG run/substream; defaults to the config value or 1")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--diagnostic-events", action="store_true")
    args, tokens = parser.parse_known_args()
    if not 1 <= args.seed <= 2**32 - 1:
        parser.error("--seed must be between 1 and 4294967295")
    config = args.config.resolve()
    if not config.is_file():
        parser.error(f"configuration file does not exist: {config}")
    try:
        values = read_config(config)
        overrides = read_overrides(tokens)
        if args.diagnostic_events:
            if overrides.get("diagnosticEvents", "true").lower() not in ("true", "1"):
                raise ValueError("--diagnostic-events conflicts with --diagnosticEvents=false")
            overrides["diagnosticEvents"] = "true"
        reserved = {"configFile", "outputPrefix", "batchRuns", "writeIndividualResults"}
        if reserved.intersection(overrides):
            raise ValueError("use --config/--output for paths; batchRuns is unsupported by the single-run launcher")
        if {"configFile", "batchRuns", "writeIndividualResults"}.intersection(values):
            raise ValueError("base config cannot contain configFile, batchRuns or writeIndividualResults")
        values.update(overrides)
        run_number = args.run if args.run is not None else int(values.get("run", "1"))
        if not 0 <= run_number <= 2**64 - 1:
            raise ValueError("--run must be between 0 and 18446744073709551615")
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    output = args.output.resolve()
    binary = (args.binary or Path(__file__).resolve().parents[2] / "build/examples/consensus/ns3.39-bidl-experiment-optimized").resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error(f"simulator binary is missing or not executable: {binary}")
    ns3_root = Path(__file__).resolve().parents[2]
    try:
        # Use the installed executable's parameter names, rather than a second
        # Python list that can drift from ConsensusExperimentConfig.
        help_command = [str(binary)]
        if values.get("preset"):
            help_command.append(f"--preset={values['preset']}")
        help_result = subprocess.run([*help_command, "--help"], cwd=ns3_root,
                                     capture_output=True, text=True, timeout=30, check=True)
        help_text = (help_result.stdout + help_result.stderr).split("General Arguments:")[0]
        options = dict(re.findall(r"^\s+--([A-Za-z][A-Za-z0-9_]*):([^\n]*)", help_text, re.MULTILINE))
        supported = set(options)
        if not supported:
            raise ValueError("cannot read experiment parameters from simulator --help")
        unknown = set(values) - supported
        if unknown:
            raise ValueError("unknown experiment parameter(s): " + ", ".join(sorted(unknown)))
        # ns-3's generic string parser rejects an empty value. Clearing an
        # empty-default string therefore means omitting the merged entry, so
        # the selected preset supplies its empty default. Numeric/boolean and
        # nonempty-default options must still receive an explicit valid value.
        cleared = {key for key, value in values.items() if value == ""}
        for key in cleared:
            if re.search(r"\[[^\]]*\]\s*$", options[key]):
                raise ValueError(f"--{key} cannot be cleared; supply an explicit value")
            del values[key]
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        parser.error(str(exc))
    if any(c.isspace() for c in str(output)):
        parser.error("output path cannot contain whitespace (simulator outputPrefix limitation)")
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        print(f"refusing to overwrite non-empty output directory: {output}", file=sys.stderr)
        return 2
    shutil.copy2(config, output / "base.conf")
    prefix = output / "run"
    values.update(seed=str(args.seed), run=str(run_number), outputPrefix=str(prefix))
    effective_config = output / "input.conf"
    effective_config.write_text("".join(f"{key}={values[key]}\n" for key in
                                      sorted(values, key=lambda key: (key != "preset", key))), encoding="utf-8")
    (output / "overrides.json").write_text(json.dumps({
        **overrides, "seed": str(args.seed), "run": str(run_number), "outputPrefix": str(prefix)
    }, indent=2) + "\n", encoding="utf-8")
    command = [str(binary), f"--configFile={effective_config}"]
    (output / "command.json").write_text(json.dumps(command, indent=2) + "\n", encoding="utf-8")
    start = time.monotonic()
    launch_error = None
    try:
        with (output / "console.log").open("w", encoding="utf-8") as log:
            child = subprocess.Popen(command, cwd=ns3_root, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            def forward_signal(signum, _frame):
                try:
                    os.killpg(child.pid, signum)
                except ProcessLookupError:
                    pass
            signal.signal(signal.SIGTERM, forward_signal)
            signal.signal(signal.SIGINT, forward_signal)
            exit_code = child.wait()
    except OSError as exc:
        exit_code = 127
        launch_error = str(exc)
    elapsed = time.monotonic() - start
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    # Preserve raw artifacts only. Evaluation is an explicit, batch-level operation.
    status = {
        "schema_version": 1, "process_exit_code": exit_code,
        "host_elapsed_seconds": elapsed,
        "host_child_cpu_user_seconds": usage.ru_utime,
        "host_child_cpu_system_seconds": usage.ru_stime,
        "host_peak_rss_kib": int(usage.ru_maxrss),
        "analysis_status": "not-run",
    }
    code = 1
    try:
        manifest = json.loads(Path(str(prefix) + "-manifest.json").read_text())
        status["protocol_status"] = manifest.get("status")
        status["confirmed_safety_error"] = manifest.get("confirmed_safety_error", False)
        code = classify_exit(exit_code, manifest)
    except (OSError, ValueError) as exc:
        status["artifact_error"] = str(exc)
    if launch_error is not None:
        status["launch_error"] = launch_error
    status["launcher_exit_code"] = code
    (output / "run-status.json").write_text(json.dumps(status, indent=2) + "\n")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
