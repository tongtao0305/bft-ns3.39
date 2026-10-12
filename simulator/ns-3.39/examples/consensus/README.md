# BFT consensus experiments

This directory is the runnable, reproducible BFT experiment package.  It is
separate from `src/consensus`, which contains only the reusable ns-3 consensus
module.

## Layout

- `bidl-experiment.cc`: experiment entry point built by CMake; it is the
  common PBFT/BIDL runner.
- `configs/baseline/`: small, documented configurations suitable for local
  validation and tutorial runs.
- `configs/paper/`: frozen parameter sets used by dated experiment campaigns.
- `topologies/`: file-backed Qbb/Fat-tree/leaf-spine topologies.
- `workloads/`: flow traces and external-workload inputs.
- `scripts/local/`: local campaign launchers and status helpers.
- `scripts/slurm/`: Slurm submission and array-task launchers.

Run from the ns-3 root with a configuration path, for example:

```sh
./ns3 run "bidl-experiment --configFile=examples/consensus/configs/baseline/bidl-k4-fat-tree.conf"
```

Output belongs in `results/` or `run-logs/`, never in this directory.  Paths in
configuration files are relative to the ns-3 root; launchers change to that
directory before invoking the binary.
