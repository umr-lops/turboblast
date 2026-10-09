# turboblast

![turboblast logo](docs/_static/logo.png)

[![Actions Status][actions-badge]][actions-link]
[![Documentation Status][rtd-badge]][rtd-link]

[![PyPI version][pypi-version]][pypi-link]
[![Conda-Forge][conda-badge]][conda-link]
[![PyPI platforms][pypi-platforms]][pypi-link]

[![GitHub Discussion][github-discussions-badge]][github-discussions-link]

[![Coverage][coverage-badge]][coverage-link]

<!-- SPHINX-START -->

## Purpose

**turboblast** is a Python library for submitting high-throughput job arrays to
a [Slurm](https://slurm.schedmd.com/) cluster using
[submitit](https://github.com/facebookincubator/submitit). It is designed for
workflows where you have a large list of command-line tasks (e.g. processing
satellite files) that need to be distributed across many compute nodes in
parallel.

The core idea is simple: you provide a text file where each line is a set of
arguments, and turboblast dispatches each line as an independent Slurm task
running a bash script of your choice. Large input lists are automatically split
into chunks of 1000 to stay within Slurm array limits and are submitted as a
pipeline (several job arrays in flight at a time) so the cluster stays busy.

### Dependencies

| Package                                                   | Role                                              |
| --------------------------------------------------------- | ------------------------------------------------- |
| [submitit](https://github.com/facebookincubator/submitit) | Submits and monitors Slurm job arrays from Python |
| Python ≥ 3.10                                             | Required runtime                                  |

## Installation

```bash
pip install turboblast
```

Or with conda:

```bash
conda install -c conda-forge turboblast
```

## Usage

### Prepare your inputs

Create a plain text file where each line contains the arguments for one task:

```
# inputs.txt
--input /data/file_001.nc --output /results/
--input /data/file_002.nc --output /results/
--input /data/file_003.nc --output /results/
```

### Write your bash script

turboblast will call `bash your_script.sh <args>` for each line. Example:

```bash
#!/bin/bash
# process.sh
python my_processor.py "$@"
```

### Submit the job array

```bash
turboblaster \
  --listing-input inputs.txt \
  --bash-slurm-exec process.sh \
  --slurm-partition gpu \
  --timeout-min 60 \
  --mem 8G \
  --cpus-per-task 4 \
  --slurm-array-parallelism 50 \
  --output-dir submitit_logs
```

### Full CLI reference

```
usage: turboblaster [-h] [--num-tasks NUM_TASKS] [--timeout-min TIMEOUT_MIN]
                    [--mem MEM] [--cpus-per-task CPUS_PER_TASK]
                    [--slurm-partition SLURM_PARTITION]
                    --listing-input LISTING_INPUT
                    --bash-slurm-exec BASH_SLURM_EXEC
                    [--output-dir OUTPUT_DIR]
                    [--slurm-array-parallelism SLURM_ARRAY_PARALLELISM]
                    [--fail-fast] [--max-arrays-inflight MAX_ARRAYS_INFLIGHT]
                    [--batch-stall-timeout-min BATCH_STALL_TIMEOUT_MIN]
                    [--task-stuck-min TASK_STUCK_MIN]
                    [--batch-wall-timeout-min BATCH_WALL_TIMEOUT_MIN]

options:
  --listing-input            Path to a file containing input lines (one task per line) [required]
  --bash-slurm-exec          Path to the bash script to execute for each task [required]
  --num-tasks                Number of tasks (unused if reading from file) [default: 20]
  --timeout-min              Timeout in minutes for each task [default: 20]
  --mem                      Memory per task, integer with optional unit suffix (M/MB/G/GB) [default: 2G]
  --cpus-per-task            Number of CPUs per task [default: 1]
  --slurm-partition          Slurm partition to use [default: cpu]
  --output-dir               Directory to store submitit logs [default: submitit_logs_array]
  --slurm-array-parallelism  Max number of tasks running concurrently [default: 20]
  --fail-fast                Stop submitting further arrays if any task in a batch fails
  --max-arrays-inflight      Job arrays kept in flight at once (pipelining) [default: 2]
  --batch-stall-timeout-min  No-progress backstop, minutes; 0 = disabled [default: --timeout-min + 15]
  --task-stuck-min           Per-task timeout (min) for tasks in HELD/REQUEUE_HOLD/COMPLETING [default: 15]
  --batch-wall-timeout-min   Hard cap on total batch wall time (min); 0 = disabled [default: auto per chunk]
```

Submitit logs (`.out` files) are written to a timestamped subdirectory under
`--output-dir`:

```
submitit_logs/
└── 20260309T143000/
    ├── 12345_0_0_log.out
    ├── 12345_1_0_log.out
    └── ...
```

Monitor a specific task with:

```bash
tail -f submitit_logs/20260309T143000/12345_0_0_log.out
```

### Monitor your running jobs

`slurm-monitor` renders a live, full-screen table of your active Slurm job
arrays (running / pending / completing / success / failed / total / done %). It
refreshes every 2 s by default and quits on `Ctrl-C`.

```bash
# all of your active job arrays
slurm-monitor

# follow a single (array) job
slurm-monitor 123456

# custom refresh interval (seconds)
slurm-monitor --interval 5
```

### Analyze submitit logs

`slurm-logs` summarises a submitit log directory (the timestamped folder under
`--output-dir`): how many tasks succeeded or failed, plus a breakdown of
failures by likely cause (permission, OOM/killed, Python exceptions,
socket/Slurm, apptainer).

```bash
slurm-logs submitit_logs/20260309T143000
```

### Throughput & tuning

The progress bar is **not** a measure of task speed. It shows an **aggregate**
rate, `s/task = elapsed / completed`, not the per-task wall time. With
`P = --slurm-array-parallelism` tasks running in parallel, a task that actually
takes `T` seconds shows as `T / P s/task`.

Example: 1000 tasks, each ~83 s (apptainer launch + work), `P = 20`:

```
164/1000 [12:00<57:41, 4.14s/task, running=20, pending=816]
```

`4.14 s/task` is `12:00 / 164`; the real per-task time is `4.14 × 20 ≈ 83 s`.
Nothing is stuck — 20 tasks run, finish, and 20 more are allocated.

Tuning:

- **`--slurm-array-parallelism` (default 20)** caps concurrency. Throughput =
  `parallelism / per-task duration`. For short tasks, 20 is often far too low (a
  1000-task batch of ~3 s tasks takes ~2.5 h). Raise it (e.g. 50–100) to
  saturate the cluster, subject to node capacity and `MaxJobCount`.
- Submission is **pipelined**: up to `--max-arrays-inflight` (default 2) job
  arrays are in flight at once, and the next chunk is submitted as soon as a
  slot frees — the cluster stays busy instead of idle while the last tasks of a
  chunk finish. Keep `--max-arrays-inflight` under the cluster's `MaxJobCount`;
  overall concurrency = `--max-arrays-inflight` × `--slurm-array-parallelism`.
- `main()` blocks until all tasks finish; run long jobs under `tmux`/`nohup`.
- Chunks of 1000 stay under the cluster `MaxArraySize` (1001).

### Stuck-task protection

A task stuck in a non-running state (e.g. `PENDING`, `HELD`, `REQUEUE_HOLD`) no
longer blocks the whole run. Each array is protected by three layers:

- **Per-task stuck timeout** (`--task-stuck-min`, default 15): a task sitting in
  a hold/transient state (HELD / REQUEUE_HOLD / REQUEUED / COMPLETING / STOPPED)
  that long is cancelled by its own id; `RUNNING` tasks are bounded by
  `--timeout-min + 15`.
- **No-progress backstop** (`--batch-stall-timeout-min`, default
  `--timeout-min + 15`): if _no_ task reaches a terminal state for that long
  (e.g. 999 done, 1 stuck), the remaining tasks are cancelled and the batch
  moves on.
- **Batch wall-time cap** (`--batch-wall-timeout-min`, auto per chunk): a hard
  maximum wall time per batch, so no single batch can stall the chain.

## Project structure

```
turboblast/
├── src/
│   └── turboblast/
│       ├── __init__.py        # Package entry point, exposes __version__
│       ├── blaster.py         # Core logic: argument parsing, job submission, task execution
│       ├── monitor.py         # Live Slurm job array monitor (`slurm-monitor`)
│       ├── log_analysis.py    # Submitit logs analysis (`slurm-logs`)
│       └── logo.py            # ASCII art logo used in the CLI help message
├── tests/
│   ├── test_package.py        # Package metadata tests (version check)
│   ├── test_blaster.py        # Unit tests for blaster.py
│   ├── test_monitor.py        # Unit tests for monitor.py
│   └── test_log_analysis.py   # Unit tests for log_analysis.py
├── pyproject.toml             # Build config, dependencies, tool settings
└── README.md
```

<!-- prettier-ignore-start -->
[actions-badge]:            https://github.com/umr-lops/turboblast/workflows/CI/badge.svg
[actions-link]:             https://github.com/umr-lops/turboblast/actions
[conda-badge]:              https://img.shields.io/conda/vn/conda-forge/turboblast
[conda-link]:               https://github.com/conda-forge/turboblast-feedstock
[github-discussions-badge]: https://img.shields.io/static/v1?label=Discussions&message=Ask&color=blue&logo=github
[github-discussions-link]:  https://github.com/umr-lops/turboblast/discussions
[pypi-link]:                https://pypi.org/project/turboblast/
[pypi-platforms]:           https://img.shields.io/pypi/pyversions/turboblast
[pypi-version]:             https://img.shields.io/pypi/v/turboblast
[rtd-badge]:                https://readthedocs.org/projects/turboblast/badge/?version=latest
[rtd-link]:                 https://turboblast.readthedocs.io/en/latest/?badge=latest
[coverage-badge]:           https://codecov.io/github/umr-lops/turboblast/branch/main/graph/badge.svg
[coverage-link]:            https://codecov.io/github/umr-lops/turboblast

<!-- prettier-ignore-end -->
