# turboblast

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
into chunks of 1000 to stay within Slurm array limits.

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
  --mem-gb 8 \
  --cpus-per-task 4 \
  --slurm-array-parallelism 50 \
  --output-dir submitit_logs
```

### Full CLI reference

```
usage: turboblaster [-h] [--num-tasks NUM_TASKS] [--timeout-min TIMEOUT_MIN]
                    [--mem-gb MEM_GB] [--cpus-per-task CPUS_PER_TASK]
                    [--slurm-partition SLURM_PARTITION]
                    --listing-input LISTING_INPUT
                    --bash-slurm-exec BASH_SLURM_EXEC
                    [--output-dir OUTPUT_DIR]
                    [--slurm-array-parallelism SLURM_ARRAY_PARALLELISM]

options:
  --listing-input            Path to a file containing input lines (one task per line) [required]
  --bash-slurm-exec          Path to the bash script to execute for each task [required]
  --num-tasks                Number of tasks (unused if reading from file) [default: 20]
  --timeout-min              Timeout in minutes for each task [default: 20]
  --mem-gb                   Memory in GB for each task [default: 2]
  --cpus-per-task            Number of CPUs per task [default: 1]
  --slurm-partition          Slurm partition to use [default: cpu]
  --output-dir               Directory to store submitit logs [default: submitit_logs_array]
  --slurm-array-parallelism  Max number of tasks running concurrently [default: 20]
```

Submitit logs (`.out` / `.err` files) are written to a timestamped subdirectory
under `--output-dir`:

```
submitit_logs/
└── 20260309T143000/
    ├── 12345_0_0.out
    ├── 12345_1_0.out
    └── ...
```

Monitor a specific task with:

```bash
tail -f submitit_logs/20260309T143000/12345_0_0.out
```

## Project structure

```
turboblast/
├── src/
│   └── turboblast/
│       ├── __init__.py       # Package entry point, exposes __version__
│       ├── blaster.py        # Core logic: argument parsing, job submission, task execution
│       └── logo.py           # ASCII art logo used in the CLI help message
├── tests/
│   ├── test_package.py       # Package metadata tests (version check)
│   └── test_blaster.py       # Unit tests for blaster.py
├── pyproject.toml            # Build config, dependencies, tool settings
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
