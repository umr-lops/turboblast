#!/usr/bin/python
import argparse
import datetime
import functools
import logging
import os
import shlex
import subprocess
import sys
from pathlib import Path

import submitit  # type: ignore[import-not-found]

from turboblast.logo import LOGO

# Configure the logger globally
# This format matches standard logging practices: [Date Time] [LEVEL] Message
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


def process_line(slurmexe: str, options_one_line: str) -> None:
    """Executes a single task in the Slurm job array by calling a bash script.

    This function is serialized and executed on the Slurm compute node by submitit.
    It constructs the command, sets up the environment, and streams both standard
    output and standard error directly to the submitit log files.

    Args:
        slurmexe (str): The path to the bash executable/script to run.
        options_one_line (str): A string containing the command-line arguments
            to pass to the bash script (e.g., "--input file.txt --output dir/").

    Raises:
        subprocess.CalledProcessError: If the bash command exits with a non-zero
            status code, ensuring submitit and Slurm mark the task as FAILED.
    """
    logger.info("Starting computation for options: %s", options_one_line)

    # Construct the command
    # shlex.split is safer than string.split() if your options contain quoted strings
    # RUF005: Use unpacking instead of list concatenation
    cmd = ["bash", slurmexe, *shlex.split(options_one_line)]

    logger.info("Executing command: %s", " ".join(cmd))

    # 1. Prepare the environment correctly
    full_env = os.environ.copy()
    full_env["PYTHONUNBUFFERED"] = "1"

    try:
        subprocess.run(
            cmd,
            shell=False,
            check=True,
            stdout=sys.stdout,
            # Redirect stderr to stdout to merge logs chronologically.
            # This ensures INFO and WARNING/ERROR messages don't get out of sync.
            stderr=subprocess.STDOUT,
            env=full_env,
        )
        # Flush to ensure everything is written to the submitit .out file immediately
        sys.stdout.flush()
        logger.info("Task completed successfully for options: %s", options_one_line)

    except subprocess.CalledProcessError as e:
        # TRY400: Use logging.exception instead of logging.error inside except block
        logger.exception(
            "Task failed for options: %s (exit status %s)",
            options_one_line,
            e.returncode,
        )
        # TRY201: Use bare raise
        raise


def parser_args() -> argparse.Namespace:
    """Parses command-line arguments for the submitit client.

    Returns:
        argparse.Namespace: An object containing all the parsed command-line
            arguments and their values.
    """

    # formatter_class=argparse.RawDescriptionHelpFormatter is REQUIRED
    # to preserve the newlines and spaces of the ASCII art logo!
    parser = argparse.ArgumentParser(
        description=LOGO, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--num-tasks",
        type=int,
        default=20,
        help="Number of tasks (unused if reading from file)",
        required=False,
    )
    parser.add_argument(
        "--timeout-min", type=int, default=20, help="Timeout in minutes for each task"
    )
    parser.add_argument(
        "--mem-gb", type=int, default=2, help="Memory in GB for each task"
    )
    parser.add_argument(
        "--cpus-per-task", type=int, default=1, help="Number of CPUs per task"
    )
    parser.add_argument(
        "--slurm-partition", type=str, default="cpu", help="Slurm partition to use"
    )
    parser.add_argument(
        "--listing-input",
        type=str,
        required=True,
        help="Path to a file containing input lines",
    )
    parser.add_argument(
        "--bash-slurm-exec", type=str, required=True, help="Path to the bash script"
    )
    parser.add_argument(
        "--output-dir",
        help="Directory to store submitit logs",
        default="submitit_logs_array",
    )
    parser.add_argument(
        "--slurm-array-parallelism", type=int, default=20, help="Max concurrent tasks"
    )
    return parser.parse_args()


def main(args: argparse.Namespace) -> None:
    """Configures and submits the job arrays to the Slurm cluster.

    This function reads the input file, configures the submitit AutoExecutor
    with the required Slurm parameters (memory, partition, time, etc.), and
    submits the jobs in chunks to respect Slurm array limits.

    Args:
        args (argparse.Namespace): The parsed command-line arguments containing
            paths and Slurm configuration variables.
    """
    # Create log directory
    # DTZ002: Use datetime.now with a timezone instead of today()
    output_dir_with_date = Path(args.output_dir) / datetime.datetime.now(
        datetime.timezone.utc
    ).strftime("%Y%m%dT%H%M%S")

    # Use instance method for directory creation
    output_dir_with_date.mkdir(parents=True, exist_ok=True)

    logger.info("Submitit logs will be stored in: %s", output_dir_with_date)
    logger.info("Bash script to execute: %s", args.bash_slurm_exec)

    executor = submitit.AutoExecutor(folder=output_dir_with_date, cluster="slurm")

    # Slurm Configuration
    executor.update_parameters(
        timeout_min=args.timeout_min,
        mem_gb=args.mem_gb,
        cpus_per_task=args.cpus_per_task,
        slurm_partition=args.slurm_partition,
        slurm_array_parallelism=args.slurm_array_parallelism,
        slurm_job_name=Path(args.bash_slurm_exec).name.replace(".sh", ""),
        slurm_additional_parameters={"export": "ALL,PYTHONUNBUFFERED=1"},
    )

    # Read inputs
    # Use Path object method to correctly open the file (fixes mypy overload error)
    with Path(args.listing_input).open("r") as f:
        array_inputs = [line.strip() for line in f if line.strip()]

    if not array_inputs:
        logger.error("Input listing file is empty. Aborting submission.")
        return

    # Define the chunk size (e.g., 1000)
    chunk_size = 1000
    all_jobs = []

    logger.info("Total tasks to submit: %d", len(array_inputs))

    # Calculate total number of chunks for logging
    total_chunks = (len(array_inputs) + chunk_size - 1) // chunk_size
    logger.info(
        "Submitting tasks in chunks of %d (Total chunks: %d)...",
        chunk_size,
        total_chunks,
    )

    process_func = functools.partial(process_line, args.bash_slurm_exec)

    # Loop through the inputs in chunks
    for chunk_idx, i in enumerate(range(0, len(array_inputs), chunk_size), start=1):
        chunk = array_inputs[i : i + chunk_size]
        logger.info(
            "Submitting chunk %d/%d (size: %d, starting at index %d)...",
            chunk_idx,
            total_chunks,
            len(chunk),
            i,
        )

        # This will create a NEW Slurm Job Array for every 1000 tasks
        jobs = executor.map_array(process_func, chunk)

        logger.info(
            "Chunk %d submitted successfully. Job Array ID: %s",
            chunk_idx,
            jobs[0].job_id,
        )
        all_jobs.extend(jobs)

    logger.info(
        "Successfully submitted all %d tasks across %d Job Arrays.",
        len(all_jobs),
        total_chunks,
    )

    if all_jobs:
        logger.info("First Job Array Main Job ID: %s", all_jobs[0].job_id)
        logger.info(
            "To monitor specific task logs, use: tail -f %s/<JOB_ID>_<TASK_ID>_0.out",
            output_dir_with_date,
        )


def entrypoint() -> None:
    """Script entrypoint.

    Calls the argument parser and passes the resulting arguments to the
    main execution function.
    """
    args = parser_args()
    main(args)


if __name__ == "__main__":
    entrypoint()
