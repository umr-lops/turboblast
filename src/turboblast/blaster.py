#!/usr/bin/python
import argparse
import datetime
import functools
import logging
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path

import submitit
from tqdm import tqdm

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

# ---------------------------------------------------------------------------
# Slurm configuration
# ---------------------------------------------------------------------------

# Maximum number of tasks per job array chunk.
# Must stay strictly below MaxArraySize (= 1001 on this cluster).
CHUNK_SIZE = 1000

# Seconds between polls when waiting for a batch to complete.
BATCH_POLL_INTERVAL = 30

# Maximum number of sbatch submission retries on transient failures.
SUBMIT_MAX_RETRIES = 2

# Base backoff in seconds between submission retries (multiplied by attempt number).
SUBMIT_RETRY_BACKOFF = 5.0

# Terminal Slurm job states — a batch is done when all tasks reach one of these.
TERMINAL_STATES = frozenset(
    {
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "TIMEOUT",
        "OUT_OF_MEMORY",
        "NODE_FAIL",
        "PREEMPTED",
        "BOOT_FAIL",
        "DEADLINE",
    }
)


# En haut du fichier, après les imports et constantes
class ChunkSubmissionError(RuntimeError):
    """Raised when a chunk fails to submit after all retry attempts."""

    def __init__(self, chunk_idx: int, total_chunks: int, max_retries: int):
        self.chunk_idx = chunk_idx
        self.total_chunks = total_chunks
        self.max_retries = max_retries
        super().__init__(
            f"Chunk {chunk_idx}/{total_chunks}: submission failed after {max_retries} attempts."
        )


def parse_memory_to_gb(value: str) -> float:
    """Parses a human-readable memory string and returns the value in gigabytes.

    Accepts an integer with an optional unit suffix (case-insensitive).
    Bare integers are assumed to be gigabytes for backward compatibility.

    Args:
        value (str): Memory string, e.g. ``"2G"``, ``"512M"``, ``"4GB"``, ``"1024MB"``.

    Returns:
        float: Memory in gigabytes, as expected by submitit's ``mem_gb`` parameter.

    Raises:
        argparse.ArgumentTypeError: If the value cannot be parsed.
    """
    value = value.strip().upper().replace(" ", "")

    # Strip trailing 'B' so both "MB" and "M", "GB" and "G" are handled uniformly.
    if value.endswith("MB"):
        unit, number = "M", value[:-2]
    elif value.endswith("GB"):
        unit, number = "G", value[:-2]
    elif value.endswith("M"):
        unit, number = "M", value[:-1]
    elif value.endswith("G"):
        unit, number = "G", value[:-1]
    else:
        # No unit — assume gigabytes for backward compatibility with --mem-gb.
        unit, number = "G", value

    try:
        amount = float(number)
    except ValueError as err:
        raise argparse.ArgumentTypeError(
            f"Invalid memory value: {value!r}. "
            "Expected an integer with optional unit suffix (M, MB, G, GB). "
            "Examples: 512M, 2G, 4GB, 1024MB."
        ) from err

    if amount <= 0:
        raise argparse.ArgumentTypeError(f"Memory must be positive, got {amount}.")

    return amount / 1024 if unit == "M" else amount


# ---------------------------------------------------------------------------
# Task execution (runs on the compute node via submitit)
# ---------------------------------------------------------------------------


def process_line(slurmexe: str, options_one_line: str) -> None:
    """Executes a single task in the Slurm job array by calling a bash script.

    This function is serialized and executed on the Slurm compute node by
    submitit.  It constructs the command, sets up the environment, and streams
    both standard output and standard error directly to the submitit log files.

    Args:
        slurmexe (str): The path to the bash executable/script to run.
        options_one_line (str): A string containing the command-line arguments
            to pass to the bash script (e.g., ``"--input file.txt --output dir/"``).

    Raises:
        subprocess.CalledProcessError: If the bash command exits with a non-zero
            status code, ensuring submitit and Slurm mark the task as FAILED.
    """
    logger.info("Starting computation for options: %s", options_one_line)

    # shlex.split handles quoted arguments safely (safer than str.split())
    cmd = ["bash", slurmexe, *shlex.split(options_one_line)]
    logger.info("Executing command: %s", " ".join(cmd))

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
        # Flush to ensure everything is written to the submitit .out file immediately.
        sys.stdout.flush()
        logger.info("Task completed successfully for options: %s", options_one_line)

    except subprocess.CalledProcessError as e:
        logger.exception(
            "Task failed for options: %s (exit status %s)",
            options_one_line,
            e.returncode,
        )
        raise


# ---------------------------------------------------------------------------
# Batch submission with retry
# ---------------------------------------------------------------------------


def submit_chunk_with_retry(
    executor: submitit.AutoExecutor,
    process_func: functools.partial,
    chunk: list[str],
    chunk_idx: int,
    total_chunks: int,
) -> list[submitit.Job]:
    """Submits a single job array chunk with retries on transient sbatch failures.

    Mirrors the retry logic used by the Airflow Slurm provider: up to
    ``SUBMIT_MAX_RETRIES`` attempts, with a linearly increasing backoff between
    attempts.  A failed attempt is logged as a warning; only a full exhaustion
    of retries raises an exception.

    Args:
        executor (submitit.AutoExecutor): Configured submitit executor.
        process_func (functools.partial): Partially applied task function.
        chunk (list[str]): Input lines for this batch.
        chunk_idx (int): 1-based index of this chunk (for logging).
        total_chunks (int): Total number of chunks (for logging).

    Returns:
        list[submitit.Job]: The submitted job handles for this chunk.

    Raises:
        RuntimeError: If all retry attempts are exhausted.
    """
    last_exc: Exception | None = None

    for attempt in range(1, SUBMIT_MAX_RETRIES + 1):
        try:
            jobs = executor.map_array(process_func, chunk)
            logger.debug(
                "Chunk %d/%d submitted (attempt %d). Job Array ID: %s",
                chunk_idx,
                total_chunks,
                attempt,
                jobs[0].job_id,
            )
            return jobs

        except Exception as exc:
            # Catch all exceptions during submission to enable retry logic.
            # Transient failures (network, Slurm congestion, etc.) can raise
            # various exception types. We preserve the last exception to
            # raise it if all retries fail.
            last_exc = exc
            logger.debug(
                "Chunk %d/%d — submission attempt %d/%d failed: %s",
                chunk_idx,
                total_chunks,
                attempt,
                SUBMIT_MAX_RETRIES,
                exc,
            )
            if attempt < SUBMIT_MAX_RETRIES:
                backoff = SUBMIT_RETRY_BACKOFF * attempt
                logger.info("Retrying in %.0fs...", backoff)
                time.sleep(backoff)
    raise ChunkSubmissionError(
        chunk_idx, total_chunks, SUBMIT_MAX_RETRIES
    ) from last_exc


# ---------------------------------------------------------------------------
# Batch completion polling
# ---------------------------------------------------------------------------


def wait_for_batch_completion(
    jobs: list[submitit.Job],
    chunk_idx: int,
    total_chunks: int,
    stall_timeout_min: int = 0,
) -> tuple[int, int]:
    """Blocks until every job in a batch reaches a terminal Slurm state.

    Polls submitit job states every ``BATCH_POLL_INTERVAL`` seconds and updates
    a tqdm progress bar in real time.  This sequential, blocking approach ensures
    at most one job array is in flight at a time, which prevents ``MaxJobCount``
    saturation on the cluster (strategy borrowed from the Airflow Slurm provider).

    The progress bar tracks completed tasks and shows live counts of each Slurm
    state (RUNNING, PENDING, FAILED, NODE_FAIL, …) in its postfix.

    If ``stall_timeout_min`` > 0, any tasks still non-terminal after that many
    minutes without progress are cancelled via ``scancel`` and counted as failed.

    Args:
        jobs (list[submitit.Job]): Job handles returned by ``map_array()``.
        chunk_idx (int): 1-based index of this chunk (for logging).
        total_chunks (int): Total number of chunks (for logging).
        stall_timeout_min (int): Minutes without progress before cancelling
            stuck tasks. 0 = disabled (wait forever).

    Returns:
        tuple[int, int]: ``(completed, failed)`` task counts.
    """
    total = len(jobs)
    logger.debug(
        "Waiting for batch %d/%d to complete (%d tasks)...",
        chunk_idx,
        total_chunks,
        total,
    )

    with tqdm(
        total=total,
        desc=f"Batch {chunk_idx}/{total_chunks}",
        unit="task",
        dynamic_ncols=True,
        leave=True,
        file=sys.stdout,
    ) as pbar:
        prev_terminal = 0
        # Initialised here so last_progress_time is always defined.
        last_progress_time = time.monotonic()

        while True:
            time.sleep(BATCH_POLL_INTERVAL)

            # Collect per-task states via submitit (wraps squeue internally).
            states: dict[str, int] = {}
            for job in jobs:
                try:
                    state = job.state
                except (
                    submitit.core.utils.FailedJobError,
                    AttributeError,
                    RuntimeError,
                ) as e:
                    # Ces exceptions sont attendues dans le contexte submitit/Slurm
                    logger.debug("Job state query failed: %s: %s", type(e).__name__, e)
                    state = "UNKNOWN"
                except Exception as e:
                    # Attraper les autres exceptions mais avec log de warning
                    # et propagation des exceptions système critiques
                    if isinstance(e, (KeyboardInterrupt, SystemExit)):
                        raise
                    logger.warning(
                        "Unexpected error in job state polling: %s", e, exc_info=True
                    )
                    state = "UNKNOWN"
                states[state] = states.get(state, 0) + 1

            # Count terminal tasks — submitit exposes its own state names
            # ("COMPLETED", "FAILED") as well as raw Slurm states for the
            # other terminal conditions (NODE_FAIL, TIMEOUT, etc.).
            n_terminal = sum(
                count
                for state, count in states.items()
                if state.upper() in TERMINAL_STATES or state in ("COMPLETED", "FAILED")
            )

            # Advance the bar by however many new terminal tasks appeared.
            delta = n_terminal - prev_terminal
            if delta > 0:
                pbar.update(delta)
                # Reset the stall clock whenever progress is observed.
                last_progress_time = time.monotonic()

            # Stall detection: computed every cycle, not just when delta > 0.
            stalled_min = (time.monotonic() - last_progress_time) / 60
            if stall_timeout_min > 0 and stalled_min >= stall_timeout_min:
                stuck_jobs = [j for j in jobs if j.state.upper() not in TERMINAL_STATES]
                job_ids = {j.job_id.split("_")[0] for j in stuck_jobs}
                logger.warning(
                    "Batch %d/%d stalled for %.0f min with %d task(s) "
                    "non-terminal. Cancelling: %s",
                    chunk_idx,
                    total_chunks,
                    stalled_min,
                    len(stuck_jobs),
                    job_ids,
                )
                for jid in job_ids:
                    subprocess.run(["scancel", jid], check=False)
                # Wait one cycle for Slurm to process the cancellations.
                time.sleep(BATCH_POLL_INTERVAL)
                completed = states.get("COMPLETED", 0)
                failed = total - completed
                pbar.set_postfix(
                    {
                        "completed": completed,
                        "failed": failed,
                        "cancelled": len(stuck_jobs),
                    },
                    refresh=True,
                )
                logger.warning(
                    "Batch %d/%d force-finished after stall — "
                    "completed=%d  failed=%d  cancelled=%d",
                    chunk_idx,
                    total_chunks,
                    completed,
                    failed,
                    len(stuck_jobs),
                )
                return completed, failed

            prev_terminal = n_terminal

            # Build a compact postfix showing every non-zero state.
            # Prioritise actionable states first (RUNNING, PENDING) then
            # failures, so the most important info is never truncated.
            state_order = [
                "RUNNING",
                "PENDING",
                "COMPLETED",
                "FAILED",
                "NODE_FAIL",
                "TIMEOUT",
                "CANCELLED",
                "UNKNOWN",
            ]
            postfix_parts = {}
            for s in state_order:
                if states.get(s, 0):
                    postfix_parts[s.lower()] = states[s]
            # Append any remaining states not in the priority list.
            for s, n in states.items():
                if s not in state_order and n:
                    postfix_parts[s.lower()] = n
            # Show stall countdown when stall detection is enabled.
            if stall_timeout_min > 0 and stalled_min > 0:
                elapsed_mins = int(stalled_min)
                postfix_parts["stall"] = f"{elapsed_mins}/{stall_timeout_min}min"  # type: ignore[assignment]
            pbar.set_postfix(postfix_parts)

            parts = [f"{s}={n}" for s, n in sorted(states.items())]
            logger.debug(
                "Batch %d/%d — total=%d  terminal=%d  [%s]",
                chunk_idx,
                total_chunks,
                total,
                n_terminal,
                "  ".join(parts),
            )

            if n_terminal >= total:
                completed = states.get("COMPLETED", 0)
                failed = total - completed
                pbar.set_postfix(
                    {"completed": completed, "failed": failed},
                    refresh=True,
                )
                logger.info(
                    "Batch %d/%d finished — completed=%d  failed=%d",
                    chunk_idx,
                    total_chunks,
                    completed,
                    failed,
                )
                return completed, failed


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------


def parser_args() -> argparse.Namespace:
    """Parses command-line arguments for the turboblaster client.

    Returns:
        argparse.Namespace: An object containing all parsed arguments.
    """
    # RawDescriptionHelpFormatter is required to preserve the ASCII art logo.
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
        "--mem",
        type=str,
        default="2G",
        help=(
            "Memory per task. Accepts an integer with optional unit suffix: "
            "M or MB for megabytes, G or GB for gigabytes (case-insensitive). "
            "Examples: --mem 512M  --mem 2G  --mem 4GB  --mem 1024MB"
        ),
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
        help="Path to a file containing one input argument per line",
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
        "--slurm-array-parallelism",
        type=int,
        default=20,
        help="Max concurrent tasks per job array (maps to %%N in --array=0-999%%N)",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        default=False,
        help="Stop submitting further batches if any task in the current batch fails",
    )
    parser.add_argument(
        "--batch-stall-timeout-min",
        type=int,
        default=0,
        help=(
            "Cancel remaining tasks and move to the next batch if no progress "
            "is observed for this many minutes. 0 = disabled (wait forever)."
        ),
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main submission logic
# ---------------------------------------------------------------------------


def main(args: argparse.Namespace) -> None:
    """Configures and submits job array chunks to the Slurm cluster.

    Strategy (inspired by the Airflow Slurm provider):
      1. Split the input listing into chunks of ``CHUNK_SIZE`` (< MaxArraySize).
      2. Submit the first chunk as a Slurm job array via submitit.
      3. **Block** until every task in that chunk reaches a terminal state,
         showing a live tqdm progress bar.
      4. Only then submit the next chunk.

    This sequential, one-array-at-a-time approach ensures the cluster's
    ``MaxJobCount`` is never saturated, at the cost of lower parallelism
    compared to submitting all chunks simultaneously.

    Each submission is retried up to ``SUBMIT_MAX_RETRIES`` times with
    linear backoff to handle transient sbatch failures.

    Args:
        args (argparse.Namespace): Parsed CLI arguments (paths + Slurm config).
    """
    # Timestamped sub-directory keeps each run's logs isolated.
    output_dir_with_date = Path(args.output_dir) / datetime.datetime.now(
        datetime.timezone.utc
    ).strftime("%Y%m%dT%H%M%S")
    output_dir_with_date.mkdir(parents=True, exist_ok=True)

    logger.info("Submitit logs will be stored in: %s", output_dir_with_date)
    logger.info("Bash script to execute: %s", args.bash_slurm_exec)
    logger.info(
        "Submission strategy: sequential batches (1 array at a time, blocking poll)"
    )
    mem_gb = parse_memory_to_gb(args.mem)
    logger.info("Parsed memory argument: %s -> %.2f GB", args.mem, mem_gb)
    executor = submitit.AutoExecutor(folder=output_dir_with_date, cluster="slurm")
    executor.update_parameters(
        timeout_min=args.timeout_min,
        mem_gb=mem_gb,
        cpus_per_task=args.cpus_per_task,
        slurm_partition=args.slurm_partition,
        slurm_array_parallelism=args.slurm_array_parallelism,
        slurm_job_name=Path(args.bash_slurm_exec).name.replace(".sh", ""),
        slurm_additional_parameters={"export": "ALL,PYTHONUNBUFFERED=1"},
    )

    with Path(args.listing_input).open("r", encoding="utf-8") as f:
        array_inputs = [line.strip() for line in f if line.strip()]

    if not array_inputs:
        logger.error("Input listing file is empty. Aborting submission.")
        return

    total_chunks = (len(array_inputs) + CHUNK_SIZE - 1) // CHUNK_SIZE
    logger.info("Total tasks to submit: %d", len(array_inputs))
    logger.info(
        "Submitting in chunks of %d (%d chunks total)...", CHUNK_SIZE, total_chunks
    )

    process_func = functools.partial(process_line, args.bash_slurm_exec)  # type: ignore[type-arg]

    global_completed = 0
    global_failed = 0

    # Outer progress bar tracks batch-level progress across all chunks.
    with tqdm(
        total=total_chunks,
        desc="Overall batches",
        unit="batch",
        dynamic_ncols=True,
        leave=True,
        file=sys.stdout,
    ) as outer_pbar:
        for chunk_idx, i in enumerate(range(0, len(array_inputs), CHUNK_SIZE), start=1):
            chunk = array_inputs[i : i + CHUNK_SIZE]
            logger.debug(
                "=== Batch %d/%d — tasks %d to %d (%d tasks) ===",
                chunk_idx,
                total_chunks,
                i,
                i + len(chunk) - 1,
                len(chunk),
            )

            # Submit with retry on transient sbatch failures.
            jobs = submit_chunk_with_retry(
                executor, process_func, chunk, chunk_idx, total_chunks
            )

            # Block until this batch is fully done before submitting the next one.
            # This is the key throttling mechanism: at most one array in flight.
            completed, failed = wait_for_batch_completion(jobs, chunk_idx, total_chunks)
            global_completed += completed
            global_failed += failed

            outer_pbar.update(1)
            outer_pbar.set_postfix(
                {
                    "completed": global_completed,
                    "failed": global_failed,
                }
            )

            if failed and args.fail_fast:
                logger.error(
                    "Batch %d/%d had %d failure(s) and --fail-fast is set. "
                    "Aborting remaining %d batches.",
                    chunk_idx,
                    total_chunks,
                    failed,
                    total_chunks - chunk_idx,
                )
                break

    logger.info(
        "All done — total_submitted=%d  completed=%d  failed=%d  across %d batch(es).",
        global_completed + global_failed,
        global_completed,
        global_failed,
        total_chunks,
    )
    logger.info(
        "Task logs are in: %s/<ARRAY_ID>_<TASK_ID>_0_log.out",
        output_dir_with_date,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def entrypoint() -> None:
    """Script entry point.

    Parses CLI arguments and delegates to :func:`main`.
    """
    args = parser_args()
    main(args)


if __name__ == "__main__":
    entrypoint()
