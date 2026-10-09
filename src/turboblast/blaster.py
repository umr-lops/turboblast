#!/usr/bin/python
import argparse
import datetime
import functools
import logging
import math
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

# Non-terminal states that indicate a task is *parked* and unlikely to ever run on
# its own (an admin hold, or a requeue that landed in a held/queued-but-stalled
# state). A task sitting in one of these for longer than the per-task stuck
# timeout is cancelled individually. Note PENDING is deliberately *not* here:
# with --slurm-array-parallelism < batch size, PENDING is the *normal* state for
# the large majority of a batch's tasks (they are simply queued behind the
# parallelism cap), so a short PENDING timer would mass-cancel healthy tasks. A
# genuinely stuck PENDING task is instead caught by the batch no-progress
# backstop (see wait_for_batch_completion).
HOLD_STATES = frozenset({"HELD", "REQUEUE_HOLD", "REQUEUED"})

# Brief transition states right before a task reaches a terminal state.
TRANSIENT_STATES = frozenset({"COMPLETING", "STOPPED"})

# Grace (minutes) added on top of --timeout-min for a RUNNING task. Slurm itself
# kills a RUNNING task at its wall-time limit (-> TIMEOUT, a terminal state), so
# this is a small safety margin, not a real "running is slow" timeout.
RUNNING_GRACE_MIN = 15.0

# Grace (minutes) added on top of --timeout-min for the batch no-progress
# backstop: if *no* task reaches a terminal state for longer than this, the batch
# is force-finished. A RUNNING task can never stay non-terminal longer than
# --timeout-min (Slurm times it out), so a no-progress window past that means only
# stuck (non-running) tasks remain.
NO_PROGRESS_GRACE_MIN = 15.0


def is_terminal_state(state: str) -> bool:
    """Return True if a Slurm/submitit state string is terminal.

    ``submitit``'s ``job.state`` returns the raw Slurm ``State`` string from
    ``sacct`` (e.g. ``"PENDING"``, ``"COMPLETED"``, ``"NODE_FAIL"``).
    """
    return state.upper() in TERMINAL_STATES


def stuck_threshold_minutes(
    state: str, task_timeout_min: int, task_stuck_min: float
) -> float | None:
    """Return the max minutes a task may stay in ``state`` before it is cancelled
    individually, or ``None`` to mean "no per-task limit" (rely on the
    batch-level backstops instead).

    State-aware policy:
      * ``RUNNING``   -> ``task_timeout_min + RUNNING_GRACE_MIN`` (Slurm times the
        task out at its wall limit anyway).
      * hold states   (HELD / REQUEUE_HOLD / REQUEUED) and transient states
        (COMPLETING / STOPPED) -> ``task_stuck_min`` (short: these will not run on
        their own). ``None`` if ``task_stuck_min`` is 0 (disabled).
      * ``PENDING``   -> ``None`` (normal queue state; a stuck one is caught by the
        no-progress backstop).
      * anything else (``UNKNOWN`` etc.) -> ``None`` (be conservative).
    """
    s = state.upper()
    if s == "RUNNING":
        return float(task_timeout_min) + RUNNING_GRACE_MIN
    if s in HOLD_STATES or s in TRANSIENT_STATES:
        return task_stuck_min if task_stuck_min > 0 else None
    return None


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

        except (OSError, RuntimeError, ValueError, TypeError) as exc:
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


def _force_finish_batch(
    job_states: list[tuple[submitit.Job, str]],
    states: dict[str, int],
    total: int,
    pbar: tqdm,
    chunk_idx: int,
    total_chunks: int,
    reason: str,
) -> tuple[int, int]:
    """Cancel every still non-terminal task in a batch and return its final counts.

    Used by the batch-level backstops (no-progress stall, wall-time cap) when
    waiting any longer is not worth it. Each task is cancelled by its full
    per-task id (a submitit array job_id is "base_task"; cancelling the base id
    would scancel the whole array). Cancelled tasks count as failed.
    """
    remaining = [j for j, s in job_states if not is_terminal_state(s)]
    job_ids = sorted(j.job_id for j in remaining)
    logger.warning(
        "Batch %d/%d: %s — cancelling %d remaining task(s): %s",
        chunk_idx,
        total_chunks,
        reason,
        len(remaining),
        job_ids,
    )
    if job_ids:
        subprocess.run(["scancel", *job_ids], check=False)
    # Wait one cycle for Slurm to process the cancellations.
    time.sleep(BATCH_POLL_INTERVAL)
    completed = states.get("COMPLETED", 0)
    failed = total - completed
    pbar.set_postfix(
        {"completed": completed, "failed": failed, "cancelled": len(remaining)},
        refresh=True,
    )
    logger.warning(
        "Batch %d/%d force-finished — completed=%d  failed=%d  cancelled=%d",
        chunk_idx,
        total_chunks,
        completed,
        failed,
        len(remaining),
    )
    return completed, failed


def wait_for_batch_completion(
    jobs: list[submitit.Job],
    chunk_idx: int,
    total_chunks: int,
    stall_timeout_min: float | None = None,
    task_stuck_min: float = 15.0,
    batch_wall_timeout_min: float | None = None,
    task_timeout_min: int = 20,
) -> tuple[int, int]:
    """Blocks until every job in a batch reaches a terminal Slurm state.

    Polls submitit job states every ``BATCH_POLL_INTERVAL`` seconds and updates a
    tqdm progress bar in real time.  This sequential, blocking approach ensures at
    most one job array is in flight at a time, which prevents ``MaxJobCount``
    saturation on the cluster (strategy borrowed from the Airflow Slurm provider).

    The progress bar tracks completed tasks and shows live counts of each Slurm
    state (RUNNING, PENDING, FAILED, NODE_FAIL, …) in its postfix.

    A batch is force-finished (cancelling the still non-terminal tasks) when any
    of these "no more progress possible" guards trips:

    * **Per-task stuck timeout** — a task sitting in a *hold* state (HELD /
      REQUEUE_HOLD / REQUEUED) or a *transient* state (COMPLETING / STOPPED) for
      more than ``task_stuck_min`` minutes is cancelled individually, regardless
      of whether the rest of the batch is still progressing.  ``RUNNING`` tasks
      are instead bounded by ``task_timeout_min + RUNNING_GRACE_MIN`` (Slurm kills
      them at the wall limit anyway).  ``PENDING`` has no per-task limit because,
      with ``--slurm-array-parallelism`` below the batch size, it is the normal
      queued state for most tasks.
    * **No-progress backstop** (``stall_timeout_min``) — if *no* task reaches a
      terminal state for that many minutes, all remaining tasks are cancelled.
      This is what catches a single task stuck in ``PENDING``/``HELD`` while the
      other 999 have already finished.
    * **Batch wall-time cap** (``batch_wall_timeout_min``) — a hard maximum wall
      time per batch, so no batch can stall the chain for arbitrarily long.

    All three default to ``None`` (= disabled) and are set from sensible
    values derived from ``--timeout-min`` in :func:`main`.

    Args:
        jobs (list[submitit.Job]): Job handles returned by ``map_array()``.
        chunk_idx (int): 1-based index of this chunk (for logging).
        total_chunks (int): Total number of chunks (for logging).
        stall_timeout_min (float | None): Minutes without progress before
            cancelling all remaining tasks. ``None`` (or ``0``) = disabled.
        task_stuck_min (float): Minutes a task may sit in a hold/transient state
            before it is cancelled individually. 0 = disabled.
        batch_wall_timeout_min (float | None): Hard cap on total batch wall time
            in minutes. ``None`` (or ``0``) = disabled.
        task_timeout_min (int): The ``--timeout-min`` value; used as the bound for
            ``RUNNING`` tasks and to sanity-check the backstop default.

    Returns:
        tuple[int, int]: ``(completed, failed)`` task counts.
    """
    total = len(jobs)
    now = time.monotonic()
    batch_start = now
    # last_progress_time: reset whenever a task reaches a terminal state.
    last_progress_time = now
    # Per-task (state, timestamp-when-that-state-started) for per-task stuck
    # detection. The clock resets whenever a task changes state, so e.g. a task
    # queued (PENDING) for a long time then starting RUNNING is measured from its
    # RUNNING start, not from when it first became non-terminal.
    state_since: dict[submitit.Job, tuple[str, float]] = {}

    logger.debug(
        "Waiting for batch %d/%d to complete (%d tasks)... "
        "stall_timeout=%smin  task_stuck=%smin  wall_timeout=%smin  task_timeout=%dmin",
        chunk_idx,
        total_chunks,
        total,
        stall_timeout_min,
        task_stuck_min,
        batch_wall_timeout_min,
        task_timeout_min,
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

        while True:
            time.sleep(BATCH_POLL_INTERVAL)
            now = time.monotonic()

            # ── Collect per-task states via submitit (wraps squeue/sacct) ──
            job_states: list[tuple[submitit.Job, str]] = []
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
                except (KeyboardInterrupt, SystemExit, OSError) as e:
                    # Attraper les autres exceptions mais avec log de warning
                    # et propagation des exceptions système critiques
                    if isinstance(e, (KeyboardInterrupt, SystemExit)):
                        raise
                    logger.warning(
                        "Unexpected error in job state polling: %s", e, exc_info=True
                    )
                    state = "UNKNOWN"
                job_states.append((job, state))
                states[state] = states.get(state, 0) + 1

            # ── Count terminal tasks ──
            n_terminal = sum(1 for _, s in job_states if is_terminal_state(s))

            # ── Per-task stuck detection (fixes #2 and #3) ──
            # Track how long each non-terminal task has been in its *current*
            # state (resetting the clock on state change) and cancel any task
            # whose state has exceeded its state-aware threshold. This works
            # regardless of whether the rest of the batch is still progressing.
            stuck_jobs: list[submitit.Job] = []
            for job, state in job_states:
                if is_terminal_state(state):
                    state_since.pop(job, None)
                    continue
                prev = state_since.get(job)
                if prev is None or prev[0] != state:
                    # New task, or just changed state -> (re)start its clock.
                    state_since[job] = (state, now)
                    continue  # don't judge it until the next poll (fair clock)
                _, since = state_since[job]
                stuck_min = (now - since) / 60.0
                threshold = stuck_threshold_minutes(
                    state, task_timeout_min, task_stuck_min
                )
                if threshold is not None and stuck_min >= threshold:
                    stuck_jobs.append(job)
                    logger.warning(
                        "Batch %d/%d: task %s stuck in %s for %.0f min "
                        "(threshold %.0f min) — cancelling",
                        chunk_idx,
                        total_chunks,
                        job.job_id,
                        state,
                        stuck_min,
                        threshold,
                    )
            if stuck_jobs:
                job_ids = sorted(j.job_id for j in stuck_jobs)
                # A submitit array job_id is "base_task" (e.g. 528266_108);
                # cancelling the base id would scancel the whole array, not just
                # the stuck tasks.
                subprocess.run(["scancel", *job_ids], check=False)
                # Drop them from tracking so they don't re-trigger; they'll show
                # as CANCELLED on the next poll.
                for j in stuck_jobs:
                    state_since.pop(j, None)

            # ── Advance progress bar / reset no-progress clock ──
            delta = n_terminal - prev_terminal
            if delta > 0:
                pbar.update(delta)
                last_progress_time = now

            # ── No-progress backstop (fix #1) ──
            stalled_min = (now - last_progress_time) / 60.0
            # ── Batch wall-time cap (fix #4) ──
            wall_min = (now - batch_start) / 60.0

            if (
                stall_timeout_min is not None
                and stall_timeout_min > 0
                and stalled_min >= stall_timeout_min
            ):
                return _force_finish_batch(
                    job_states,
                    states,
                    total,
                    pbar,
                    chunk_idx,
                    total_chunks,
                    f"stalled for {stalled_min:.0f} min with no progress "
                    f"(threshold {stall_timeout_min:.0f} min)",
                )

            if (
                batch_wall_timeout_min is not None
                and batch_wall_timeout_min > 0
                and wall_min >= batch_wall_timeout_min
            ):
                return _force_finish_batch(
                    job_states,
                    states,
                    total,
                    pbar,
                    chunk_idx,
                    total_chunks,
                    f"exceeded batch wall-time cap of "
                    f"{batch_wall_timeout_min:.0f} min (now at {wall_min:.0f} min)",
                )

            prev_terminal = n_terminal

            # ── Build a compact postfix showing every non-zero state ──
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
            postfix_parts: dict[str, object] = {}
            for s in state_order:
                if states.get(s, 0):
                    postfix_parts[s.lower()] = states[s]
            # Append any remaining states not in the priority list.
            for s, n in states.items():
                if s not in state_order and n:
                    postfix_parts[s.lower()] = n
            # Show stall countdown when the no-progress backstop is enabled.
            if stall_timeout_min is not None and stall_timeout_min > 0:
                postfix_parts["stall"] = (
                    f"{int(stalled_min)}/{int(stall_timeout_min)}min"
                )
            # Show wall-time countdown when the wall cap is enabled.
            if batch_wall_timeout_min is not None and batch_wall_timeout_min > 0:
                postfix_parts["wall"] = (
                    f"{int(wall_min)}/{int(batch_wall_timeout_min)}min"
                )
            pbar.set_postfix(postfix_parts)

            parts = [f"{s}={n}" for s, n in sorted(states.items())]
            logger.debug(
                "Batch %d/%d — total=%d  terminal=%d  [%s]  stalled=%.0fmin  wall=%.0fmin",
                chunk_idx,
                total_chunks,
                total,
                n_terminal,
                "  ".join(parts),
                stalled_min,
                wall_min,
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
        help=(
            "Max tasks running concurrently (maps to %%N in --array=0-999%%N). "
            "Throughput = parallelism / per-task duration, so for short tasks "
            "raise it (e.g. 50-100) to go faster, subject to cluster capacity."
        ),
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        default=False,
        help="Stop submitting further batches if any task in the current batch fails",
    )
    parser.add_argument(
        "--batch-stall-timeout-min",
        type=float,
        default=None,
        help=(
            "No-progress backstop: cancel the still-pending tasks and move to the "
            "next batch if NO task reaches a terminal state for this many minutes. "
            "This is what catches a single task stuck in PENDING/HELD while the "
            "other 999 have finished. "
            "Default: --timeout-min + 15. Use 0 to disable."
        ),
    )
    parser.add_argument(
        "--task-stuck-min",
        type=float,
        default=15.0,
        help=(
            "Per-task stuck timeout (minutes) for tasks sitting in a hold state "
            "(HELD / REQUEUE_HOLD / REQUEUED) or a transient state (COMPLETING / "
            "STOPPED); such a task is cancelled individually. PENDING tasks have "
            "no per-task limit (normal queue state). 0 = disabled."
        ),
    )
    parser.add_argument(
        "--batch-wall-timeout-min",
        type=float,
        default=None,
        help=(
            "Hard cap on total batch wall time (minutes). A batch running longer "
            "is force-finished (remaining tasks cancelled) so it cannot stall the "
            "chain. Default: --timeout-min * 4 + 60. Use 0 to disable."
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
    logger.info(
        "At most %d task(s) run concurrently. Note: the progress bar's 's/task' "
        "is an aggregate rate (elapsed / completed), NOT the per-task wall time — "
        "with %d in parallel, a task taking T seconds shows as ~T/%d s/task. A "
        "slow-looking run usually means slow tasks, or --slurm-array-parallelism "
        "too low to saturate the cluster.",
        args.slurm_array_parallelism,
        args.slurm_array_parallelism,
        args.slurm_array_parallelism,
    )

    process_func = functools.partial(process_line, args.bash_slurm_exec)

    # Derive the batch-protection defaults from --timeout-min. A RUNNING task can
    # never stay non-terminal longer than its wall-time limit (Slurm kills it ->
    # TIMEOUT), so the no-progress backstop only needs to outlive that by a margin.
    stall_timeout_min = args.batch_stall_timeout_min
    if stall_timeout_min is None:
        stall_timeout_min = args.timeout_min + NO_PROGRESS_GRACE_MIN
    task_stuck_min = args.task_stuck_min
    logger.info(
        "Batch protection — no-progress backstop: %.0f min | per-task stuck: %.0f "
        "min | wall cap: %s | task timeout: %d min",
        stall_timeout_min,
        task_stuck_min,
        (
            "auto (per chunk)"
            if args.batch_wall_timeout_min is None
            else f"{args.batch_wall_timeout_min:.0f} min"
        ),
        args.timeout_min,
    )

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

            # Per-chunk wall cap. A RUNNING task can't exceed --timeout-min, so the
            # theoretical max healthy duration is `waves * timeout_min` where
            # waves = ceil(chunk_size / parallelism). We use 2x that as a loose
            # safety net (won't falsely kill a healthy batch, but bounds a
            # pathological "creeping progress forever" case). The primary
            # protection is the per-task + no-progress logic, not this cap.
            waves = math.ceil(len(chunk) / args.slurm_array_parallelism)
            batch_wall = (
                args.batch_wall_timeout_min
                if args.batch_wall_timeout_min is not None
                else 2.0 * waves * (args.timeout_min + RUNNING_GRACE_MIN)
            )

            # Block until this batch is fully done before submitting the next one.
            # This is the key throttling mechanism: at most one array in flight.
            completed, failed = wait_for_batch_completion(
                jobs,
                chunk_idx,
                total_chunks,
                stall_timeout_min=stall_timeout_min,
                task_stuck_min=task_stuck_min,
                batch_wall_timeout_min=batch_wall,
                task_timeout_min=args.timeout_min,
            )
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
