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


class _SacctBypassLogFilter(logging.Filter):
    """Rate-limit submitit's repeated ``Bypassing sacct error`` warnings.

    While the Slurm accounting daemon (slurmdbd) is unreachable, submitit logs
    one such warning per failed query. This filter lets at most one through per
    ``_INTERVAL_S`` and notes how many identical warnings were suppressed.
    """

    _INTERVAL_S = 600.0

    def __init__(self) -> None:
        super().__init__()
        self._last_seen: float = 0.0
        self._suppressed: int = 0

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        if "Bypassing sacct error" not in message:
            return True
        if record.created - self._last_seen < self._INTERVAL_S:
            self._suppressed += 1
            return False
        self._last_seen = record.created
        if self._suppressed and not record.args:
            record.msg = (
                f"{record.msg} [{self._suppressed} identical warning(s) "
                "suppressed since the last one was shown]"
            )
        self._suppressed = 0
        return True


# submitit logs its own "Bypassing sacct error ..." warnings with TWO handlers
# (stdout + stderr) and also propagates them to the root logger (a third copy,
# in our format). Keep a single, rate-limited copy: stop the root propagation
# (submitit keeps its own handlers) and install the filter above.
_submitit_logger = logging.getLogger("submitit")
_submitit_logger.propagate = False
_submitit_logger.addFilter(_SacctBypassLogFilter())

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


def _safe_state(job: submitit.Job) -> str:
    """Return the job's Slurm state, or ``"UNKNOWN"`` if it cannot be read.

    ``submitit`` returns ``"UNKNOWN"`` when its ``sacct`` call fails (e.g. the
    accounting daemon / DBD is unreachable) or the job has no state yet. A few
    UNKNOWNs are normal per-task hiccups; a *whole batch* of them means the
    state source itself is down (see the blind guard in ``ArrayBatch.poll``).
    ``KeyboardInterrupt`` / ``SystemExit`` always propagate.
    """
    try:
        return job.state
    except (
        submitit.core.utils.FailedJobError,
        AttributeError,
        RuntimeError,
    ) as e:
        # Ces exceptions sont attendues dans le contexte submitit/Slurm
        logger.debug("Job state query failed: %s: %s", type(e).__name__, e)
        return "UNKNOWN"
    except OSError as e:
        logger.warning("Unexpected error in job state polling: %s", e, exc_info=True)
        return "UNKNOWN"


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


def _query_state(job: submitit.Job) -> str:
    """Force a fresh state read for one job; ``"UNKNOWN"`` if unreadable."""
    try:
        return job.get_info(mode="force").get("State") or "UNKNOWN"
    except (
        submitit.core.utils.FailedJobError,
        AttributeError,
        RuntimeError,
        OSError,
    ):
        return "UNKNOWN"


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

    Tasks whose state was already ``UNKNOWN`` when the backstop fired are
    cancelled **blind** (their real state is unreadable — e.g. the accounting
    daemon is down, issue #20). After the cancellation a fresh forced state
    read is attempted for every remaining task; whatever is still unreadable is
    reported as ``state_unverified`` rather than silently counted.
    """
    remaining = [j for j, s in job_states if not is_terminal_state(s)]
    blind_remaining = [j for j, s in job_states if s == "UNKNOWN"]
    job_ids = sorted(j.job_id for j in remaining)
    logger.warning(
        "Batch %d/%d: %s — cancelling %d remaining task(s)%s: %s",
        chunk_idx,
        total_chunks,
        reason,
        len(remaining),
        (
            f" (BLIND: {len(blind_remaining)} task(s) had UNKNOWN state — they "
            "may not be stuck at all)"
            if blind_remaining
            else ""
        ),
        job_ids,
    )
    if job_ids:
        subprocess.run(["scancel", *job_ids], check=False)
    # Wait one cycle for Slurm to process the cancellations.
    time.sleep(BATCH_POLL_INTERVAL)
    # Re-query the still-non-terminal tasks so final counts use verified states
    # where the state source has come back in the meantime.
    verified: dict[submitit.Job, str] = {
        j: _query_state(j) for j, s in job_states if not is_terminal_state(s)
    }
    unverified = sum(1 for s in verified.values() if s == "UNKNOWN")
    # Previously-terminal COMPLETED jobs + any still-running job that reached
    # COMPLETED in the meantime.
    completed = states.get("COMPLETED", 0) + sum(
        1 for s in verified.values() if s == "COMPLETED"
    )
    failed = total - completed
    postfix: dict[str, object] = {
        "completed": completed,
        "failed": failed,
        "cancelled": len(remaining),
    }
    if unverified:
        postfix["state_unverified"] = unverified
    pbar.set_postfix(postfix, refresh=True)
    logger.warning(
        "Batch %d/%d force-finished — completed=%d  failed=%d  cancelled=%d%s",
        chunk_idx,
        total_chunks,
        completed,
        failed,
        len(remaining),
        (
            f"  state_unverified={unverified} (state source still down — check "
            "with 'sacct -j <array_id>' later)"
            if unverified
            else ""
        ),
    )
    return completed, failed


class ArrayBatch:
    """State and polling for one in-flight Slurm job array (a "batch").

    Encapsulates the per-batch stuck-detection / backstop logic so that several
    arrays can be polled concurrently by :func:`main` (pipelined submission) or
    one at a time by :func:`wait_for_batch_completion`.

    Call :meth:`poll` once per cycle (passing a monotonic timestamp).  It returns
    ``True`` when the batch has reached a final state — every task terminal, or a
    backstop force-finished it — at which point :attr:`completed` / :attr:`failed`
    are set and the progress bar is closed.

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
    * **Blind guard** (``blind_timeout_min``) — when *every* task reports
      ``UNKNOWN`` in a poll cycle, the Slurm state source itself (sacct/DBD) is
      unreachable and the states are meaningless (issue #20): the stall and
      wall clocks are paused (re-based) for as long as that lasts, so a
      transient outage can never blind-cancel a healthy batch. If the source
      stays down for ``blind_timeout_min`` minutes, the batch is given up
      **without cancelling** and its unreadable tasks are reported as failed
      but flagged ``state_unverified``.
    """

    def __init__(
        self,
        jobs: list[submitit.Job],
        chunk_idx: int,
        total_chunks: int,
        *,
        stall_timeout_min: float | None = None,
        task_stuck_min: float = 15.0,
        batch_wall_timeout_min: float | None = None,
        task_timeout_min: int = 20,
        blind_timeout_min: float | None = None,
        position: int = 0,
        leave: bool = True,
    ) -> None:
        self.jobs = jobs
        self.total = len(jobs)
        self.chunk_idx = chunk_idx
        self.total_chunks = total_chunks
        self.stall_timeout_min = stall_timeout_min
        self.task_stuck_min = task_stuck_min
        self.batch_wall_timeout_min = batch_wall_timeout_min
        self.task_timeout_min = task_timeout_min
        self.position = position
        self.completed = 0
        self.failed = 0
        self.done = False
        now = time.monotonic()
        # last_progress_time: reset whenever a task reaches a terminal state.
        self.batch_start = now
        self.last_progress_time = now
        # Per-task (state, time-when-that-state-started); the clock resets on a
        # state change so a task queued (PENDING) long then starting RUNNING is
        # measured from its RUNNING start, not from when it first went non-terminal.
        self.state_since: dict[submitit.Job, tuple[str, float]] = {}
        self.prev_terminal = 0
        # Blind-state (state source unreachable) tracking: when the whole batch
        # reports UNKNOWN, the backstop clocks are paused (last-progress and
        # batch-start are re-based each cycle so they don't accumulate while
        # blind) and the blind clock below bounds how long we wait for the
        # state source to recover before giving up *without* cancelling.
        self._blind = False
        self._blind_since: float | None = None
        # Max minutes the state source (sacct/slurmdbd) may stay unreachable
        # before the batch is given up without cancelling (see poll()).
        blind_cap = blind_timeout_min
        if blind_cap is None or blind_cap <= 0:
            blind_cap = (
                2.0 * stall_timeout_min
                if stall_timeout_min is not None and stall_timeout_min > 0
                else 120.0
            )
        self.blind_timeout_min = blind_cap
        self.pbar = tqdm(
            total=self.total,
            desc=f"Batch {chunk_idx}/{total_chunks}",
            unit="task",
            dynamic_ncols=True,
            leave=leave,
            position=position,
            file=sys.stdout,
        )
        logger.debug(
            "Waiting for batch %d/%d to complete (%d tasks)... "
            "stall_timeout=%smin  task_stuck=%smin  wall_timeout=%smin  task_timeout=%dmin",
            chunk_idx,
            total_chunks,
            self.total,
            stall_timeout_min,
            task_stuck_min,
            batch_wall_timeout_min,
            task_timeout_min,
        )

    def set_position(self, position: int) -> None:
        """Move this batch's progress bar to a new terminal line (pipelining)."""
        self.position = position
        self.pbar.position = position
        self.pbar.refresh()

    def _give_up_blind(self, states: dict[str, int], now: float) -> None:
        """Give up on a batch whose state source never came back.

        Unlike the backstops, nothing is cancelled: the tasks may be running
        fine and we simply cannot verify it. The unreadable tasks are reported
        as failed (so ``--fail-fast`` still reacts) but the log and progress
        bar make it explicit that their state is unverified.
        """
        blind_min = (now - (self._blind_since or now)) / 60.0
        self.completed = states.get("COMPLETED", 0)
        self.failed = self.total - self.completed
        self.pbar.set_postfix(
            {
                "completed": self.completed,
                "failed": self.failed,
                "state_unverified": self.failed,
            },
            refresh=True,
        )
        logger.error(
            "Batch %d/%d: state source (sacct/slurmdbd) unreachable for %.0f min "
            "(blind cap %.0f min). Giving up WITHOUT cancelling — completed=%d, "
            "failed=%d but state_unverified: check the array with "
            "'sacct -j <array_id>' once accounting is back.",
            self.chunk_idx,
            self.total_chunks,
            blind_min,
            self.blind_timeout_min,
            self.completed,
            self.failed,
        )
        self._finish()

    def _finish(self) -> None:
        """Mark the batch done and finalize its progress bar."""
        self.done = True
        self.pbar.refresh()
        self.pbar.close()

    def poll(self, now: float) -> bool:
        """Advance one poll cycle; return True if the batch is now finished."""
        # ── Collect per-task states via submitit (wraps squeue/sacct) ──
        job_states = [(job, _safe_state(job)) for job in self.jobs]
        states: dict[str, int] = {}
        for _, state in job_states:
            states[state] = states.get(state, 0) + 1

        # ── Count terminal tasks ──
        n_terminal = sum(1 for _, s in job_states if is_terminal_state(s))

        # ── Blind guard: is the state source itself unreachable? ──
        # When *every* task reads UNKNOWN, submitit's single sacct query failed
        # (e.g. slurmdbd/DBD outage). The states are worthless: the backstop
        # clocks must not advance on them (a healthy batch would otherwise be
        # blind-cancelled). We pause by re-basing the clocks each blind cycle
        # and bound the wait with the blind clock below.
        blind = 0 < self.total == states.get("UNKNOWN", 0)
        if blind:
            self._blind_since = now if self._blind_since is None else self._blind_since
            self.last_progress_time = now
            self.batch_start = now
            if not self._blind:
                self._blind = True
                logger.warning(
                    "Batch %d/%d: all %d task(s) report UNKNOWN state — the Slurm "
                    "state source (sacct/slurmdbd) appears unreachable. Backstops "
                    "paused; will give up after %.0f min of blindness WITHOUT "
                    "cancelling (check slurmdbd on the cluster).",
                    self.chunk_idx,
                    self.total_chunks,
                    self.total,
                    self.blind_timeout_min,
                )
            if (now - self._blind_since) / 60.0 >= self.blind_timeout_min:
                self._give_up_blind(states, now)
                return True
        elif self._blind:
            self._blind = False
            self._blind_since = None
            logger.info(
                "Batch %d/%d: Slurm state source recovered after blindness — "
                "resuming backstops.",
                self.chunk_idx,
                self.total_chunks,
            )

        # ── Per-task stuck detection ──
        # Track how long each non-terminal task has been in its *current* state
        # (resetting the clock on state change) and cancel any task whose state
        # has exceeded its state-aware threshold. Works regardless of whether the
        # rest of the batch is still progressing.
        stuck_jobs: list[submitit.Job] = []
        for job, state in job_states:
            if is_terminal_state(state):
                self.state_since.pop(job, None)
                continue
            prev = self.state_since.get(job)
            if prev is None or prev[0] != state:
                # New task, or just changed state -> (re)start its clock.
                self.state_since[job] = (state, now)
                continue  # don't judge it until the next poll (fair clock)
            _, since = self.state_since[job]
            stuck_min = (now - since) / 60.0
            threshold = stuck_threshold_minutes(
                state, self.task_timeout_min, self.task_stuck_min
            )
            if threshold is not None and stuck_min >= threshold:
                stuck_jobs.append(job)
                logger.warning(
                    "Batch %d/%d: task %s stuck in %s for %.0f min "
                    "(threshold %.0f min) — cancelling",
                    self.chunk_idx,
                    self.total_chunks,
                    job.job_id,
                    state,
                    stuck_min,
                    threshold,
                )
        if stuck_jobs:
            job_ids = sorted(j.job_id for j in stuck_jobs)
            # A submitit array job_id is "base_task" (e.g. 528266_108); cancelling
            # the base id would scancel the whole array, not just the stuck tasks.
            subprocess.run(["scancel", *job_ids], check=False)
            # Drop them from tracking so they don't re-trigger; they'll show as
            # CANCELLED on the next poll.
            for j in stuck_jobs:
                self.state_since.pop(j, None)

        # ── Advance progress bar / reset no-progress clock ──
        delta = n_terminal - self.prev_terminal
        if delta > 0:
            self.pbar.update(delta)
            self.last_progress_time = now

        # ── No-progress backstop ──
        stalled_min = (now - self.last_progress_time) / 60.0
        # ── Batch wall-time cap ──
        wall_min = (now - self.batch_start) / 60.0

        if (
            self.stall_timeout_min is not None
            and self.stall_timeout_min > 0
            and stalled_min >= self.stall_timeout_min
        ):
            self.completed, self.failed = _force_finish_batch(
                job_states,
                states,
                self.total,
                self.pbar,
                self.chunk_idx,
                self.total_chunks,
                f"stalled for {stalled_min:.0f} min with no progress "
                f"(threshold {self.stall_timeout_min:.0f} min)",
            )
            self._finish()
            return True

        if (
            self.batch_wall_timeout_min is not None
            and self.batch_wall_timeout_min > 0
            and wall_min >= self.batch_wall_timeout_min
        ):
            self.completed, self.failed = _force_finish_batch(
                job_states,
                states,
                self.total,
                self.pbar,
                self.chunk_idx,
                self.total_chunks,
                f"exceeded batch wall-time cap of "
                f"{self.batch_wall_timeout_min:.0f} min (now at {wall_min:.0f} min)",
            )
            self._finish()
            return True

        self.prev_terminal = n_terminal

        # ── Build a compact postfix showing every non-zero state ──
        # Prioritise actionable states first (RUNNING, PENDING) then failures, so
        # the most important info is never truncated.
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
        # Show countdowns when the respective backstop is enabled.
        if self.stall_timeout_min is not None and self.stall_timeout_min > 0:
            postfix_parts["stall"] = (
                f"{int(stalled_min)}/{int(self.stall_timeout_min)}min"
            )
        if self.batch_wall_timeout_min is not None and self.batch_wall_timeout_min > 0:
            postfix_parts["wall"] = (
                f"{int(wall_min)}/{int(self.batch_wall_timeout_min)}min"
            )
        if self._blind:
            postfix_parts["blind"] = (
                f"{int((now - (self._blind_since or now)) / 60.0)}/"
                f"{int(self.blind_timeout_min)}min"
            )
        self.pbar.set_postfix(postfix_parts)

        parts = [f"{s}={n}" for s, n in sorted(states.items())]
        logger.debug(
            "Batch %d/%d — total=%d  terminal=%d  [%s]  stalled=%.0fmin  wall=%.0fmin",
            self.chunk_idx,
            self.total_chunks,
            self.total,
            n_terminal,
            "  ".join(parts),
            stalled_min,
            wall_min,
        )

        if n_terminal >= self.total:
            self.completed = states.get("COMPLETED", 0)
            self.failed = self.total - self.completed
            self.pbar.set_postfix(
                {"completed": self.completed, "failed": self.failed},
                refresh=True,
            )
            logger.info(
                "Batch %d/%d finished — completed=%d  failed=%d",
                self.chunk_idx,
                self.total_chunks,
                self.completed,
                self.failed,
            )
            self._finish()
            return True

        return False


def wait_for_batch_completion(
    jobs: list[submitit.Job],
    chunk_idx: int,
    total_chunks: int,
    stall_timeout_min: float | None = None,
    task_stuck_min: float = 15.0,
    batch_wall_timeout_min: float | None = None,
    task_timeout_min: int = 20,
    blind_timeout_min: float | None = None,
) -> tuple[int, int]:
    """Blocks until every job in a batch reaches a terminal Slurm state.

    Thin blocking wrapper over :class:`ArrayBatch`: polls every
    ``BATCH_POLL_INTERVAL`` seconds until the batch finishes, showing a live tqdm
    progress bar.  Used for the sequential (one array at a time) path; the
    pipelined path in :func:`main` polls several :class:`ArrayBatch` concurrently.

    See :class:`ArrayBatch` for the stuck-task / backstop semantics.

    Args:
        jobs (list[submitit.Job]): Job handles returned by ``map_array()``.
        chunk_idx (int): 1-based index of this chunk (for logging).
        total_chunks (int): Total number of chunks (for logging).
        stall_timeout_min (float | None): No-progress backstop (minutes).
            ``None`` (or ``0``) = disabled.
        task_stuck_min (float): Per-task stuck timeout for hold/transient states.
        batch_wall_timeout_min (float | None): Hard cap on batch wall time.
        task_timeout_min (int): The ``--timeout-min`` value (bounds RUNNING tasks).
        blind_timeout_min (float | None): Max minutes to wait for the Slurm
            state source when the whole batch reads UNKNOWN. ``None`` = auto
            (2x the no-progress backstop, or 120 min if it is disabled).

    Returns:
        tuple[int, int]: ``(completed, failed)`` task counts.
    """
    batch = ArrayBatch(
        jobs,
        chunk_idx,
        total_chunks,
        stall_timeout_min=stall_timeout_min,
        task_stuck_min=task_stuck_min,
        batch_wall_timeout_min=batch_wall_timeout_min,
        task_timeout_min=task_timeout_min,
        blind_timeout_min=blind_timeout_min,
    )
    while True:
        time.sleep(BATCH_POLL_INTERVAL)
        if batch.poll(time.monotonic()):
            return batch.completed, batch.failed


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
        "--max-arrays-inflight",
        type=int,
        default=2,
        help=(
            "Maximum number of job arrays (batches) kept in flight at the same "
            "time (pipelined submission). Each array is one Slurm job, so this "
            "must stay under the cluster's MaxJobCount. Overall concurrency = "
            "--max-arrays-inflight x --slurm-array-parallelism. "
            "1 = the previous strictly-sequential behavior. Default: 2."
        ),
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
            "chain. Default: auto, per chunk (2x the theoretical max healthy "
            "duration: 2 x ceil(chunk/parallelism) x (--timeout-min + 15)). "
            "Use 0 to disable."
        ),
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main submission logic
# ---------------------------------------------------------------------------


def main(args: argparse.Namespace) -> int:
    """Configures and submits job array chunks to the Slurm cluster (pipelined).

    Strategy:
      1. Split the input listing into chunks of ``CHUNK_SIZE`` (<= 1000 = the
         cluster ``MaxArraySize``).
      2. Submit up to ``--max-arrays-inflight`` chunks (each a Slurm job array)
         and keep polling them all.
      3. As soon as an array reaches a terminal state (or is force-finished by
         the stuck-task backstops), submit the next pending chunk in its place.
      4. Repeat until every chunk is submitted and all in-flight arrays have
         finished.

    Keeping several arrays in flight (pipelining) means the cluster is never
    idle waiting for an entire array to drain before the next one starts, while
    the number of jobs in the queue stays bounded by ``--max-arrays-inflight``
    (keep it under the cluster ``MaxJobCount``).  Overall concurrency is
    ``--max-arrays-inflight x --slurm-array-parallelism``.  Each array keeps the
    Phase-1 stuck-task protection (see :class:`ArrayBatch`).

    Submission failures (e.g. the Slurm controller briefly unreachable, issue
    #24) do NOT kill the run: each submission is retried up to
    ``SUBMIT_MAX_RETRIES`` times with linear backoff, and if that is exhausted
    the chunk is left pending and retried again on every subsequent poll
    cycle (``BATCH_POLL_INTERVAL`` seconds apart) until the controller is back.
    Only when the controller has stayed unreachable for ``blind_timeout_min``
    minutes (the same cap used by :class:`ArrayBatch`'s blind guard) AND no
    in-flight array is left to monitor does this function give up: it returns
    ``1`` (a clean exit, nothing is cancelled) after logging which tasks were
    never submitted.

    Returns:
        int: ``0`` on success, ``1`` if the run was given up because the Slurm
        controller stayed unreachable for the blind cap.

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
        "Submission strategy: pipelined — up to %d job array(s) in flight at once",
        args.max_arrays_inflight,
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
        return 0

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
    # How long the whole batch may stay "blind" (state source unreachable, all
    # states UNKNOWN) before it is given up *without* cancelling. 0 = disabled
    # (wait forever). Default: 2x the no-progress backstop.
    blind_timeout_min = (
        2.0 * stall_timeout_min
        if stall_timeout_min is not None and stall_timeout_min > 0
        else 120.0
    )
    logger.info(
        "Batch protection — no-progress backstop: %.0f min | per-task stuck: %.0f "
        "min | wall cap: %s | task timeout: %d min | blind (state source down): "
        "%.0f min",
        stall_timeout_min,
        task_stuck_min,
        (
            "auto (per chunk)"
            if args.batch_wall_timeout_min is None
            else f"{args.batch_wall_timeout_min:.0f} min"
        ),
        args.timeout_min,
        blind_timeout_min,
    )

    # Guard against a non-positive pipeline depth (0 would make the fill loop never
    # submit and the poll loop spin forever).
    max_inflight = max(1, int(args.max_arrays_inflight))

    # Precompute the chunks (each <= CHUNK_SIZE) that will be submitted.
    chunks = [
        array_inputs[i : i + CHUNK_SIZE]
        for i in range(0, len(array_inputs), CHUNK_SIZE)
    ]
    next_chunk_idx = 0
    in_flight: list[ArrayBatch] = []
    global_completed = 0
    global_failed = 0
    # --fail-fast: once set, no more arrays are submitted; in-flight ones drain.
    fail_fast_stop = False
    # When a submission is impossible (Slurm controller unreachable, issue
    # #24): the monotonic time the outage started, or None when submissions
    # work. Bounds how long the run keeps retrying an unsubmittable chunk with
    # nothing in flight to monitor (the "blind" state of the submission side).
    submit_unavailable_since: float | None = None

    def _submit_next() -> bool:
        """Try to submit the next pending chunk (if any) and add it to in_flight.

        Returns False if there are no more chunks to submit, or if the
        submission failed: in that case the chunk is left pending
        (``next_chunk_idx`` is NOT advanced) so the poll loop retries it on a
        later cycle. A transient Slurm controller outage (issue #24) must
        therefore neither kill the run nor lose a chunk's tasks.
        """
        nonlocal next_chunk_idx
        nonlocal submit_unavailable_since
        if next_chunk_idx >= len(chunks):
            return False
        chunk_idx = next_chunk_idx
        chunk = chunks[next_chunk_idx]
        logger.debug(
            "=== Submitting batch %d/%d — tasks %d to %d (%d tasks) ===",
            chunk_idx + 1,
            total_chunks,
            chunk_idx * CHUNK_SIZE,
            chunk_idx * CHUNK_SIZE + len(chunk) - 1,
            len(chunk),
        )
        try:
            jobs = submit_chunk_with_retry(
                executor, process_func, chunk, chunk_idx + 1, total_chunks
            )
        except ChunkSubmissionError:
            # All SUBMIT_MAX_RETRIES attempts failed (e.g. "Unable to contact
            # slurm controller"): stay up, keep monitoring the in-flight
            # arrays, and retry this same chunk on the next poll cycle.
            if submit_unavailable_since is None:
                submit_unavailable_since = time.monotonic()
            # logger.error (not exception): the failure is expected and already
            # logged by submit_chunk_with_retry; a full traceback every poll
            # cycle during an outage would be pure noise.
            logger.error(  # noqa: TRY400
                "Chunk %d/%d could not be submitted (Slurm controller "
                "unreachable?). Keeping the run alive: the chunk stays pending "
                "and will be retried on the next poll cycle (%d s); in-flight "
                "arrays keep being monitored.",
                chunk_idx + 1,
                total_chunks,
                BATCH_POLL_INTERVAL,
            )
            return False
        # Successful submission: reset the outage clock and advance only now,
        # so a failed chunk is never skipped.
        submit_unavailable_since = None
        next_chunk_idx += 1
        # Per-chunk wall cap. A RUNNING task can't exceed --timeout-min, so the
        # theoretical max healthy duration is `waves * timeout_min` where
        # waves = ceil(chunk_size / parallelism). We use 2x that as a loose
        # safety net (won't falsely kill a healthy batch, but bounds a
        # pathological "creeping progress forever" case). The primary protection
        # is the per-task + no-progress logic, not this cap.
        waves = math.ceil(len(chunk) / args.slurm_array_parallelism)
        batch_wall = (
            args.batch_wall_timeout_min
            if args.batch_wall_timeout_min is not None
            else 2.0 * waves * (args.timeout_min + RUNNING_GRACE_MIN)
        )
        in_flight.append(
            ArrayBatch(
                jobs,
                chunk_idx + 1,
                total_chunks,
                stall_timeout_min=stall_timeout_min,
                task_stuck_min=task_stuck_min,
                batch_wall_timeout_min=batch_wall,
                task_timeout_min=args.timeout_min,
                blind_timeout_min=blind_timeout_min,
            )
        )
        return True

    # Outer progress bar tracks batch-level progress across all chunks.
    with tqdm(
        total=total_chunks,
        desc="Overall batches",
        unit="batch",
        dynamic_ncols=True,
        leave=True,
        file=sys.stdout,
    ) as outer_pbar:
        # Fill the pipeline up to the max number of in-flight arrays.
        while len(in_flight) < max_inflight and _submit_next():
            pass

        # Keep progress-bar line positions stable (0..N-1).
        for i, b in enumerate(in_flight):
            b.set_position(i)

        # Main poll loop: poll all in-flight arrays, remove the finished ones
        # (freeing a slot), and submit the next pending chunk into that slot.
        # With --fail-fast, stop submitting once a batch reports failures, but
        # still drain the arrays already in flight.
        while in_flight or (not fail_fast_stop and next_chunk_idx < len(chunks)):
            for b in in_flight:
                b.poll(time.monotonic())

            # Remove finished arrays (releasing their slot for the next chunk).
            newly_done = [b for b in in_flight if b.done]
            if newly_done:
                for b in newly_done:
                    in_flight.remove(b)
                    global_completed += b.completed
                    global_failed += b.failed
                    outer_pbar.update(1)
                outer_pbar.set_postfix(
                    {
                        "completed": global_completed,
                        "failed": global_failed,
                    }
                )
                if args.fail_fast and any(b.failed for b in newly_done):
                    failed_batch = next(b for b in newly_done if b.failed)
                    fail_fast_stop = True
                    logger.error(
                        "Batch %d/%d had %d failure(s) and --fail-fast is set. "
                        "Stopping submission of the remaining %d batch(es); "
                        "draining the %d in-flight.",
                        failed_batch.chunk_idx,
                        failed_batch.total_chunks,
                        failed_batch.failed,
                        len(chunks) - next_chunk_idx,
                        len(in_flight),
                    )

            # Refill the pipeline while there are free slots and pending chunks.
            # (_submit_next() returns False on submission failure, so the loop
            # cannot spin while the controller is down.)
            if not fail_fast_stop:
                while (
                    len(in_flight) < max_inflight
                    and next_chunk_idx < len(chunks)
                    and _submit_next()
                ):
                    pass

            # Give up — cleanly — when the Slurm controller is unreachable,
            # there is nothing left in flight to monitor, and the outage has
            # lasted longer than the blind cap (issue #24): after this point
            # there is nothing to do but wait for a human to restart the run.
            # Returning 1 (instead of letting ChunkSubmissionError escape)
            # keeps the exit clean and bounded.
            if submit_unavailable_since is not None and not in_flight:
                outage_min = (time.monotonic() - submit_unavailable_since) / 60.0
                if outage_min >= blind_timeout_min:
                    pending_tasks = sum(len(c) for c in chunks[next_chunk_idx:])
                    logger.error(
                        "Giving up: the Slurm controller has been unreachable "
                        "for %.0f min (blind cap %.0f min) and no array is "
                        "left in flight to monitor. %d task(s) were never "
                        "submitted; completed=%d  failed=%d so far. Re-run "
                        "turboblaster with the same --listing-input once the "
                        "controller is back.",
                        outage_min,
                        blind_timeout_min,
                        pending_tasks,
                        global_completed,
                        global_failed,
                    )
                    return 1

            # Reposition bars after removals/additions.
            for i, b in enumerate(in_flight):
                b.set_position(i)

            time.sleep(BATCH_POLL_INTERVAL)

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
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def entrypoint() -> None:
    """Script entry point.

    Parses CLI arguments and delegates to :func:`main`. Exits with a non-zero
    status if the run was given up (e.g. the Slurm controller stayed
    unreachable, issue #24), and with ``0`` otherwise.
    """
    args = parser_args()
    sys.exit(main(args))


if __name__ == "__main__":
    entrypoint()
