"""Live monitor for active Slurm job arrays.

Python port of the former ``monitore_jobarrays_slurm.sh`` helper. It renders a
full-screen, auto-refreshing table of the user's active Slurm job arrays
(running / pending / completing / success / failed / total / done %). The data
is gathered with the standard Slurm client tools (``squeue``, ``sacct``,
``scontrol``); no extra dependency is required.
"""

import argparse
import subprocess
import sys
import time
from dataclasses import dataclass

# Column layout: (header, width). The widths are the single source of truth
# for both the header and the data rows, so the table can never drift out of
# alignment (the class of bug the hand-written ASCII separator used to have).
COLUMNS: tuple[tuple[str, int], ...] = (
    ("ARRAY_ID", 10),
    ("NAME", 20),
    ("MEM", 8),
    ("STARTED", 11),
    ("RUN", 3),
    ("PEN", 3),
    ("CG", 3),
    ("SUCCESS", 7),
    ("FAIL", 4),
    ("TOTAL", 5),
    ("DONE%", 4),
)

_ID_W = 10
_NAME_W = 20
_MEM_W = 8
_STARTED_W = 11

# Slurm states counted in the FAIL column.
FAILURE_STATES: tuple[str, ...] = (
    "FAILED",
    "TIMEOUT",
    "CANCELLED",
    "NODE_FAIL",
    "BOOT_FAIL",
    "OUT_OF_MEMORY",
    "DEADLINE",
)


@dataclass
class JobStats:
    """Aggregated per-task states for one (array) job."""

    job_id: str
    name: str
    mem: str
    started: str
    running: int
    pending: int
    completing: int
    success: int
    failed: int
    total: int

    @property
    def done_percent(self) -> int:
        """Percentage of the declared total that reached a terminal state."""
        if self.total <= 0:
            return 0
        return (self.success + self.failed) * 100 // self.total


def _run(cmd: list[str]) -> str:
    """Run *cmd* and return its stdout.

    Never raises on a non-zero exit status (squeue/sacct/scontrol may return
    non-zero) and returns an empty string if the executable is missing, so the
    monitor degrades to an empty report instead of crashing.
    """
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except OSError:
        return ""
    return proc.stdout


def fetch_job_ids(job_id: str | None) -> list[str]:
    """Return the base job IDs to monitor.

    If *job_id* is given it is used as-is (single target); otherwise the list
    of the user's active job array base IDs is read from ``squeue``.
    """
    if job_id is not None:
        return [job_id]
    out = _run(["squeue", "--me", "-h", "-o", "%F"])
    return sorted({line for line in out.split() if line})


def fetch_declared_total(job_id: str) -> int:
    """Return the declared array size (batch x batch_size) for *job_id*.

    Reads ``ArrayTaskCount`` from ``scontrol show job``. This is the total
    number of tasks that will be submitted, which ``sacct`` under-counts
    because it only lists tasks that already have an accounting record.
    Returns 0 for non-array jobs or once the job is purged.
    """
    out = _run(["scontrol", "show", "job", job_id])
    for line in out.splitlines():
        for field in line.split():
            if field.startswith("ArrayTaskCount="):
                value = field.split("=", 1)[1]
                return int(value) if value.isdigit() else 0
    return 0


def _format_start(raw: str) -> str:
    """Shorten a ``YYYY-MM-DDTHH:MM:SS`` start time to ``MM-DD HH:MM``."""
    if raw in ("Unknown", "None"):
        return "Pending"
    return raw[5:16].replace("T", " ")


def parse_sacct(job_id: str, raw: str, declared_total: int) -> JobStats:
    """Parse raw ``sacct -X`` output into a :class:`JobStats`.

    The first line (the array/parent record) supplies NAME, MEM and STARTED.
    Per-task states are counted on the individual task lines only: for an array
    (``declared_total > 0``) the parent line (JobIDRaw == *job_id*) is excluded
    so each task is counted exactly once.
    """
    lines = [line.split() for line in raw.splitlines() if line.strip()]
    first = lines[0] if lines else []
    name = first[2][:_NAME_W] if len(first) > 2 else ""
    mem = first[3][:_MEM_W] if len(first) > 3 else "-"
    started = _format_start(first[4])[:_STARTED_W] if len(first) > 4 else "-"

    if declared_total > 0:
        total = declared_total
        task_lines = [line for line in lines if line and line[0] != job_id]
    else:
        total = len(lines)
        task_lines = lines

    running = sum(
        1 for line in task_lines if len(line) > 1 and line[1].startswith("RUNNING")
    )
    pending = sum(
        1 for line in task_lines if len(line) > 1 and line[1].startswith("PENDING")
    )
    completing = sum(
        1 for line in task_lines if len(line) > 1 and line[1].startswith("COMPLETING")
    )
    success = sum(
        1 for line in task_lines if len(line) > 1 and line[1].startswith("COMPLETED")
    )
    failed = sum(
        1 for line in task_lines if len(line) > 1 and line[1].startswith(FAILURE_STATES)
    )

    return JobStats(
        job_id=job_id[:_ID_W],
        name=name,
        mem=mem,
        started=started,
        running=running,
        pending=pending,
        completing=completing,
        success=success,
        failed=failed,
        total=total,
    )


def format_row(values: list[str]) -> str:
    """Format one table row from *values* using the :data:`COLUMNS` widths."""
    return " | ".join(
        value.ljust(width) for (_, width), value in zip(COLUMNS, values, strict=True)
    )


def _separator() -> str:
    """Build the dashed separator line matching the :data:`COLUMNS` widths."""
    return " | ".join("-" * width for (_, width) in COLUMNS)


def _not_found_row(job_id: str) -> str:
    return format_row(
        [job_id[:_ID_W], "NOT_FOUND", "-", "-", "0", "0", "0", "0", "0", "0", "0%"]
    )


def build_report(job_id: str | None) -> str:
    """Build the full report text (title, header, separator, data rows)."""
    now = time.strftime("%H:%M:%S")
    if job_id is not None:
        title = f"SLURM REPORT FOR JOB: {job_id} [{now}]"
    else:
        title = f"SLURM ACTIVE ARRAYS SUMMARY [{now}]"

    rule = _separator()
    header = format_row([col for col, _ in COLUMNS])
    lines = [title, rule, header, rule]

    ids = fetch_job_ids(job_id)
    if not ids:
        lines.append(
            f"Job {job_id} not found." if job_id is not None else "No active jobs."
        )
    for jid in ids:
        raw = _run(
            [
                "sacct",
                "-j",
                jid,
                "-X",
                "-n",
                "--format=JobIDRaw,State,JobName%50,ReqMem,Start",
            ]
        )
        if not raw.strip():
            lines.append(_not_found_row(jid))
            continue
        stats = parse_sacct(jid, raw, fetch_declared_total(jid))
        lines.append(
            format_row(
                [
                    stats.job_id,
                    stats.name,
                    stats.mem,
                    stats.started,
                    str(stats.running),
                    str(stats.pending),
                    str(stats.completing),
                    str(stats.success),
                    str(stats.failed),
                    str(stats.total),
                    f"{stats.done_percent}%",
                ]
            )
        )
    lines.append(rule)
    return "\n".join(lines)


def live_monitor(job_id: str | None, interval: float = 2.0) -> None:
    """Render the live, auto-refreshing report until interrupted.

    Hides the cursor, redraws the report every *interval* seconds, and restores
    the cursor on exit (including ``Ctrl-C``).
    """
    sys.stdout.write("\033[?25l")
    sys.stdout.flush()
    try:
        while True:
            sys.stdout.write("\033[H\033[J")
            sys.stdout.write(build_report(job_id) + "\n")
            sys.stdout.flush()
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        sys.stdout.write("\033[?25h")
        sys.stdout.flush()


def parser_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for ``slurm-monitor``."""
    parser = argparse.ArgumentParser(
        prog="slurm-monitor",
        description=(
            "Live view of your active Slurm job arrays (RUNNING / PENDING / "
            "COMPLETING / SUCCESS / FAILED / TOTAL / DONE%). Press Ctrl-C to quit."
        ),
    )
    parser.add_argument(
        "job_id",
        nargs="?",
        default=None,
        help=(
            "Optional Slurm (array) job ID to follow. "
            "If omitted, all of your active jobs are shown."
        ),
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Refresh interval in seconds (default: 2)",
    )
    return parser.parse_args(argv)


def entrypoint() -> None:
    """Console entry point for the ``slurm-monitor`` command."""
    args = parser_args()
    live_monitor(args.job_id, args.interval)


if __name__ == "__main__":
    entrypoint()
