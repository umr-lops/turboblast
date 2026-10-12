"""One-shot analysis of submitit Slurm log directories.

Python port of the former ``submitit_logs_analysis.sh`` helper. Given the
directory that ``turboblaster`` writes its submitit logs into, it counts the
tasks, how many succeeded, and breaks the failures down by likely cause
(permission, OOM/killed, Python exceptions, socket/Slurm, apptainer).

All messages are emitted through the ``logging`` module, and the scan runs
under a live ``tqdm`` progress bar whose postfix shows the running count of
each failure category in real time.
"""

import argparse
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path

from tqdm import tqdm

logger = logging.getLogger(__name__)

# This format matches standard logging practices: [Date Time] [LEVEL] Message
_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
_LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"


def _configure_logger() -> None:
    """Attach a stdout handler to this module's logger (idempotent)."""
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# (label, compiled pattern, file suffixes to scan) — order matches the output.
_CATEGORIES: tuple[tuple[str, re.Pattern[str], tuple[str, ...]], ...] = (
    (
        "Permission Issues",
        re.compile(r"Permission denied|AccessDenied|EACCES", re.IGNORECASE),
        (".out", ".err"),
    ),
    (
        "Killed / Out of Mem",
        re.compile(
            r"Out of memory|Killed|OOM killer|slurm_step_terminate", re.IGNORECASE
        ),
        (".out", ".err"),
    ),
    (
        "Python Exceptions",
        re.compile(r"Traceback \(most recent call last\):"),
        (".err",),
    ),
    (
        "Socket/Slurm Errors",
        re.compile(r"socket|missing socket|confirm allocation", re.IGNORECASE),
        (".out", ".err"),
    ),
    (
        "Apptainer Errors",
        re.compile(r"apptainer: error|FATAL:|image not found", re.IGNORECASE),
        (".out", ".err"),
    ),
)
# Short labels for the live progress bar postfix (kept narrow on purpose).
_CATEGORY_SHORT: dict[str, str] = {
    "Permission Issues": "perm",
    "Killed / Out of Mem": "oom",
    "Python Exceptions": "py-exc",
    "Socket/Slurm Errors": "sock",
    "Apptainer Errors": "appt",
}
_SUCCESS = re.compile(r"completed successfully|success", re.IGNORECASE)


@dataclass
class LogReport:
    """Aggregated counts for a submitit logs directory."""

    total: int
    success: int
    failures: int
    categories: dict[str, int]


def _read(path: Path) -> str:
    """Return the file contents, or an empty string if it cannot be read."""
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _pct(count: int, total: int) -> str:
    """Return *count* as a percentage of *total*, formatted with one decimal."""
    if total == 0:
        return "0.0"
    return f"{(count / total) * 100:.1f}"


def _postfix(categories: dict[str, int]) -> dict[str, int]:
    """Map full category labels to their short progress-bar names."""
    return {_CATEGORY_SHORT[label]: count for label, count in categories.items()}


def analyze_dir(log_dir: Path) -> LogReport:
    """Scan *log_dir* and return aggregated success/failure counts.

    Each log file is read once; the tqdm bar in the postfix shows, in real
    time, how many files matched each failure category so far.
    """
    out_files = sorted(log_dir.glob("*.out"))
    err_files = sorted(log_dir.glob("*.err"))
    files = [*out_files, *err_files]

    success = 0
    categories = {label: 0 for label, _, _ in _CATEGORIES}
    with tqdm(
        files,
        desc="Scanning logs",
        unit="file",
        dynamic_ncols=True,
        leave=False,
        file=sys.stdout,
    ) as pbar:
        pbar.set_postfix(_postfix(categories))
        for path in pbar:
            content = _read(path)
            if path.suffix == ".out" and _SUCCESS.search(content):
                success += 1
            for label, pattern, suffixes in _CATEGORIES:
                if path.suffix in suffixes and pattern.search(content):
                    categories[label] += 1
            pbar.set_postfix(_postfix(categories))

    return LogReport(
        total=len(out_files),
        success=success,
        failures=len(out_files) - success,
        categories=categories,
    )


def format_report(report: LogReport, log_dir: Path) -> str:
    """Render *report* as the human-readable text block."""
    rule = "-" * 64
    lines = [rule, f" ANALYZING SUBMITIT LOGS IN: {log_dir}", rule]

    if report.total == 0:
        lines.append(f"No log files found in {log_dir}")
        return "\n".join(lines)

    lines += [
        f"Total Tasks Scanned: {report.total}",
        rule,
        f"{'SUCCESSFUL TASKS':<25} : {report.success:>5} ({_pct(report.success, report.total):>6}%)",
        f"{'TOTAL FAILURES':<25} : {report.failures:>5} ({_pct(report.failures, report.total):>6}%)",
        rule,
        "BREAKDOWN OF FAILURES (Percentage of Total Tasks):",
    ]
    for label, _, _ in _CATEGORIES:
        count = report.categories[label]
        lines.append(f"  {label:<23} : {count:>5} ({_pct(count, report.total):>6}%)")
    lines.append(rule)

    if report.failures > 0:
        lines.append("HINT: To see a list of failing log files:")
        lines.append(f'grep -L "success" {log_dir}/*.out | head -n 5')
    return "\n".join(lines)


def parser_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for ``slurm-logs``."""
    parser = argparse.ArgumentParser(
        prog="slurm-logs",
        description=(
            "Analyse a submitit Slurm log directory: success/failure counts and a "
            "breakdown of failures by error type."
        ),
    )
    parser.add_argument("log_dir", help="Path to the submitit logs directory")
    return parser.parse_args(argv)


def entrypoint() -> None:
    """Console entry point for the ``slurm-logs`` command."""
    _configure_logger()
    args = parser_args()
    log_dir = Path(args.log_dir)
    if not log_dir.is_dir():
        logger.error("Directory %s does not exist.", log_dir)
        raise SystemExit(1)
    report = analyze_dir(log_dir)
    logger.info(format_report(report, log_dir))


if __name__ == "__main__":
    entrypoint()
