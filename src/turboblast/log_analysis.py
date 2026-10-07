"""One-shot analysis of submitit Slurm log directories.

Python port of the former ``submitit_logs_analysis.sh`` helper. Given the
directory that ``turboblaster`` writes its submitit logs into, it counts the
tasks, how many succeeded, and breaks the failures down by likely cause
(permission, OOM/killed, Python exceptions, socket/Slurm, apptainer).
"""

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

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


def analyze_dir(log_dir: Path) -> LogReport:
    """Scan *log_dir* and return aggregated success/failure counts."""
    out_files = sorted(log_dir.glob("*.out"))
    err_files = sorted(log_dir.glob("*.err"))
    files = [*out_files, *err_files]
    contents = {path: _read(path) for path in files}

    total = len(out_files)
    success = sum(1 for path in out_files if _SUCCESS.search(contents[path]))

    categories: dict[str, int] = {}
    for label, pattern, suffixes in _CATEGORIES:
        categories[label] = sum(
            1
            for path in files
            if path.suffix in suffixes and pattern.search(contents[path])
        )

    return LogReport(
        total=total,
        success=success,
        failures=total - success,
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
    args = parser_args()
    log_dir = Path(args.log_dir)
    if not log_dir.is_dir():
        sys.stderr.write(f"Error: Directory {log_dir} does not exist.\n")
        raise SystemExit(1)
    sys.stdout.write(format_report(analyze_dir(log_dir), log_dir) + "\n")


if __name__ == "__main__":
    entrypoint()
