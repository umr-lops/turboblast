from pathlib import Path
from unittest.mock import patch

import pytest

from turboblast.log_analysis import (
    LogReport,
    _pct,
    analyze_dir,
    entrypoint,
    format_report,
    parser_args,
)

# ─── _pct ────────────────────────────────────────────────────────────────────


class TestPct:
    @pytest.mark.parametrize(
        ("count", "total", "expected"),
        [
            (1, 3, "33.3"),
            (1, 1, "100.0"),
            (0, 5, "0.0"),
            (2, 8, "25.0"),
        ],
    )
    def test_values(self, count, total, expected):
        assert _pct(count, total) == expected

    def test_zero_total(self):
        assert _pct(3, 0) == "0.0"


# ─── analyze_dir ─────────────────────────────────────────────────────────────


class TestAnalyzeDir:
    def _write(self, log_dir: Path) -> None:
        (log_dir / "task_a.out").write_text(
            "completed successfully\n", encoding="utf-8"
        )
        (log_dir / "task_a.err").write_text(
            "Traceback (most recent call last):\n", encoding="utf-8"
        )
        (log_dir / "task_b.out").write_text("Permission denied\n", encoding="utf-8")
        (log_dir / "task_c.out").write_text("nothing to see\n", encoding="utf-8")

    def test_counts(self, tmp_path):
        self._write(tmp_path)
        report = analyze_dir(tmp_path)
        assert report.total == 3
        assert report.success == 1
        assert report.failures == 2
        assert report.categories["Permission Issues"] == 1
        assert report.categories["Python Exceptions"] == 1
        assert report.categories["Killed / Out of Mem"] == 0
        assert report.categories["Socket/Slurm Errors"] == 0
        assert report.categories["Apptainer Errors"] == 0

    def test_empty_dir(self, tmp_path):
        report = analyze_dir(tmp_path)
        assert report.total == 0
        assert report.success == 0
        assert report.failures == 0
        assert all(v == 0 for v in report.categories.values())

    def test_killed_counted_from_err(self, tmp_path):
        (tmp_path / "task_x.out").write_text("ok\n", encoding="utf-8")
        (tmp_path / "task_x.err").write_text("Killed by OOM killer\n", encoding="utf-8")
        report = analyze_dir(tmp_path)
        assert report.categories["Killed / Out of Mem"] == 1

    def test_success_case_insensitive(self, tmp_path):
        (tmp_path / "task_y.out").write_text("Task SUCCESS\n", encoding="utf-8")
        report = analyze_dir(tmp_path)
        assert report.success == 1


# ─── format_report ───────────────────────────────────────────────────────────


class TestFormatReport:
    def test_no_logs(self, tmp_path):
        report = LogReport(total=0, success=0, failures=0, categories={})
        text = format_report(report, tmp_path)
        assert "No log files found" in text

    def test_full_report(self, tmp_path):
        report = LogReport(
            total=4,
            success=1,
            failures=3,
            categories={
                "Permission Issues": 1,
                "Killed / Out of Mem": 1,
                "Python Exceptions": 1,
                "Socket/Slurm Errors": 0,
                "Apptainer Errors": 0,
            },
        )
        text = format_report(report, tmp_path)
        assert "Total Tasks Scanned: 4" in text
        assert "SUCCESSFUL TASKS" in text
        assert "TOTAL FAILURES" in text
        assert "Permission Issues" in text
        assert "BREAKDOWN OF FAILURES" in text
        assert 'grep -L "success"' in text

    def test_no_hint_when_no_failures(self, tmp_path):
        report = LogReport(
            total=2,
            success=2,
            failures=0,
            categories={
                "Permission Issues": 0,
                "Killed / Out of Mem": 0,
                "Python Exceptions": 0,
                "Socket/Slurm Errors": 0,
                "Apptainer Errors": 0,
            },
        )
        text = format_report(report, tmp_path)
        assert "HINT" not in text


# ─── parser_args ─────────────────────────────────────────────────────────────


class TestParserArgs:
    def test_log_dir(self):
        with patch("sys.argv", ["slurm-logs", "/some/logs"]):
            args = parser_args()
        assert args.log_dir == "/some/logs"

    def test_missing_arg_exits(self):
        with patch("sys.argv", ["slurm-logs"]), pytest.raises(SystemExit):
            parser_args()


# ─── entrypoint ──────────────────────────────────────────────────────────────


class TestEntrypoint:
    def test_missing_dir_exits(self):
        with (
            patch("sys.argv", ["slurm-logs", "/does/not/exist"]),
            pytest.raises(SystemExit) as exc_info,
        ):
            entrypoint()
        assert exc_info.value.code == 1

    def test_happy_path_prints_report(self, tmp_path, capsys):
        (tmp_path / "task_a.out").write_text(
            "completed successfully\n", encoding="utf-8"
        )
        with patch("sys.argv", ["slurm-logs", str(tmp_path)]):
            entrypoint()
        out = capsys.readouterr().out
        assert "ANALYZING SUBMITIT LOGS" in out
        assert "SUCCESSFUL TASKS" in out
        # ensure stdout write was used (no print), captured by capsys
        assert "Total Tasks Scanned: 1" in out
