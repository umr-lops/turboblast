from unittest.mock import MagicMock, patch

import pytest

from turboblast.monitor import (
    COLUMNS,
    _format_start,
    _run,
    build_report,
    entrypoint,
    fetch_declared_total,
    fetch_job_ids,
    format_row,
    live_monitor,
    parse_sacct,
    parser_args,
)

# A sample sacct -X dump: parent line + 4 task lines.
SACCT_SAMPLE = """\
528266 COMPLETED myjobnamehere 4096M 2026-10-07T10:00:00
528266_0 COMPLETED myjobnamehere 4096M 2026-10-07T10:00:00
528266_1 COMPLETED myjobnamehere 4096M 2026-10-07T10:00:00
528266_2 RUNNING myjobnamehere 4096M 2026-10-07T10:00:00
528266_3 FAILED myjobnamehere 4096M 2026-10-07T10:00:00
"""


# ─── _format_start ───────────────────────────────────────────────────────────


class TestFormatStart:
    def test_normal(self):
        assert _format_start("2026-10-07T10:00:00") == "10-07 10:00"

    @pytest.mark.parametrize("raw", ["Unknown", "None"])
    def test_unknown(self, raw):
        assert _format_start(raw) == "Pending"


# ─── parse_sacct ─────────────────────────────────────────────────────────────


class TestParseSacct:
    def test_array_excludes_parent_and_uses_declared_total(self):
        stats = parse_sacct("528266", SACCT_SAMPLE, declared_total=1000)
        assert stats.name == "myjobnamehere"
        assert stats.mem == "4096M"
        assert stats.started == "10-07 10:00"
        assert stats.running == 1
        assert stats.success == 2
        assert stats.failed == 1
        assert stats.total == 1000
        assert stats.done_percent == (2 + 1) * 100 // 1000

    def test_non_array_falls_back_to_line_count(self):
        stats = parse_sacct("528266", SACCT_SAMPLE, declared_total=0)
        # 5 lines counted, parent included as a task
        assert stats.total == 5
        assert stats.success == 3
        assert stats.running == 1
        assert stats.failed == 1
        assert stats.done_percent == 80

    def test_empty(self):
        stats = parse_sacct("123", "", declared_total=0)
        assert stats.total == 0
        assert stats.done_percent == 0


# ─── fetch_declared_total ────────────────────────────────────────────────────


class TestFetchDeclaredTotal:
    def test_found(self):
        out = "JobId=528266 JobId=528266\nArrayTaskCount=1000 ArrayTaskIds=0-999\n"
        with patch("turboblast.monitor._run", return_value=out):
            assert fetch_declared_total("528266") == 1000

    def test_not_array(self):
        with patch("turboblast.monitor._run", return_value="JobId=99\n"):
            assert fetch_declared_total("99") == 0

    def test_missing_tool(self):
        with patch("turboblast.monitor._run", return_value=""):
            assert fetch_declared_total("99") == 0


# ─── fetch_job_ids ───────────────────────────────────────────────────────────


class TestFetchJobIds:
    def test_explicit_job_id(self):
        assert fetch_job_ids("42") == ["42"]

    def test_from_squeue_sorted_unique(self):
        with patch("turboblast.monitor._run", return_value="3\n1\n3\n2\n"):
            assert fetch_job_ids(None) == ["1", "2", "3"]

    def test_empty(self):
        with patch("turboblast.monitor._run", return_value=""):
            assert fetch_job_ids(None) == []


# ─── format_row / alignment ──────────────────────────────────────────────────


class TestFormatRow:
    def test_alignment_matches_columns(self):
        row = format_row(
            [
                "528266",
                "myjob",
                "4096M",
                "10-07 10:00",
                "1",
                "2",
                "0",
                "10",
                "1",
                "1000",
                "1%",
            ]
        )
        # Every cell is padded to its declared width.
        for (_, width), value in zip(COLUMNS, row.split(" | "), strict=True):
            assert len(value) == width

    def test_column_count_matches(self):
        row = format_row(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"])
        assert len(row.split(" | ")) == len(COLUMNS)


# ─── build_report ────────────────────────────────────────────────────────────


class TestBuildReport:
    def test_no_active_jobs(self):
        with (
            patch("turboblast.monitor.fetch_job_ids", return_value=[]),
            patch("turboblast.monitor._run", return_value=""),
        ):
            report = build_report(None)
        assert "No active jobs." in report
        assert "SLURM ACTIVE ARRAYS SUMMARY" in report

    def test_not_found(self):
        with (
            patch("turboblast.monitor.fetch_job_ids", return_value=["999"]),
            patch("turboblast.monitor._run", return_value=""),
        ):
            report = build_report("999")
        assert "SLURM REPORT FOR JOB: 999" in report
        assert "NOT_FOUND" in report

    def test_with_data(self):
        with (
            patch("turboblast.monitor.fetch_job_ids", return_value=["528266"]),
            patch("turboblast.monitor._run", return_value=SACCT_SAMPLE),
            patch("turboblast.monitor.fetch_declared_total", return_value=1000),
        ):
            report = build_report("528266")
        assert "528266" in report
        assert "1000" in report
        # header present
        assert "DONE%" in report


# ─── parser_args ─────────────────────────────────────────────────────────────


class TestParserArgs:
    def test_defaults(self):
        with patch("sys.argv", ["slurm-monitor"]):
            args = parser_args()
        assert args.job_id is None
        assert args.interval == 2.0

    def test_with_job_id_and_interval(self):
        with patch("sys.argv", ["slurm-monitor", "42", "--interval", "5"]):
            args = parser_args()
        assert args.job_id == "42"
        assert args.interval == 5.0


# ─── _run ────────────────────────────────────────────────────────────────────


class TestRun:
    def test_returns_stdout(self):
        proc = MagicMock()
        proc.stdout = "hello\n"
        with patch("turboblast.monitor.subprocess.run", return_value=proc):
            assert _run(["echo", "hello"]) == "hello\n"

    def test_oserror_returns_empty(self):
        with patch("turboblast.monitor.subprocess.run", side_effect=OSError("nope")):
            assert _run(["sacct"]) == ""


# ─── entrypoint ──────────────────────────────────────────────────────────────


class TestEntrypoint:
    def test_delegates_to_live_monitor(self):
        with (
            patch("turboblast.monitor.parser_args") as mock_parser,
            patch("turboblast.monitor.live_monitor") as mock_live,
        ):
            mock_parser.return_value = MagicMock(job_id="7", interval=1.5)
            entrypoint()
        mock_live.assert_called_once_with("7", 1.5)


# ─── live_monitor ────────────────────────────────────────────────────────────


class TestLiveMonitor:
    def test_stops_on_ctrl_c_and_restores_cursor(self, capsys):
        calls = {"n": 0}

        def fake_sleep(_):
            calls["n"] += 1
            raise KeyboardInterrupt

        with (
            patch("turboblast.monitor.time.sleep", side_effect=fake_sleep),
            patch("turboblast.monitor.build_report", return_value="REPORT"),
        ):
            live_monitor(None, interval=0.0)

        out = capsys.readouterr().out
        assert "\033[?25l" in out  # cursor hidden on start
        assert "\033[?25h" in out  # cursor restored on exit
        assert "REPORT" in out
        assert calls["n"] == 1
