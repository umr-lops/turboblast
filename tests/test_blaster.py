import argparse
import subprocess
from pathlib import Path
from typing import ClassVar
from unittest.mock import MagicMock, patch

import pytest

from turboblast.blaster import (
    main,
    parse_memory_to_gb,
    parser_args,
    process_line,
    submit_chunk_with_retry,
)

# ─── process_line ────────────────────────────────────────────────────────────


class TestProcessLine:
    def test_success(self):
        with patch("turboblast.blaster.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            process_line("/path/to/script.sh", "--input a.nc --output /tmp")
            mock_run.assert_called_once()
            cmd = mock_run.call_args[0][0]
            assert cmd == [
                "bash",
                "/path/to/script.sh",
                "--input",
                "a.nc",
                "--output",
                "/tmp",
            ]

    def test_passes_env_with_pythonunbuffered(self):
        with patch("turboblast.blaster.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            process_line("/script.sh", "--flag value")
            env = mock_run.call_args[1]["env"]
            assert env["PYTHONUNBUFFERED"] == "1"

    def test_raises_on_nonzero_exit(self):
        with patch("turboblast.blaster.subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(1, "bash")
            with pytest.raises(subprocess.CalledProcessError):
                process_line("/script.sh", "--input bad.nc")

    def test_shell_false(self):
        with patch("turboblast.blaster.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            process_line("/script.sh", "--flag")
            assert mock_run.call_args[1]["shell"] is False

    def test_options_with_quoted_strings(self):
        """shlex.split should correctly handle quoted arguments."""
        with patch("turboblast.blaster.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            process_line("/script.sh", '--input "file with spaces.nc"')
            cmd = mock_run.call_args[0][0]
            assert "file with spaces.nc" in cmd


# ─── parse_memory_to_gb ──────────────────────────────────────────────────────


class TestParseMemoryToGb:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("2G", 2.0),
            ("2g", 2.0),
            ("2GB", 2.0),
            ("2gb", 2.0),
            ("1024M", 1.0),
            ("512M", 0.5),
            ("512MB", 0.5),
            ("100M", 100 / 1024),
            ("2", 2.0),  # bare int → gigabytes (backward compat)
        ],
    )
    def test_valid_values(self, value, expected):
        assert parse_memory_to_gb(value) == pytest.approx(expected)

    @pytest.mark.parametrize("value", ["abc", "G", "MB", "2X", ""])
    def test_invalid_values_raise(self, value):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_memory_to_gb(value)

    def test_zero_raises(self):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_memory_to_gb("0G")

    def test_negative_raises(self):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_memory_to_gb("-1G")


# ─── parser_args ─────────────────────────────────────────────────────────────


class TestParserArgs:
    BASE_ARGS: ClassVar[list[str]] = [
        "--listing-input",
        "/data/inputs.txt",
        "--bash-slurm-exec",
        "/scripts/run.sh",
    ]

    def test_required_args(self):
        with patch("sys.argv", ["blaster", *self.BASE_ARGS]):
            args = parser_args()
            assert args.listing_input == "/data/inputs.txt"
            assert args.bash_slurm_exec == "/scripts/run.sh"

    def test_defaults(self):
        with patch("sys.argv", ["blaster", *self.BASE_ARGS]):
            args = parser_args()
            assert args.num_tasks == 20
            assert args.timeout_min == 20
            assert args.mem == "2G"  # new: string, not int
            assert args.cpus_per_task == 1
            assert args.slurm_partition == "cpu"
            assert args.slurm_array_parallelism == 20
            assert args.output_dir == "submitit_logs_array"
            assert args.fail_fast is False
            assert args.batch_stall_timeout_min == 0

    def test_custom_mem_gb(self):
        with patch("sys.argv", ["blaster", *self.BASE_ARGS, "--mem", "8G"]):
            args = parser_args()
            assert args.mem == "8G"

    def test_custom_mem_mb(self):
        with patch("sys.argv", ["blaster", *self.BASE_ARGS, "--mem", "512M"]):
            args = parser_args()
            assert args.mem == "512M"

    def test_custom_partition_and_timeout(self):
        with patch(
            "sys.argv",
            [
                "blaster",
                *self.BASE_ARGS,
                "--timeout-min",
                "60",
                "--slurm-partition",
                "gpu",
            ],
        ):
            args = parser_args()
            assert args.timeout_min == 60
            assert args.slurm_partition == "gpu"

    def test_fail_fast_flag(self):
        with patch("sys.argv", ["blaster", *self.BASE_ARGS, "--fail-fast"]):
            args = parser_args()
            assert args.fail_fast is True

    def test_batch_stall_timeout(self):
        with patch(
            "sys.argv",
            ["blaster", *self.BASE_ARGS, "--batch-stall-timeout-min", "15"],
        ):
            args = parser_args()
            assert args.batch_stall_timeout_min == 15

    def test_missing_required_args_exits(self):
        with patch("sys.argv", ["blaster"]), pytest.raises(SystemExit):
            parser_args()


# ─── submit_chunk_with_retry ─────────────────────────────────────────────────


class TestSubmitChunkWithRetry:
    def _make_executor(self, job_id: str = "42") -> MagicMock:
        mock_job = MagicMock()
        mock_job.job_id = job_id
        executor = MagicMock()
        executor.map_array.return_value = [mock_job]
        return executor

    def test_success_on_first_attempt(self):
        executor = self._make_executor()
        func = MagicMock()
        jobs = submit_chunk_with_retry(executor, func, ["a", "b"], 1, 3)
        assert len(jobs) == 1
        executor.map_array.assert_called_once()

    def test_retries_on_failure_then_succeeds(self):
        executor = MagicMock()
        mock_job = MagicMock()
        mock_job.job_id = "99"
        # Fail on first attempt, succeed on second attempt (max retries = 2)
        executor.map_array.side_effect = [
            RuntimeError("sbatch failed"),
            [mock_job],  # succeed on second attempt
        ]
        with patch("turboblast.blaster.time.sleep"):
            jobs = submit_chunk_with_retry(executor, MagicMock(), ["a"], 1, 1)
        assert jobs == [mock_job]
        assert executor.map_array.call_count == 2  # 1 failure + 1 success = 2 calls

    def test_raises_after_max_retries(self):
        executor = MagicMock()
        # Fail all attempts (max retries = 2, so 2 failures)
        executor.map_array.side_effect = RuntimeError("sbatch always fails")
        with (
            patch("turboblast.blaster.time.sleep"),
            pytest.raises(RuntimeError, match="submission failed after 2 attempts"),
        ):
            submit_chunk_with_retry(executor, MagicMock(), ["a"], 1, 1)
        # SUBMIT_MAX_RETRIES = 2, so 2 attempts total
        assert executor.map_array.call_count == 2


# ─── main ─────────────────────────────────────────────────────────────────────


class TestMain:
    def _make_args(
        self, tmp_path: Path, lines: list[str] | None = None
    ) -> argparse.Namespace:
        input_file = tmp_path / "inputs.txt"
        content = (
            "\n".join(lines) if lines is not None else "--input a.nc\n--input b.nc"
        )
        input_file.write_text(content, encoding="utf-8")
        return argparse.Namespace(
            listing_input=str(input_file),
            bash_slurm_exec="/scripts/run.sh",
            output_dir=str(tmp_path / "logs"),
            timeout_min=20,
            mem="2G",  # new: string
            cpus_per_task=1,
            slurm_partition="cpu",
            slurm_array_parallelism=20,
            fail_fast=False,  # new
            batch_stall_timeout_min=0,  # new
        )

    def test_submits_jobs(self, tmp_path):
        args = self._make_args(tmp_path)
        mock_executor = MagicMock()
        mock_job = MagicMock()
        mock_job.job_id = "12345"
        mock_executor.map_array.return_value = [mock_job, mock_job]

        with (
            patch(
                "turboblast.blaster.submitit.AutoExecutor", return_value=mock_executor
            ),
            # Mock wait_for_batch_completion to avoid blocking on real Slurm.
            patch(
                "turboblast.blaster.wait_for_batch_completion",
                return_value=(2, 0),
            ),
        ):
            main(args)
            # Vérifier que map_array a été appelé avec les bons arguments
            call_args = mock_executor.map_array.call_args
            assert call_args is not None
            # Le deuxième argument devrait être la liste des inputs
            assert len(call_args[0][1]) == 2

    def test_empty_input_file_aborts(self, tmp_path):
        args = self._make_args(tmp_path, lines=[])
        mock_executor = MagicMock()

        with patch(
            "turboblast.blaster.submitit.AutoExecutor", return_value=mock_executor
        ):
            main(args)
            mock_executor.map_array.assert_not_called()

    def test_chunks_large_input(self, tmp_path):
        """Input > 1000 lines should produce multiple map_array calls."""
        args = self._make_args(tmp_path, lines=[f"--input {i}.nc" for i in range(2500)])
        mock_executor = MagicMock()
        mock_job = MagicMock()
        mock_job.job_id = "99"
        mock_executor.map_array.return_value = [mock_job]

        with (
            patch(
                "turboblast.blaster.submitit.AutoExecutor", return_value=mock_executor
            ),
            patch(
                "turboblast.blaster.wait_for_batch_completion",
                return_value=(1000, 0),
            ),
        ):
            main(args)
            # Vérifier que map_array a été appelé 3 fois (1000 + 1000 + 500)
            assert mock_executor.map_array.call_count == 3

    def test_blank_lines_ignored(self, tmp_path):
        args = self._make_args(
            tmp_path, lines=["--input a.nc", "", "  ", "--input b.nc"]
        )
        mock_executor = MagicMock()
        mock_job = MagicMock()
        mock_job.job_id = "1"
        mock_executor.map_array.return_value = [mock_job, mock_job]

        with (
            patch(
                "turboblast.blaster.submitit.AutoExecutor", return_value=mock_executor
            ),
            patch(
                "turboblast.blaster.wait_for_batch_completion",
                return_value=(2, 0),
            ),
        ):
            main(args)
            # Vérifier que seulement 2 inputs ont été soumis (les lignes vides ignorées)
            submitted = mock_executor.map_array.call_args[0][1]
            assert len(submitted) == 2

    def test_fail_fast_stops_after_first_failure(self, tmp_path):
        """With fail_fast=True and a failing batch, only 1 chunk should be submitted."""
        args = self._make_args(tmp_path, lines=[f"--input {i}.nc" for i in range(2500)])
        args.fail_fast = True
        mock_executor = MagicMock()
        mock_job = MagicMock()
        mock_job.job_id = "99"
        mock_executor.map_array.return_value = [mock_job]

        with (
            patch(
                "turboblast.blaster.submitit.AutoExecutor", return_value=mock_executor
            ),
            # First batch has failures (900 completed, 100 failed)
            patch(
                "turboblast.blaster.wait_for_batch_completion",
                return_value=(900, 100),
            ),
        ):
            main(args)
            # fail_fast → stopped after first batch, not 3
            # Note: Le premier batch contient 1000 tâches (CHUNK_SIZE=1000)
            assert mock_executor.map_array.call_count == 1
