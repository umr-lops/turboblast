import argparse
import subprocess
from pathlib import Path
from typing import ClassVar
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from turboblast.blaster import (
    main,
    parse_memory_to_gb,
    parser_args,
    process_line,
    submit_chunk_with_retry,
    wait_for_batch_completion,
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
            ("2", 2.0),
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
            assert args.mem == "2G"
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
        executor.map_array.side_effect = [
            RuntimeError("sbatch failed"),
            [mock_job],
        ]
        with patch("turboblast.blaster.time.sleep"):
            jobs = submit_chunk_with_retry(executor, MagicMock(), ["a"], 1, 1)
        assert jobs == [mock_job]
        assert executor.map_array.call_count == 2

    def test_raises_after_max_retries(self):
        executor = MagicMock()
        executor.map_array.side_effect = RuntimeError("sbatch always fails")
        with (
            patch("turboblast.blaster.time.sleep"),
            pytest.raises(RuntimeError, match="submission failed after 2 attempts"),
        ):
            submit_chunk_with_retry(executor, MagicMock(), ["a"], 1, 1)
        assert executor.map_array.call_count == 2


# ─── wait_for_batch_completion ───────────────────────────────────────────────


class TestWaitForBatchCompletion:
    def test_all_jobs_complete_successfully(self):
        jobs = []
        for _i in range(3):
            job = MagicMock()
            # Simuler la propriété state avec des valeurs changeantes
            state_mock = PropertyMock(side_effect=["RUNNING", "RUNNING", "COMPLETED"])
            type(job).state = state_mock
            jobs.append(job)

        with (
            patch("turboblast.blaster.time.sleep") as _,
            patch("turboblast.blaster.tqdm"),
            patch("turboblast.blaster.BATCH_POLL_INTERVAL", 0.001),
        ):
            completed, failed = wait_for_batch_completion(jobs, 1, 1, 0)
            assert completed == 3
            assert failed == 0

    def test_some_jobs_fail(self):
        jobs = []
        job1 = MagicMock()
        type(job1).state = PropertyMock(side_effect=["RUNNING", "COMPLETED"])
        job2 = MagicMock()
        type(job2).state = PropertyMock(side_effect=["RUNNING", "FAILED"])
        jobs = [job1, job2]

        with (
            patch("turboblast.blaster.time.sleep"),
            patch("turboblast.blaster.tqdm"),
            patch("turboblast.blaster.BATCH_POLL_INTERVAL", 0.001),
        ):
            completed, failed = wait_for_batch_completion(jobs, 1, 1, 0)
            assert completed == 1
            assert failed == 1

    def test_stall_timeout_triggers_cancellation(self):
        jobs = []
        for i in range(2):
            job = MagicMock()
            type(job).state = PropertyMock(return_value="RUNNING")
            job.job_id = f"1234{i}"
            jobs.append(job)

        # Simuler l'écoulement du temps : après quelques appels, dépasser le timeout
        time_values = [0.0, 0.1, 0.2, 0.3, 0.4, 120.0] * 10  # assez de valeurs
        mock_monotonic = MagicMock(side_effect=time_values)

        with (
            patch("turboblast.blaster.time.sleep") as _,
            patch("turboblast.blaster.BATCH_POLL_INTERVAL", 0.001),
            patch("turboblast.blaster.tqdm"),
            patch("turboblast.blaster.subprocess.run") as mock_run,
            patch("time.monotonic", mock_monotonic),
        ):
            completed, failed = wait_for_batch_completion(
                jobs, 1, 1, stall_timeout_min=1
            )
            assert mock_run.call_count == 2
            mock_run.assert_any_call(["scancel", "12340"], check=False)
            mock_run.assert_any_call(["scancel", "12341"], check=False)
            assert completed == 0
            assert failed == 2

    def test_exception_during_state_fetch(self):
        """When job.state raises an exception, treat as UNKNOWN and retry."""
        job = MagicMock()
        # Une exception au premier appel, puis COMPLETED
        state_mock = PropertyMock(side_effect=[RuntimeError("error"), "COMPLETED"])
        type(job).state = state_mock
        jobs = [job]

        # Réduire l'intervalle de poll pour éviter les délais
        with (
            patch("turboblast.blaster.time.sleep"),
            patch("turboblast.blaster.BATCH_POLL_INTERVAL", 0.001),
            patch("turboblast.blaster.tqdm"),
        ):
            completed, failed = wait_for_batch_completion(jobs, 1, 1, 0)
            assert completed == 1
            assert failed == 0


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
            mem="2G",
            cpus_per_task=1,
            slurm_partition="cpu",
            slurm_array_parallelism=20,
            fail_fast=False,
            batch_stall_timeout_min=0,
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
            patch("turboblast.blaster.wait_for_batch_completion", return_value=(2, 0)),
        ):
            main(args)
            call_args = mock_executor.map_array.call_args
            assert call_args is not None
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
                "turboblast.blaster.wait_for_batch_completion", return_value=(1000, 0)
            ),
        ):
            main(args)
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
            patch("turboblast.blaster.wait_for_batch_completion", return_value=(2, 0)),
        ):
            main(args)
            submitted = mock_executor.map_array.call_args[0][1]
            assert len(submitted) == 2

    def test_fail_fast_stops_after_first_failure(self, tmp_path):
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
            patch(
                "turboblast.blaster.wait_for_batch_completion", return_value=(900, 100)
            ),
        ):
            main(args)
            assert mock_executor.map_array.call_count == 1

    def test_fail_fast_false_continues_after_failures(self, tmp_path):
        """With fail_fast=False, continue to next batches even if failures occur."""
        args = self._make_args(tmp_path, lines=[f"--input {i}.nc" for i in range(2500)])
        args.fail_fast = False
        mock_executor = MagicMock()
        mock_job = MagicMock()
        mock_job.job_id = "99"
        mock_executor.map_array.return_value = [mock_job]

        # Trois chunks → trois résultats
        side_effects = [
            (900, 100),
            (1000, 0),
            (1000, 0),
        ]  # premier échoue, les autres réussissent
        with (
            patch(
                "turboblast.blaster.submitit.AutoExecutor", return_value=mock_executor
            ),
            patch(
                "turboblast.blaster.wait_for_batch_completion",
                side_effect=side_effects,
            ),
        ):
            main(args)
            # fail_fast=False → tous les chunks sont soumis
            assert mock_executor.map_array.call_count == 3

    def test_output_dir_creation_with_timestamp(self, tmp_path):
        args = self._make_args(tmp_path)
        mock_executor = MagicMock()
        mock_executor.map_array.return_value = [MagicMock(), MagicMock()]
        with (
            patch(
                "turboblast.blaster.submitit.AutoExecutor", return_value=mock_executor
            ),
            patch("turboblast.blaster.wait_for_batch_completion", return_value=(2, 0)),
        ):
            main(args)
            output_dir = Path(args.output_dir)
            subdirs = list(output_dir.glob("*"))
            assert len(subdirs) == 1
            assert subdirs[0].name.startswith("20")
