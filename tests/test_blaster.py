import argparse
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from turboblast.blaster import main, parser_args, process_line


# ─── process_line ────────────────────────────────────────────────────────────

class TestProcessLine:
    def test_success(self):
        with patch("turboblast.blaster.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            process_line("/path/to/script.sh", "--input a.nc --output /tmp")
            mock_run.assert_called_once()
            cmd = mock_run.call_args[0][0]
            assert cmd == ["bash", "/path/to/script.sh", "--input", "a.nc", "--output", "/tmp"]

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


# ─── parser_args ─────────────────────────────────────────────────────────────

class TestParserArgs:
    BASE_ARGS = [
        "--listing-input", "/data/inputs.txt",
        "--bash-slurm-exec", "/scripts/run.sh",
    ]

    def test_required_args(self):
        with patch("sys.argv", ["blaster"] + self.BASE_ARGS):
            args = parser_args()
            assert args.listing_input == "/data/inputs.txt"
            assert args.bash_slurm_exec == "/scripts/run.sh"

    def test_defaults(self):
        with patch("sys.argv", ["blaster"] + self.BASE_ARGS):
            args = parser_args()
            assert args.num_tasks == 20
            assert args.timeout_min == 20
            assert args.mem_gb == 2
            assert args.cpus_per_task == 1
            assert args.slurm_partition == "cpu"
            assert args.slurm_array_parallelism == 20
            assert args.output_dir == "submitit_logs_array"

    def test_custom_values(self):
        with patch("sys.argv", ["blaster"] + self.BASE_ARGS + [
            "--timeout-min", "60",
            "--mem-gb", "8",
            "--slurm-partition", "gpu",
        ]):
            args = parser_args()
            assert args.timeout_min == 60
            assert args.mem_gb == 8
            assert args.slurm_partition == "gpu"

    def test_missing_required_args_exits(self):
        with patch("sys.argv", ["blaster"]):
            with pytest.raises(SystemExit):
                parser_args()


# ─── main ─────────────────────────────────────────────────────────────────────

class TestMain:
    def _make_args(self, tmp_path, lines=None):
        input_file = tmp_path / "inputs.txt"
        content = "\n".join(lines) if lines is not None else "--input a.nc\n--input b.nc"
        input_file.write_text(content)
        return argparse.Namespace(
            listing_input=str(input_file),
            bash_slurm_exec="/scripts/run.sh",
            output_dir=str(tmp_path / "logs"),
            timeout_min=20,
            mem_gb=2,
            cpus_per_task=1,
            slurm_partition="cpu",
            slurm_array_parallelism=20,
        )

    def test_submits_jobs(self, tmp_path):
        args = self._make_args(tmp_path)
        mock_executor = MagicMock()
        mock_job = MagicMock()
        mock_job.job_id = "12345"
        mock_executor.map_array.return_value = [mock_job, mock_job]

        with patch("turboblast.blaster.submitit.AutoExecutor", return_value=mock_executor):
            main(args)
            mock_executor.map_array.assert_called_once()

    def test_empty_input_file_aborts(self, tmp_path):
        args = self._make_args(tmp_path, lines=[])
        mock_executor = MagicMock()

        with patch("turboblast.blaster.submitit.AutoExecutor", return_value=mock_executor):
            main(args)
            mock_executor.map_array.assert_not_called()

    def test_chunks_large_input(self, tmp_path):
        """Input > 1000 lines should produce multiple map_array calls."""
        args = self._make_args(tmp_path, lines=[f"--input {i}.nc" for i in range(2500)])
        mock_executor = MagicMock()
        mock_job = MagicMock()
        mock_job.job_id = "99"
        mock_executor.map_array.return_value = [mock_job]

        with patch("turboblast.blaster.submitit.AutoExecutor", return_value=mock_executor):
            main(args)
            assert mock_executor.map_array.call_count == 3  # 1000 + 1000 + 500

    def test_blank_lines_ignored(self, tmp_path):
        args = self._make_args(tmp_path, lines=["--input a.nc", "", "  ", "--input b.nc"])
        mock_executor = MagicMock()
        mock_job = MagicMock()
        mock_job.job_id = "1"
        mock_executor.map_array.return_value = [mock_job, mock_job]

        with patch("turboblast.blaster.submitit.AutoExecutor", return_value=mock_executor):
            main(args)
            submitted = mock_executor.map_array.call_args[0][1]
            assert len(submitted) == 2