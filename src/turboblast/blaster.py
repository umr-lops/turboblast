import argparse
import datetime
import functools
import os
import subprocess
import sys

import submitit


def process_line(slurmexe, options_one_line):
    """
    Function executed by each task in the job array.
    """
    print(f"[Task options: {options_one_line}] Starting computation", flush=True)

    # Construct the command
    # We use shlex.split if options are complex, but for simple strings list structure is better
    # However, since shell=False is safer, we construct the command list
    cmd = ["bash", slurmexe] + options_one_line.split()

    print(f"Executing: {' '.join(cmd)}", flush=True)

    # We use subprocess.call or run WITHOUT capture_output.
    # This allows logs to stream directly to the submitit log files in real-time.
    # 1. Prepare the environment correctly
    # Copy the current environment (including PATH, LD_LIBRARY_PATH, etc.)
    full_env = os.environ.copy()
    # Add your specific variable
    full_env["PYTHONUNBUFFERED"] = "1"
    try:
        # check=True will raise a CalledProcessError if return code != 0
        # subprocess.run(cmd, shell=False, check=True, env=env)
        subprocess.run(
            cmd,
            shell=False,
            check=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
            env=full_env,
        )
        sys.stdout.flush()  # Force a flush after the process finishes
        print(
            f"Task with options {options_one_line} completed successfully.", flush=True
        )
    except subprocess.CalledProcessError as e:
        print(f"Error in task with options: {options_one_line}", file=sys.stderr)
        print(f"Command returned exit status {e.returncode}", file=sys.stderr)
        raise e  # Re-raise exception so submitit marks the job as FAILED


def parser_args():
    parser = argparse.ArgumentParser(description="Submitit array job example")
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
        "--mem-gb", type=int, default=2, help="Memory in GB for each task"
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
        help="Path to a file containing input lines",
    )
    parser.add_argument(
        "--bash-slurm-exec", type=str, required=True, help="Path to the bash script"
    )
    parser.add_argument(
        "--output-dir",
        help="Directory to store submitit logs (suffixed with out/YYYYMMDDTHHMMSS/)",
        default="submitit_logs_array",
    )
    parser.add_argument(
        "--slurm-array-parallelism", type=int, default=20, help="Max concurrent tasks"
    )
    return parser.parse_args()


def main(args):
    # Create log directory
    output_dir_with_date = os.path.join(
        args.output_dir, datetime.datetime.today().strftime("%Y%m%dT%H%M%S")
    )
    os.makedirs(output_dir_with_date, exist_ok=True)

    executor = submitit.AutoExecutor(folder=output_dir_with_date, cluster="slurm")
    print("args.bash_slurm_exec", args.bash_slurm_exec)
    # Slurm Configuration
    executor.update_parameters(
        timeout_min=args.timeout_min,
        mem_gb=args.mem_gb,
        cpus_per_task=args.cpus_per_task,
        slurm_partition=args.slurm_partition,
        slurm_array_parallelism=args.slurm_array_parallelism,
        # Naming the job makes it easier to find in squeue
        slurm_job_name=os.path.basename(args.bash_slurm_exec).replace(".sh", ""),
        slurm_additional_parameters={"export": "ALL,PYTHONUNBUFFERED=1"},
    )

    # Read inputs
    with open(args.listing_input) as f:
        # Filter out empty lines just in case
        array_inputs = [line.strip() for line in f if line.strip()]

    if not array_inputs:
        print("Error: Input listing file is empty.")
        return

    print(f"Submitting {len(array_inputs)} tasks...")

    # if an image must be pulled from registry, honestly it is much easier with a .sif on fs.
    # reason why the block below is commented out.
    # Pull the image on the submission node first
    # img_url = "oras://registry.hpc.ifremer.fr/lops-siam-sentinel1-workbench/unifiedwvalticolocs:2026.2.17"
    # img_sif = "unifiedwvalticolocs_2026.2.17.sif"

    # if not os.path.exists(img_sif):
    #     print(f"Pulling image {img_url}...")
    #     subprocess.run(["apptainer", "pull", img_sif, img_url], check=True)

    # KEY CHANGE: Use partial to bind the bash script path to the function
    # The map_array will only vary the second argument (options_one_line)

    # Define the chunk size (set this slightly below your MaxArraySize, e.g., 500 or 1000)
    chunk_size = 1000  # Adjust based on your cluster's MaxArraySize and the total number of tasks, here I am not sure this is the limit but it failed with 1379 lines
    all_jobs = []

    print(f"Total tasks to submit: {len(array_inputs)}")

    # Loop through the inputs in chunks
    chuncked_inputs = range(0, len(array_inputs), chunk_size)
    print(f"Submitting tasks in chunks of {chunk_size}...")
    print(f"Total chunks to submit: {len(list(chuncked_inputs))}")
    for i in chuncked_inputs:
        chunk = array_inputs[i : i + chunk_size]
        print(f"Submitting chunk starting at index {i} (size: {len(chunk)})...")

        # This will create a NEW Slurm Job ID for every 1000 tasks
        process_func = functools.partial(process_line, args.bash_slurm_exec)
        jobs = executor.map_array(process_func, chunk)

        print(f"Submitted. Job ID for this chunk: {jobs[0].job_id}")
        all_jobs.extend(jobs)

    print(
        f"Successfully submitted all {len(all_jobs)} tasks across multiple Job Arrays."
    )

    # process_func = functools.partial(process_line, args.bash_slurm_exec)

    # # Submit the array
    # jobs = executor.map_array(process_func, array_inputs)

    # Get the ID of the array job
    # jobs[0] gives access to the whole array group usually, or iterate to see IDs
    print(f"Job Array submitted. Main Job ID: {jobs[0].job_id}")
    print(f"Logs are being written to: {output_dir_with_date}")
    print(
        f"You can monitor specific logs using: tail -f {output_dir_with_date}/<JOB_ID>_<TASK_ID>_0.out"
    )


def entrypoint():
    args = parser_args()
    main(args)


if __name__ == "__main__":
    entrypoint()
