from pathlib import Path
from tqdm import tqdm
import time
import re
import subprocess


def submit_job(job_file: Path, queue: str = "day", jobname: str = "indexing", time: str = "23:00:00") -> int:

    submit_cmd = ["sbatch", "-p", queue, "--cpus-per-task=36", "--exclusive", f"--time={time}", "-J", jobname, job_file]
    job_output = subprocess.check_output(submit_cmd)

    pattern = r"Submitted batch job (\d+)"
    job_id = re.search(pattern, job_output.decode().strip()).group(1)

    return int(job_id)


def wait_for_jobs(job_ids: list[int], sleep_time: int = 30) -> None:
    with tqdm(total=len(job_ids), desc="Jobs Completed", unit="job") as pbar:
        while job_ids:
            completed_jobs = set()
            for job_id in job_ids:
                status_cmd = ["squeue", "-h", "-j", str(job_id)]
                status = subprocess.check_output(status_cmd)
                if not status:
                    completed_jobs.add(job_id)
                    pbar.update(1)
            job_ids.difference_update(completed_jobs)
            time.sleep(sleep_time)
