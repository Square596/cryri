import io
import logging
from typing import Optional, List, Dict
from contextlib import redirect_stdout


class JobNotFoundError(Exception):
    pass


class ClientLibMissingError(Exception):
    pass


def _require_client_lib():
    try:
        import client_lib  # noqa: F401
        return client_lib
    except ModuleNotFoundError:
        raise ClientLibMissingError(
            "client_lib is not installed. Install it to use cloud features."
        )


class JobManager:
    def __init__(self, region: str):
        self.region = region

    def get_jobs(self) -> List[str]:
        client_lib = _require_client_lib()
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            client_lib.jobs(region=self.region)
        output = buffer.getvalue()
        buffer.close()
        return output.splitlines()

    def get_jobs_structured(self) -> List[Dict]:
        raw_jobs = self.get_jobs()
        result = []
        for i, raw in enumerate(raw_jobs, start=1):
            parts = raw.split(" : ")
            if len(parts) >= 3:
                result.append({
                    "index": i,
                    "created_at": parts[0].strip(),
                    "job_name": parts[1].strip(),
                    "status": parts[2].strip(),
                })
            elif len(parts) >= 2:
                result.append({
                    "index": i,
                    "created_at": "",
                    "job_name": parts[0].strip(),
                    "status": parts[1].strip(),
                })
        return result

    def find_job_by_hash(self, partial_hash: str) -> Optional[str]:
        """Find a job by partial hash match. Returns full job_name if found."""
        for job in self.get_jobs_structured():
            if partial_hash in job["job_name"]:
                return job["job_name"]
        return None

    def get_instance_types(self):
        client_lib = _require_client_lib()
        return client_lib.get_instance_types(regions=self.region)

    def show_logs(self, job_hash: str) -> str:
        client_lib = _require_client_lib()
        full_hash = self.find_job_by_hash(job_hash)
        if not full_hash:
            raise JobNotFoundError(f"No job found with hash: {job_hash}")
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            client_lib.logs(full_hash, region=self.region)
        output = buffer.getvalue()
        buffer.close()
        return output

    def kill_job(self, job_hash: str) -> str:
        client_lib = _require_client_lib()
        full_hash = self.find_job_by_hash(job_hash)
        if not full_hash:
            raise JobNotFoundError(f"No job found with hash: {job_hash}")
        client_lib.kill(full_hash, region=self.region)
        logging.info("Job %s terminated successfully", full_hash)
        return full_hash

    @staticmethod
    def build_image(from_image: str, requirements_file: str,
                    install_type: str = "pip", conda_env: Optional[str] = None,
                    poetrylock_file: Optional[str] = None) -> Dict:
        client_lib = _require_client_lib()
        job = client_lib.ImageBuildJob(
            from_image=from_image,
            requirements_file=requirements_file,
            install_type=install_type,
            conda_env=conda_env,
            poetrylock_file=poetrylock_file,
        )
        result = job.submit()
        return {
            "result": result,
            "job_name": job.job_name,
            "new_image": job.new_image,
        }
