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
            if len(parts) >= 2:
                result.append({
                    "index": i,
                    "description": parts[0].strip(),
                    "hash": parts[1].strip(),
                })
        return result

    def find_job_by_hash(self, partial_hash: str) -> Optional[str]:
        """Find a job by partial hash match. Returns full hash if found."""
        for job_name in self.get_jobs():
            job_hash = self.raw_job_to_id(job_name)
            if partial_hash in job_hash:
                return job_hash
        return None

    @staticmethod
    def raw_job_to_id(job_string: str) -> str:
        return job_string.split(" : ")[1].strip()

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
