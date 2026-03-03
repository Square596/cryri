import io
import logging
from typing import Optional, List, Dict
from contextlib import redirect_stdout

from cryri import api


class JobNotFoundError(Exception):
    pass


class ClientLibMissingError(Exception):
    pass


def _require_client_lib():
    """Import client_lib for the legacy backend path."""
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

    # ------------------------------------------------------------------
    # Legacy helpers (only used when CRYRI_USE_CLIENT_LIB=1)
    # ------------------------------------------------------------------

    def _get_jobs_legacy(self) -> List[str]:
        client_lib = _require_client_lib()
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            client_lib.jobs(region=self.region)
        output = buffer.getvalue()
        buffer.close()
        return output.splitlines()

    @staticmethod
    def _parse_raw_jobs(raw_jobs: List[str]) -> List[Dict]:
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

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_jobs_structured(self) -> List[Dict]:
        if api.use_legacy_backend():
            return self._parse_raw_jobs(self._get_jobs_legacy())

        jobs = api.list_jobs(region=self.region)
        result = []
        for i, j in enumerate(jobs, start=1):
            result.append({
                "index": i,
                "created_at": j.get("created_at", ""),
                "job_name": j.get("job_name", ""),
                "status": j.get("status", ""),
            })
        return result

    def find_job_by_hash(self, partial_hash: str) -> Optional[str]:
        """Find a job by partial hash match. Returns full job_name if found."""
        for job in self.get_jobs_structured():
            if partial_hash in job["job_name"]:
                return job["job_name"]
        return None

    def get_instance_types(self):
        if api.use_legacy_backend():
            client_lib = _require_client_lib()
            return client_lib.get_instance_types(regions=self.region)
        return api.get_instance_types(regions=self.region)

    @staticmethod
    def get_images(cluster_type: str = "MT"):
        if api.use_legacy_backend():
            client_lib = _require_client_lib()
            return client_lib.get_docker_images(cluster_type)
        return api.get_images(cluster_type=cluster_type)

    def show_logs(self, job_hash: str) -> str:
        full_hash = self.find_job_by_hash(job_hash)
        if not full_hash:
            raise JobNotFoundError(f"No job found with hash: {job_hash}")

        if api.use_legacy_backend():
            client_lib = _require_client_lib()
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                client_lib.logs(full_hash, region=self.region)
            output = buffer.getvalue()
            buffer.close()
            return output

        return api.get_logs(full_hash, region=self.region)

    def kill_job(self, job_hash: str) -> str:
        full_hash = self.find_job_by_hash(job_hash)
        if not full_hash:
            raise JobNotFoundError(f"No job found with hash: {job_hash}")

        if api.use_legacy_backend():
            client_lib = _require_client_lib()
            client_lib.kill(full_hash, region=self.region)
        else:
            api.kill_job(full_hash, region=self.region)

        logging.info("Job %s terminated successfully", full_hash)
        return full_hash

    @staticmethod
    def build_image(from_image: str, requirements_file: str,
                    install_type: str = "pip", conda_env: Optional[str] = None,
                    poetrylock_file: Optional[str] = None) -> Dict:
        if api.use_legacy_backend():
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

        data = api.build_image(
            from_image=from_image,
            requirements_file=requirements_file,
            install_type=install_type,
            conda_env=conda_env,
            poetrylock_file=poetrylock_file,
        )
        return {
            "result": "submitted",
            "job_name": data.get("job_name", ""),
            "new_image": data.get("new_image", ""),
        }
