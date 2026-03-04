"""Built-in API client for cryri — replaces client_lib dependency."""

import os
from typing import Optional, List, Dict

import requests
from rich.table import Table


class ApiError(Exception):
    """Raised on non-200 API responses."""

    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"API error {status_code}: {detail}")


def use_legacy_backend() -> bool:
    """Check if legacy client_lib backend is requested via env var."""
    return os.environ.get("CRYRI_USE_CLIENT_LIB", "").strip() == "1"


def _get_namespace() -> str:
    """Resolve namespace from NAMESPACE env var, falling back to NB_PREFIX."""
    ns = os.environ.get("NAMESPACE")
    if ns:
        return ns
    nb_prefix = os.environ["NB_PREFIX"]
    ns = nb_prefix.split("/")[2]
    if ns == "notebook":
        return nb_prefix.split("/")[3]
    return ns


def _base_url() -> str:
    return f"http://{os.environ['GWAPI_ADDR']}"


def _auth_headers() -> Dict[str, str]:
    return {
        "X-Api-Key": os.environ["GWAPI_KEY"],
        "X-Namespace": _get_namespace(),
    }


# ---------------------------------------------------------------------------
# API functions
# ---------------------------------------------------------------------------


def submit_job(
    script: str,
    base_image: str,
    instance_type: str,
    region: str,
    *,
    job_type: str = "binary",
    n_workers: int = 1,
    processes_per_worker: int = 1,
    env_variables: Optional[Dict[str, str]] = None,
    job_desc: str = "",
    flags: Optional[Dict] = None,
) -> dict:
    """Submit a job. Returns dict with ``job_name``."""
    payload = {
        "script": script,
        "base_image": base_image,
        "instance_type": instance_type,
        "region": region,
        "type": job_type,
        "n_workers": n_workers,
        "processes_per_worker": processes_per_worker,
        "env_variables": env_variables or {},
        "job_desc": job_desc,
        "flags": flags or {},
    }
    resp = requests.post(
        f"{_base_url()}/run_job",
        json=payload,
        headers=_auth_headers(),
        timeout=30,
    )
    if resp.status_code != 200:
        raise ApiError(resp.status_code, resp.text)
    return resp.json()


def list_jobs(region: Optional[str] = None) -> List[dict]:
    """List jobs. Returns list of dicts with ``created_at``, ``job_name``, ``status``."""
    payload: Dict = {}
    if region:
        payload["region"] = region
    resp = requests.post(
        f"{_base_url()}/job_list",
        json=payload,
        headers=_auth_headers(),
        timeout=30,
    )
    if resp.status_code != 200:
        raise ApiError(resp.status_code, resp.text)
    jobs = resp.json()
    jobs.sort(key=lambda j: j.get("created_at", ""))
    return jobs


def get_logs(
    job_name: str,
    region: Optional[str] = None,
    tail: int = 0,
    verbose: bool = True,
) -> str:
    """Read logs for a job. Returns collected log text."""
    payload: Dict = {
        "job_name": job_name,
        "tail": tail,
        "verbose": verbose,
        "region": region,
    }
    resp = requests.post(
        f"{_base_url()}/read_logs",
        json=payload,
        headers=_auth_headers(),
        stream=True,
        timeout=60,
    )
    if resp.status_code != 200:
        raise ApiError(resp.status_code, resp.text)
    return resp.text


def kill_job(job_name: str, region: str) -> str:
    """Kill a running job. Returns status message."""
    payload = {"job_name": job_name, "region": region}
    resp = requests.post(
        f"{_base_url()}/delete_job/v2",
        json=payload,
        headers=_auth_headers(),
        timeout=30,
    )
    if resp.status_code != 200:
        raise ApiError(resp.status_code, resp.text)
    return resp.text


def get_instance_types(regions: str) -> Table:
    """Fetch instance types for the given region. Returns a ``rich.Table``."""
    headers = _auth_headers()
    base = _base_url()

    resp = requests.get(f"{base}/v1/clusters", headers=headers, timeout=30)
    if resp.status_code != 200:
        raise ApiError(resp.status_code, resp.text)

    table = Table(title="Instance Types")
    table.add_column("Cluster", style="cyan")
    table.add_column("Instance Type", style="green")
    table.add_column("GPUs", justify="right")
    table.add_column("CPUs", justify="right")
    table.add_column("Memory", justify="right")

    for cluster in resp.json():
        cluster_key = cluster.get("cluster_key", "")
        resp2 = requests.get(
            f"{base}/v1/clusters/{cluster_key}/instance_types",
            headers=headers,
            timeout=30,
        )
        if resp2.status_code != 200:
            continue
        for it in resp2.json().get("instance_types", []):
            table.add_row(
                cluster_key,
                it.get("name", ""),
                str(it.get("gpu", "")),
                str(it.get("cpu", "")),
                str(it.get("memory", "")),
            )

    return table


def get_images(cluster_type: str = "MT") -> Table:
    """Fetch available Docker images from cluster config. Returns a ``rich.Table``."""
    resp = requests.get(
        f"{_base_url()}/v2/config",
        params={"region_type": cluster_type},
        headers=_auth_headers(),
        timeout=30,
    )
    if resp.status_code != 200:
        raise ApiError(resp.status_code, resp.text)

    config = resp.json()

    table = Table(title="Available Images")
    table.add_column("Type", style="magenta")
    table.add_column("Name", style="cyan")
    table.add_column("Tags", style="green")

    seen = set()
    for image in config.get("customImages", []):
        key = ("CUSTOM", image.get("name", ""))
        if key not in seen:
            seen.add(key)
            table.add_row("CUSTOM", image.get("name", ""), ", ".join(image.get("tags", [])))

    for image in config.get("datahubImages", []):
        key = ("DATAHUB", image.get("name", ""))
        if key not in seen:
            seen.add(key)
            table.add_row("DATAHUB", image.get("name", ""), ", ".join(image.get("tags", [])))

    for region in config.get("regions", []):
        for inst in region.get("instances_types", []):
            for image in inst.get("images", []):
                key = ("REGION", image.get("name", ""))
                if key not in seen:
                    seen.add(key)
                    table.add_row("REGION", image.get("name", ""), ", ".join(image.get("tags", [])))

    return table


def build_image(
    from_image: str,
    requirements_file: str,
    install_type: str = "pip",
    conda_env: Optional[str] = None,
    poetrylock_file: Optional[str] = None,
) -> dict:
    """Submit an image build job. Returns dict with ``job_name`` and ``new_image``."""
    payload: Dict = {
        "from_image": from_image,
        "requirements_file": requirements_file,
        "install_type": install_type,
    }
    if conda_env:
        payload["conda_env"] = conda_env
    if poetrylock_file:
        payload["poetrylock_file"] = poetrylock_file

    resp = requests.post(
        f"{_base_url()}/image_build",
        json=payload,
        headers=_auth_headers(),
        timeout=30,
    )
    if resp.status_code != 200:
        raise ApiError(resp.status_code, resp.text)
    data = resp.json()
    return {
        "job_name": data.get("job_name", ""),
        "new_image": data.get("image", ""),
    }
