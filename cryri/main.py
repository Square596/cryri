from pathlib import Path
from typing import Optional

import typer
import yaml
from rich.prompt import Confirm

from cryri import __version__
from cryri.api import ApiError
from cryri.config import CryConfig, CloudConfig, DEFAULT_REGION
from cryri.display import (
    console,
    setup_logging,
    render_config_panel,
    confirm_submission,
    render_jobs_table,
    interactive_job_select,
    print_success,
    print_error,
    prompt_text,
    prompt_select,
    prompt_env_vars,
)
from cryri.job_manager import JobManager, JobNotFoundError, ClientLibMissingError

app = typer.Typer(no_args_is_help=True, rich_markup_mode="rich")


def _version_callback(value: bool):
    if value:
        console.print(f"cryri [bold cyan]{__version__}[/bold cyan]")
        raise typer.Exit()


@app.callback()
def callback(
    version: bool = typer.Option(
        False, "--version", "-v",
        help="Show cryri version and exit.",
        callback=_version_callback,
        is_eager=True,
    ),
):
    """[bold cyan]cryri[/bold cyan] — Cloud job runner CLI."""
    setup_logging()


def _resolve_job_interactive(jm: JobManager, hash: Optional[str], action: str) -> str:
    """Resolve a job name from a hash or via interactive selection."""
    if hash is not None:
        full_name = jm.find_job_by_hash(hash)
        if full_name is None:
            raise JobNotFoundError(f"No job found matching '{hash}'")
        return full_name

    try:
        with console.status("[bold green]Fetching jobs...[/bold green]"):
            structured = jm.get_jobs_structured()
    except (ApiError, ClientLibMissingError) as e:
        print_error(str(e))
        raise typer.Exit(code=1)

    if not structured:
        console.print("[dim]No jobs found.[/dim]")
        raise typer.Exit()

    return interactive_job_select(structured, action)


@app.command()
def init(
    output: str = typer.Option("run.yaml", "--output", "-o", help="Output YAML file path."),
):
    """Interactive wizard to create a job config file."""
    console.print("\n  [bold cyan]Welcome to cryri![/bold cyan] Let's set up your job.\n")

    # Derive default description from parent_dir-current_dir
    cwd = Path.cwd()
    default_description = f"{cwd.parent.name}-{cwd.name}"

    DEFAULT_IMAGE = "cr.ai.cloud.ru/aicloud-base-images/cuda12.1-torch2-py311:0.0.36"
    INSTANCE_CHOICES = [
        "cpu.2C.8G",
        "a100plus.1gpu.80vG.12C.96G",
    ]

    command = prompt_text("Command to run", default="python3 main.py")
    if not command:
        print_error("Command is required.")
        raise typer.Exit(code=1)

    image = prompt_text("Docker image", default=DEFAULT_IMAGE)
    work_dir = prompt_text("Working directory", default=".")
    instance_type = prompt_select("Instance type", INSTANCE_CHOICES)
    region = prompt_text("Region", default=DEFAULT_REGION)
    description = prompt_text("Description", default=default_description)
    environment = prompt_env_vars()

    # Build config dict
    container = {"command": command, "image": image, "work_dir": work_dir or ".", "run_from_copy": False}
    if environment:
        container["environment"] = environment

    cloud = {
        "description": description or default_description,
        "instance_type": instance_type,
        "n_workers": 1,
    }
    if region and region != DEFAULT_REGION:
        cloud["region"] = region

    config_dict = {"container": container, "cloud": cloud}

    # Build CryConfig for display
    cfg = CryConfig(**{
        "container": {**container},
        "cloud": {**cloud, "region": region or DEFAULT_REGION},
    })

    console.print()
    console.print(render_config_panel(cfg))
    console.print()

    if Path(output).exists():
        if not Confirm.ask(f"[yellow]{output}[/yellow] already exists. Overwrite?", default=False):
            console.print("[dim]Cancelled.[/dim]")
            raise typer.Exit()

    with open(output, "w", encoding="utf-8") as f:
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)
    print_success(f"Config saved to {output}")

    # Create starter main.py if using default command and it doesn't exist
    if command == "python3 main.py" and not Path("main.py").exists():
        Path("main.py").write_text(
            'import os\n\nprint("Hello from cryri!")\nprint(f"Running on {os.uname().nodename}")\n'
        )
        print_success("Created starter main.py")


@app.command()
def submit(
    config_file: str = typer.Argument(..., help="Path to the YAML configuration file."),
    region: Optional[str] = typer.Option(None, "--region", "-r", help="Cloud region override."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt (for CI)."),
):
    """Submit a job from a YAML config file."""
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            cfg = CryConfig(**yaml.safe_load(f))
    except FileNotFoundError:
        print_error(f"Configuration file '{config_file}' not found.")
        raise typer.Exit(code=1)
    except yaml.YAMLError as e:
        print_error(f"Error parsing YAML file: {e}")
        raise typer.Exit(code=1)

    if region:
        cfg.cloud.region = region

    console.print(render_config_panel(cfg))

    if not yes and not confirm_submission():
        print_error("Submission cancelled.")
        raise typer.Exit()

    try:
        jm = JobManager(cfg.cloud.region)
        with console.status("[bold green]Submitting job...[/bold green]"):
            status = jm.submit_run(cfg)
        print_success(f"Job submitted: {status}")
    except (ApiError, ClientLibMissingError) as e:
        print_error(f"Failed to submit job: {e}")
        raise typer.Exit(code=1)
    except Exception as e:
        print_error(f"Failed to submit job: {e}")
        raise typer.Exit(code=1)


@app.command()
def jobs(
    region: str = typer.Option(DEFAULT_REGION, "--region", "-r", help="Cloud region."),
    limit: int = typer.Option(10, "--limit", "-n", help="Number of latest jobs to show (0 for all)."),
):
    """List running jobs."""
    jm = JobManager(region)
    try:
        with console.status("[bold green]Fetching jobs...[/bold green]"):
            structured = jm.get_jobs_structured()
    except (ApiError, ClientLibMissingError) as e:
        print_error(str(e))
        raise typer.Exit(code=1)

    if not structured:
        console.print("[dim]No jobs found.[/dim]")
        raise typer.Exit()

    total = len(structured)
    if limit > 0 and total > limit:
        structured = structured[-limit:]
        console.print(f"[dim]Showing {limit} of {total} jobs. Use -n 0 to show all.[/dim]\n")

    console.print(render_jobs_table(structured))


@app.command()
def logs(
    hash: Optional[str] = typer.Argument(None, help="Job hash (interactive selection if omitted)."),
    region: str = typer.Option(DEFAULT_REGION, "--region", "-r", help="Cloud region."),
    raw: bool = typer.Option(False, "--raw", help="Show raw unfiltered logs."),
):
    """Show logs for a job."""
    jm = JobManager(region)

    try:
        job_name = _resolve_job_interactive(jm, hash, "view logs")
    except JobNotFoundError as e:
        print_error(str(e))
        raise typer.Exit(code=1)

    try:
        jm.show_logs(job_name, raw=raw)
    except (ApiError, ClientLibMissingError) as e:
        print_error(str(e))
        raise typer.Exit(code=1)


@app.command()
def kill(
    hash: Optional[str] = typer.Argument(None, help="Job hash (interactive selection if omitted)."),
    region: str = typer.Option(DEFAULT_REGION, "--region", "-r", help="Cloud region."),
):
    """Kill a running job."""
    jm = JobManager(region)

    try:
        job_name = _resolve_job_interactive(jm, hash, "kill")
    except JobNotFoundError as e:
        print_error(str(e))
        raise typer.Exit(code=1)

    try:
        with console.status("[bold green]Terminating job...[/bold green]"):
            jm.kill_job(job_name)
        print_success(f"Job {job_name} terminated successfully.")
    except (ApiError, ClientLibMissingError) as e:
        print_error(str(e))
        raise typer.Exit(code=1)


@app.command()
def instances(
    region: str = typer.Option(DEFAULT_REGION, "--region", "-r", help="Cloud region."),
):
    """Show available instance types."""
    jm = JobManager(region)
    try:
        with console.status("[bold green]Fetching instance types...[/bold green]"):
            table = jm.get_instance_types()
        console.print(table)
    except (ApiError, ClientLibMissingError) as e:
        print_error(str(e))
        raise typer.Exit(code=1)


def main():
    app()


if __name__ == '__main__':
    main()
