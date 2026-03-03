from typing import Optional

import typer
import yaml

from cryri import __version__
from cryri.config import CryConfig, CloudConfig
from cryri.display import (
    console,
    setup_logging,
    render_config_panel,
    render_build_image_panel,
    confirm_submission,
    render_jobs_table,
    interactive_job_select,
    print_success,
    print_error,
)
from cryri.job_manager import JobManager, JobNotFoundError, ClientLibMissingError
from cryri.utils import create_job_description, create_run_copy

try:
    import client_lib
except ModuleNotFoundError:
    client_lib = None

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


def submit_run(cfg: CryConfig) -> str:
    """Submit a job run with the given configuration."""
    if cfg.container.run_from_copy:
        assert cfg.container.cry_copy_dir, \
            f'Copy dir is not set: {cfg.container.cry_copy_dir}'
        cfg.container.work_dir = create_run_copy(cfg.container)

    job_description = create_job_description(cfg)

    quoted_command = cfg.container.command.replace('"', '\\"')
    run_script = f'bash -c "cd {cfg.container.work_dir} && {quoted_command}"'

    job = client_lib.Job(
        base_image=cfg.container.image,
        script=run_script,
        instance_type=cfg.cloud.instance_type,
        processes_per_worker=cfg.cloud.processes_per_worker,
        n_workers=cfg.cloud.n_workers,
        region=cfg.cloud.region,
        type='binary',
        env_variables=cfg.container.environment,
        job_desc=job_description,
    )
    return job.submit()


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
        with console.status("[bold green]Submitting job...[/bold green]"):
            status = submit_run(cfg)
        print_success(f"Job submitted: {status}")
    except Exception as e:
        print_error(f"Failed to submit job: {e}")
        raise typer.Exit(code=1)


@app.command()
def jobs(
    region: str = typer.Option("SR006", "--region", "-r", help="Cloud region."),
):
    """List running jobs."""
    jm = JobManager(region)
    try:
        with console.status("[bold green]Fetching jobs...[/bold green]"):
            structured = jm.get_jobs_structured()
    except ClientLibMissingError as e:
        print_error(str(e))
        raise typer.Exit(code=1)

    if not structured:
        console.print("[dim]No jobs found.[/dim]")
        raise typer.Exit()

    console.print(render_jobs_table(structured))


@app.command()
def logs(
    hash: Optional[str] = typer.Argument(None, help="Job hash (interactive selection if omitted)."),
    region: str = typer.Option("SR006", "--region", "-r", help="Cloud region."),
):
    """Show logs for a job."""
    jm = JobManager(region)

    if hash is None:
        try:
            with console.status("[bold green]Fetching jobs...[/bold green]"):
                structured = jm.get_jobs_structured()
        except ClientLibMissingError as e:
            print_error(str(e))
            raise typer.Exit(code=1)

        if not structured:
            console.print("[dim]No jobs found.[/dim]")
            raise typer.Exit()

        hash = interactive_job_select(structured, "view logs")

    try:
        with console.status("[bold green]Fetching logs...[/bold green]"):
            output = jm.show_logs(hash)
        console.print(output)
    except JobNotFoundError as e:
        print_error(str(e))
        raise typer.Exit(code=1)
    except ClientLibMissingError as e:
        print_error(str(e))
        raise typer.Exit(code=1)


@app.command()
def kill(
    hash: Optional[str] = typer.Argument(None, help="Job hash (interactive selection if omitted)."),
    region: str = typer.Option("SR006", "--region", "-r", help="Cloud region."),
):
    """Kill a running job."""
    jm = JobManager(region)

    if hash is None:
        try:
            with console.status("[bold green]Fetching jobs...[/bold green]"):
                structured = jm.get_jobs_structured()
        except ClientLibMissingError as e:
            print_error(str(e))
            raise typer.Exit(code=1)

        if not structured:
            console.print("[dim]No jobs found.[/dim]")
            raise typer.Exit()

        hash = interactive_job_select(structured, "kill")

    try:
        with console.status("[bold green]Terminating job...[/bold green]"):
            full_hash = jm.kill_job(hash)
        print_success(f"Job {full_hash} terminated successfully.")
    except JobNotFoundError as e:
        print_error(str(e))
        raise typer.Exit(code=1)
    except ClientLibMissingError as e:
        print_error(str(e))
        raise typer.Exit(code=1)


@app.command("build-image")
def build_image(
    requirements_file: str = typer.Argument(..., help="Path to requirements.txt, pyproject.toml, or environment.yml."),
    image: str = typer.Option(..., "--image", help="Base Docker image to extend."),
    install_type: str = typer.Option("pip", "--type", help="Install method: pip / conda / poetry."),
    conda_env: Optional[str] = typer.Option(None, "--conda-env", help="Conda environment name to activate before installing."),
    poetry_lock: Optional[str] = typer.Option(None, "--poetry-lock", help="Path to poetry.lock file (poetry type only)."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt (for CI)."),
):
    """Build a custom Docker image by installing packages on top of a base image."""
    import os

    if not os.path.isfile(requirements_file):
        print_error(f"Requirements file '{requirements_file}' not found.")
        raise typer.Exit(code=1)

    console.print(render_build_image_panel(image, requirements_file, install_type, conda_env, poetry_lock))

    if not yes and not confirm_submission():
        print_error("Build cancelled.")
        raise typer.Exit()

    try:
        with console.status("[bold green]Submitting image build...[/bold green]"):
            result = JobManager.build_image(
                from_image=image,
                requirements_file=requirements_file,
                install_type=install_type,
                conda_env=conda_env,
                poetrylock_file=poetry_lock,
            )
        print_success(f"Image build submitted: {result['result']}")
        if result.get("job_name"):
            console.print(f"  Job name:  [cyan]{result['job_name']}[/cyan]")
        if result.get("new_image"):
            console.print(f"  New image: [cyan]{result['new_image']}[/cyan]")
    except ClientLibMissingError as e:
        print_error(str(e))
        raise typer.Exit(code=1)
    except Exception as e:
        print_error(f"Failed to submit image build: {e}")
        raise typer.Exit(code=1)


@app.command()
def instances(
    region: str = typer.Option("SR006", "--region", "-r", help="Cloud region."),
):
    """Show available instance types."""
    jm = JobManager(region)
    try:
        with console.status("[bold green]Fetching instance types...[/bold green]"):
            table = jm.get_instance_types()
        console.print(table)
    except ClientLibMissingError as e:
        print_error(str(e))
        raise typer.Exit(code=1)


def main():
    app()


if __name__ == '__main__':
    main()
