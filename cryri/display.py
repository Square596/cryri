import logging
from typing import List, Dict

from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.prompt import Confirm, IntPrompt
from rich.table import Table
from rich.text import Text

console = Console()


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )


def render_config_panel(cfg) -> Panel:
    lines: List[str] = []
    c = cfg.container
    cl = cfg.cloud

    if c.image:
        lines.append(f"[bold]Image:[/bold]         {c.image}")
    if c.command:
        lines.append(f"[bold]Command:[/bold]       {c.command}")
    if cl.instance_type:
        lines.append(f"[bold]Instance:[/bold]      {cl.instance_type}")
    if cl.region:
        lines.append(f"[bold]Region:[/bold]        {cl.region}")
    if cl.n_workers and cl.n_workers > 1:
        lines.append(f"[bold]Workers:[/bold]        {cl.n_workers}")
    if c.work_dir:
        lines.append(f"[bold]Work dir:[/bold]      {c.work_dir}")
    if c.run_from_copy:
        lines.append(f"[bold]Run from copy:[/bold] True")
    if cl.description:
        lines.append(f"[bold]Description:[/bold]   {cl.description}")
    if c.environment:
        lines.append("[bold]Environment:[/bold]")
        for k, v in c.environment.items():
            display_v = v if len(str(v)) <= 40 else str(v)[:37] + "..."
            lines.append(f"  {k} = {display_v}")

    body = "\n".join(lines) if lines else "[dim]No configuration details[/dim]"
    return Panel(body, title="[bold cyan]Job Configuration[/bold cyan]", border_style="cyan")



def confirm_submission() -> bool:
    return Confirm.ask("[bold yellow]Submit this job?[/bold yellow]")


STATUS_STYLES = {
    "Running": "bold green",
    "Completed": "dim",
    "Failed": "bold red",
    "Pending": "bold yellow",
}


def _format_created_at(value) -> str:
    if not value:
        return ""
    from datetime import datetime
    try:
        if isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(value)
        else:
            dt = datetime.fromisoformat(str(value))
        return dt.strftime("%b %d %H:%M")
    except (ValueError, TypeError, OSError):
        return str(value)


def render_jobs_table(jobs: List[Dict]) -> Table:
    table = Table(title="Jobs", show_lines=False)
    table.add_column("#", style="dim", width=4)
    table.add_column("Job Name", style="cyan")
    table.add_column("Created", style="dim")
    table.add_column("Status")

    for job in jobs:
        status = job.get("status", "")
        style = STATUS_STYLES.get(status, "")
        status_text = Text(status, style=style)
        table.add_row(
            str(job["index"]),
            str(job["job_name"]),
            _format_created_at(job.get("created_at", "")),
            status_text,
        )

    return table


def interactive_job_select(jobs: List[Dict], action: str) -> str:
    console.print(f"\n[bold]Select a job to {action}:[/bold]\n")
    for job in jobs:
        status = job.get("status", "")
        style = STATUS_STYLES.get(status, "")
        console.print(
            f"  [dim]{job['index']}.[/dim] [cyan]{job['job_name']}[/cyan]  [{style}]{status}[/{style}]"
        )
    console.print()

    choice = IntPrompt.ask(
        "Enter job number",
        choices=[str(j["index"]) for j in jobs],
    )
    selected = next(j for j in jobs if j["index"] == choice)
    return selected["job_name"]


def print_success(msg: str) -> None:
    console.print(f"[bold green]{msg}[/bold green]")


def print_error(msg: str) -> None:
    console.print(f"[bold red]{msg}[/bold red]")


def print_warning(msg: str) -> None:
    console.print(f"[bold yellow]{msg}[/bold yellow]")
