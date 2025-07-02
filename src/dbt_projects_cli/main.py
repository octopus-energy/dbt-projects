"""
Main CLI entry point for dbt-projects-cli.

This tool provides utilities for managing dbt projects at Octopus Energy,
including documentation generation, model analysis, and integration with
external services like Databricks and LLMs.
"""

import click
from rich.console import Console
from rich.table import Table

from .commands import descriptions, projects, models, utils

console = Console()


@click.group()
@click.version_option()
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
@click.pass_context
def cli(ctx: click.Context, verbose: bool) -> None:
    """
    🐙 dbt-projects-cli - Octopus Energy dbt project management tool.
    
    This CLI provides utilities for managing dbt projects including:
    - Adding descriptions to models and columns
    - Project discovery and analysis
    - Integration with Databricks and LLMs
    - Model validation and linting
    """
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    
    if verbose:
        console.print("[bold blue]dbt-projects-cli[/bold blue] - Debug mode enabled", style="dim")


@cli.command()
def info() -> None:
    """Show information about the current dbt projects repository."""
    from .core.project_discovery import ProjectDiscovery
    
    discovery = ProjectDiscovery()
    projects_info = discovery.discover_all_projects()
    
    table = Table(title="dbt Projects Overview")
    table.add_column("Type", style="cyan")
    table.add_column("Project", style="magenta")
    table.add_column("Location", style="green")
    table.add_column("Models", justify="right", style="blue")
    
    for project_type, projects_list in projects_info.items():
        for project in projects_list:
            table.add_row(
                project_type.title(),
                project["name"],
                project["path"],
                str(project.get("model_count", "N/A"))
            )
    
    console.print(table)


# Add command groups
cli.add_command(descriptions.descriptions)
cli.add_command(projects.projects)
cli.add_command(models.models)
cli.add_command(utils.utils)


if __name__ == "__main__":
    cli()
