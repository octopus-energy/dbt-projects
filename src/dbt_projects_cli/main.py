"""
Main CLI entry point for dbt-projects-cli.

This tool provides utilities for managing dbt projects at Octopus Energy,
including documentation generation, model analysis, and integration with
external services like Databricks and LLMs.
"""

import click
from rich.console import Console
from rich.table import Table

from .commands import scaffold, migrate

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


# Info command removed - requires project_discovery which is in core infrastructure


# Add command groups
cli.add_command(scaffold.scaffold)
cli.add_command(migrate.migrate)


if __name__ == "__main__":
    cli()
