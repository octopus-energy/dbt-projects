"""
Command module for managing dbt models.
"""

import click
from rich.console import Console
from rich.table import Table

console = Console()


@click.group()
def models() -> None:
    """Commands for managing dbt models."""
    pass


@models.command()
@click.option("--project", "-p", help="Filter by project name")
def list(project: str) -> None:
    """List all models in the repository or a specific project."""
    from ..core.project_discovery import ProjectDiscovery

    discovery = ProjectDiscovery()

    if project:
        # List models for a specific project
        models_list = discovery.list_models_in_project(project)
        if not models_list:
            console.print(f"[red]No models found for project '{project}'[/red]")
            return

        console.print(f"[bold blue]Models in project '{project}':[/bold blue]")
        for model_path in models_list:
            console.print(f"  {model_path.relative_to(discovery.root_path)}")
    else:
        # List all models across all projects
        projects_info = discovery.discover_all_projects()

        table = Table(title="All dbt Models")
        table.add_column("Project", style="cyan")
        table.add_column("Model Name", style="magenta")
        table.add_column("Path", style="green")

        for project_type, projects_list in projects_info.items():
            for project_info in projects_list:
                project_name = project_info["name"]
                models_list = discovery.list_models_in_project(project_name)

                for model_path in models_list:
                    table.add_row(
                        project_name,
                        model_path.stem,
                        str(model_path.relative_to(discovery.root_path)),
                    )

        console.print(table)


@models.command()
@click.argument("model_path")
def analyze(model_path: str) -> None:
    """Analyze a specific model file."""
    from pathlib import Path

    model_file = Path(model_path)

    if not model_file.exists():
        console.print(f"[red]Model file '{model_path}' not found[/red]")
        return

    console.print(f"[bold blue]Analyzing model: {model_file.name}[/bold blue]")

    # Read and display basic information about the model
    with open(model_file, "r") as f:
        content = f.read()

    lines = content.split("\n")
    console.print(f"Lines of code: {len(lines)}")

    # Simple analysis of SQL content
    content_lower = content.lower()

    if "select" in content_lower:
        console.print("✓ Contains SELECT statements")
    if "from" in content_lower:
        console.print("✓ Contains FROM clauses")
    if "join" in content_lower:
        console.print("✓ Contains JOIN operations")
    if "where" in content_lower:
        console.print("✓ Contains WHERE conditions")
    if "group by" in content_lower:
        console.print("✓ Contains GROUP BY clauses")

    # Look for dbt-specific syntax
    if "{{" in content and "}}" in content:
        console.print("✓ Contains dbt Jinja templating")
    if "ref(" in content:
        console.print("✓ Contains dbt ref() functions")
    if "source(" in content:
        console.print("✓ Contains dbt source() functions")
