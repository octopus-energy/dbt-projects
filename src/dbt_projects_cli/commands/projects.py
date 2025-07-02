"""
Command module for managing dbt projects.
"""

import click
from rich.console import Console
from rich.table import Table

console = Console()


@click.group()
def projects() -> None:
    """Commands for managing dbt projects."""
    pass


@projects.command()
def list() -> None:
    """List all dbt projects in the repository."""
    from ..core.project_discovery import ProjectDiscovery
    
    discovery = ProjectDiscovery()
    projects_info = discovery.discover_all_projects()
    
    table = Table(title="All dbt Projects")
    table.add_column("Type", style="cyan")
    table.add_column("Project Name", style="magenta")
    table.add_column("Path", style="green")
    table.add_column("Models", justify="right", style="blue")
    table.add_column("Macros", justify="right", style="yellow")
    table.add_column("Tests", justify="right", style="red")
    
    for project_type, projects_list in projects_info.items():
        for project in projects_list:
            table.add_row(
                project_type.title(),
                project["name"],
                project["path"],
                str(project.get("model_count", 0)),
                str(project.get("macro_count", 0)),
                str(project.get("test_count", 0))
            )
    
    console.print(table)


@projects.command()
@click.argument('project_name')
def info(project_name: str) -> None:
    """Show detailed information about a specific project."""
    from ..core.project_discovery import ProjectDiscovery
    
    discovery = ProjectDiscovery()
    project = discovery.get_project_by_name(project_name)
    
    if not project:
        console.print(f"[red]Project '{project_name}' not found[/red]")
        return
    
    console.print(f"[bold blue]Project: {project.name}[/bold blue]")
    console.print(f"Type: {project.project_type}")
    console.print(f"Path: {project.path}")
    console.print(f"Models: {project.model_count}")
    console.print(f"Macros: {project.macro_count}")
    console.print(f"Tests: {project.test_count}")
    
    if project.config.get('profile'):
        console.print(f"Profile: {project.config['profile']}")
    
    if project.config.get('version'):
        console.print(f"Version: {project.config['version']}")
