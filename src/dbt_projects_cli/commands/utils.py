"""
Utility commands for dbt projects.
"""

import click
from rich.console import Console

console = Console()


@click.group()
def utils() -> None:
    """Utility commands for dbt projects."""
    pass


@utils.command()
def validate() -> None:
    """Validate all dbt projects in the repository."""
    from ..core.project_discovery import ProjectDiscovery

    discovery = ProjectDiscovery()
    projects_info = discovery.discover_all_projects()

    console.print("[bold blue]Validating dbt projects...[/bold blue]")

    total_projects = 0
    valid_projects = 0

    for project_type, projects_list in projects_info.items():
        for project in projects_list:
            total_projects += 1
            project_name = project["name"]

            # Basic validation checks
            has_models = project.get("model_count", 0) > 0
            has_config = project.get("config") is not None
            has_profile = project.get("config", {}).get("profile") is not None

            if has_config and has_profile:
                valid_projects += 1
                status = "✓"
                color = "green"
            else:
                status = "✗"
                color = "red"

            console.print(
                f"[{color}]{status}[/{color}] {project_name} ({project_type})"
            )

            if not has_config:
                console.print(
                    "  [yellow]Warning: Missing or invalid dbt_project.yml[/yellow]"
                )
            if not has_profile:
                console.print("  [yellow]Warning: No profile specified[/yellow]")
            if not has_models:
                console.print("  [yellow]Info: No models found[/yellow]")

    console.print(
        f"\n[bold]Summary:[/bold] {valid_projects}/{total_projects} "
        f"projects validated successfully"
    )


@utils.command()
def clean() -> None:
    """Clean generated files from all dbt projects."""
    import shutil

    from ..core.project_discovery import ProjectDiscovery

    discovery = ProjectDiscovery()
    projects_info = discovery.discover_all_projects()

    console.print("[bold blue]Cleaning dbt projects...[/bold blue]")

    cleaned_dirs = []

    for project_type, projects_list in projects_info.items():
        for project in projects_list:
            project_path = discovery.root_path / project["path"]

            # Common dbt directories to clean
            dirs_to_clean = ["target", "dbt_packages", "logs"]

            for dir_name in dirs_to_clean:
                dir_path = project_path / dir_name
                if dir_path.exists():
                    try:
                        shutil.rmtree(dir_path)
                        cleaned_dirs.append(
                            str(dir_path.relative_to(discovery.root_path))
                        )
                        console.print(
                            f"[green]\u2713[/green] Cleaned "
                            f"{dir_path.relative_to(discovery.root_path)}"
                        )
                    except Exception as e:
                        console.print(
                            f"[red]\u2717[/red] Failed to clean "
                            f"{dir_path.relative_to(discovery.root_path)}: {e}"
                        )

    if cleaned_dirs:
        console.print(
            f"\n[bold green]Successfully cleaned {len(cleaned_dirs)} "
            f"directories[/bold green]"
        )
    else:
        console.print("\n[yellow]No directories to clean[/yellow]")
