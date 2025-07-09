"""
Migration commands for backfilling template changes to existing packages.
"""

import difflib
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click
import yaml
from rich.console import Console
from rich.prompt import Confirm
from rich.table import Table

from dbt_projects_cli.core.template_engine import create_template_engine

console = Console()


@click.group()
def migrate() -> None:
    """Commands for migrating existing packages to new template configurations."""
    pass


@migrate.command()
@click.option("--package-path", "-p", help="Path to the package to migrate")
@click.option(
    "--dry-run", is_flag=True, help="Show what would be changed without applying"
)
@click.option("--force", is_flag=True, help="Apply changes without confirmation")
def package(package_path: Optional[str], dry_run: bool, force: bool) -> None:
    """Migrate a single package to the latest template configuration.

    This updates the dbt_project.yml file with the latest template standards
    while preserving custom configurations. Groups and packages are not modified.

    Example:
        dbt-cli migrate package --package-path \
            packages/domains/source-aligned/databricks/systems-data --dry-run
    """

    if not package_path:
        console.print("[red]Error: --package-path is required[/red]")
        return

    package_dir = Path(package_path)
    if not package_dir.exists():
        console.print(f"[red]Error: Package path {package_path} does not exist[/red]")
        return

    dbt_project_file = package_dir / "dbt_project.yml"
    if not dbt_project_file.exists():
        console.print(f"[red]Error: No dbt_project.yml found in {package_path}[/red]")
        return

    # Load existing configuration
    with open(dbt_project_file, "r") as f:
        existing_config = yaml.safe_load(f)

    package_name = existing_config.get("name")
    if not package_name:
        console.print("[red]Error: No package name found in dbt_project.yml[/red]")
        return

    # Determine alignment and context from existing config
    alignment, context = _infer_package_context(package_dir, existing_config)

    if not alignment:
        console.print(
            f"[red]Error: Could not determine package alignment for "
            f"{package_name}[/red]"
        )
        return

    console.print(
        f"[bold blue]Migrating package '{package_name}' "
        f"({alignment} alignment)[/bold blue]"
    )

    # Generate new configuration using template engine, preserving existing config
    engine = create_template_engine()
    new_dbt_project = engine.generate_dbt_project_yml(context, existing_config)
    new_packages = engine.generate_packages_yml(context)
    new_groups = engine.generate_group_yml(context)

    # Check if any changes are actually needed
    changes_needed = _detect_changes(
        package_dir, new_dbt_project, new_packages, new_groups
    )

    if not changes_needed:
        console.print(
            f"[green]✅ Package '{package_name}' is already up to date with "
            f"the latest template[/green]"
        )
        return

    # Show changes summary
    console.print("\n[bold yellow]Changes to be applied:[/bold yellow]")
    _show_changes_summary(package_dir, new_dbt_project, new_packages, new_groups)

    if dry_run:
        _show_file_diffs(package_dir, new_dbt_project, new_packages, new_groups)
        console.print(
            "\n[yellow]This was a dry run. Use --force to apply changes.[/yellow]"
        )
        return

    # Apply changes
    if not force:
        if not Confirm.ask(f"Apply template updates to {package_name}?"):
            console.print("[yellow]Migration cancelled[/yellow]")
            return

    # Backup existing files
    _backup_files(package_dir)

    # Write only dbt_project.yml - packages and groups should not be updated
    # during migration
    dbt_project_file.write_text(new_dbt_project)

    console.print(
        f"[bold green]✅ Package '{package_name}' migrated successfully![/bold green]"
    )
    console.print("[dim]Backup files created with .bak extension[/dim]")


@migrate.command()
@click.option(
    "--domain-type",
    type=click.Choice(["source-aligned", "consumer-aligned", "utils"]),
    help="Only migrate packages of this type",
)
@click.option(
    "--dry-run", is_flag=True, help="Show what would be changed without applying"
)
@click.option("--force", is_flag=True, help="Apply changes without confirmation")
def all(domain_type: Optional[str], dry_run: bool, force: bool) -> None:
    """Migrate all packages in the repository to the latest template configuration."""

    console.print("[bold blue]Scanning for packages to migrate...[/bold blue]")

    packages = _find_packages(domain_type)

    if not packages:
        console.print("[yellow]No packages found to migrate[/yellow]")
        return

    # Show summary
    table = Table(title="Packages to Migrate")
    table.add_column("Package Name", style="cyan")
    table.add_column("Alignment", style="green")
    table.add_column("Path", style="yellow")

    for pkg in packages:
        table.add_row(pkg["name"], pkg["alignment"], str(pkg["path"]))

    console.print(table)

    if dry_run:
        console.print(
            "\n[yellow]This was a dry run. Use --force to apply changes.[/yellow]"
        )
        return

    if not force:
        if not Confirm.ask(f"Migrate {len(packages)} packages?"):
            console.print("[yellow]Migration cancelled[/yellow]")
            return

    # Migrate each package
    for pkg in packages:
        console.print(f"\n[bold blue]Migrating {pkg['name']}...[/bold blue]")

        try:
            _migrate_single_package(pkg["path"], pkg["context"])
            console.print(f"[green]✅ {pkg['name']} migrated successfully[/green]")
        except Exception as e:
            console.print(f"[red]❌ Failed to migrate {pkg['name']}: {e}[/red]")

    console.print("\n[bold green]✅ Migration completed![/bold green]")


@migrate.command()
def list_migrations() -> None:
    """List available migrations that can be applied."""

    engine = create_template_engine()
    migrations = engine.get_migrations()

    if not migrations:
        console.print("[yellow]No migrations available[/yellow]")
        return

    table = Table(title="Available Migrations")
    table.add_column("Version", style="cyan")
    table.add_column("Description", style="green")
    table.add_column("Changes", style="yellow")

    for migration in migrations:
        changes_summary = f"{len(migration.get('changes', []))} changes"
        table.add_row(
            migration.get("version", "unknown"),
            migration.get("description", "No description"),
            changes_summary,
        )

    console.print(table)


def _infer_package_context(
    package_dir: Path, existing_config: Dict[str, Any]
) -> Tuple[Optional[str], Dict[str, Any]]:
    """Infer package alignment and context from existing configuration."""

    package_name = existing_config.get("name", "")
    package_path = str(package_dir)

    # Try to infer from path structure
    if "source-aligned" in package_path:
        # Extract source system from path or package name
        path_parts = package_path.split("/")
        source_system = None

        for i, part in enumerate(path_parts):
            if part == "source-aligned" and i + 1 < len(path_parts):
                source_system = path_parts[i + 1]
                break

        if not source_system and "_" in package_name:
            source_system = package_name.split("_")[0]

        domain_name = (
            package_name.replace(f"{source_system}_", "") if source_system else "data"
        )

        return "source-aligned", {
            "package_name": package_name,
            "alignment": "source-aligned",
            "source_system": source_system or "unknown",
            "domain_name": domain_name,
            "description": f"Source-aligned package for {source_system}",
            "group_name": source_system or "data_platform",
            "group_description": (
                f"{(source_system or 'Unknown').title()} system data group"
            ),
        }

    elif "consumer-aligned" in package_path:
        # Extract business area from path or package name
        path_parts = package_path.split("/")
        business_area = None

        for i, part in enumerate(path_parts):
            if part == "consumer-aligned" and i + 1 < len(path_parts):
                business_area = path_parts[i + 1]
                break

        if not business_area and "_" in package_name:
            business_area = package_name.split("_")[0]

        domain_name = (
            package_name.replace(f"{business_area}_", "")
            if business_area
            else "analytics"
        )

        return "consumer-aligned", {
            "package_name": package_name,
            "alignment": "consumer-aligned",
            "business_area": business_area or "analytics",
            "domain_name": domain_name,
            "description": f"Consumer-aligned package for {business_area}",
            "group_name": business_area or "analytics",
            "group_description": (
                f"{(business_area or 'Analytics').title()} analytics group"
            ),
        }

    elif "utils" in package_path or package_name.startswith("utils_"):
        domain_name = (
            package_name.replace("utils_", "")
            if package_name.startswith("utils_")
            else package_name
        )

        return "utils", {
            "package_name": package_name,
            "alignment": "utils",
            "domain_name": domain_name,
            "description": f"Utility package for {domain_name}",
            "group_name": "data_platform",
            "group_description": "Data platform utilities group",
        }

    # Default to utils if we can't determine
    return None, {}


def _find_packages(domain_type: Optional[str] = None) -> List[Dict[str, Any]]:
    """Find all packages that can be migrated."""

    packages: List[Dict[str, Any]] = []
    base_dir = Path("packages")

    if not base_dir.exists():
        return packages

    # Search for dbt_project.yml files
    for dbt_project_file in base_dir.rglob("dbt_project.yml"):
        package_dir = dbt_project_file.parent

        try:
            with open(dbt_project_file, "r") as f:
                config = yaml.safe_load(f)

            package_name = config.get("name")
            if not package_name:
                continue

            alignment, context = _infer_package_context(package_dir, config)

            if alignment and (not domain_type or alignment == domain_type):
                packages.append(
                    {
                        "name": package_name,
                        "alignment": alignment,
                        "path": package_dir,
                        "context": context,
                    }
                )

        except Exception as e:
            console.print(
                f"[dim red]Warning: Could not parse {dbt_project_file}: {e}[/dim red]"
            )

    return packages


def _migrate_single_package(package_dir: Path, context: Dict[str, Any]) -> None:
    """Migrate a single package using the template engine."""

    engine = create_template_engine()

    # Load existing configuration to preserve manual changes
    dbt_project_file = package_dir / "dbt_project.yml"
    existing_config = {}
    if dbt_project_file.exists():
        with open(dbt_project_file, "r") as f:
            existing_config = yaml.safe_load(f)

    # Backup existing files
    _backup_files(package_dir)

    # Generate new configuration, preserving existing config
    new_dbt_project = engine.generate_dbt_project_yml(context, existing_config)
    new_packages = engine.generate_packages_yml(context)
    new_groups = engine.generate_group_yml(context)

    # Write new files
    (package_dir / "dbt_project.yml").write_text(new_dbt_project)
    (package_dir / "packages.yml").write_text(new_packages)

    groups_dir = package_dir / "groups"
    groups_dir.mkdir(exist_ok=True)
    (groups_dir / "_group.yml").write_text(new_groups)


def _detect_changes(
    package_dir: Path,
    new_dbt_project: str,
    new_packages: str,
    new_groups: str,
) -> bool:
    """Detect if there are meaningful changes between existing and new
    configurations.
    """

    # Only check dbt_project.yml for changes
    # Groups and packages should not be updated during migration
    dbt_project_file = package_dir / "dbt_project.yml"
    if dbt_project_file.exists():
        existing_dbt_project = dbt_project_file.read_text().strip()
        if _normalize_yaml_content(existing_dbt_project) != _normalize_yaml_content(
            new_dbt_project
        ):
            return True
    else:
        # If file doesn't exist, we need to create it
        return True

    # No changes detected
    return False


def _normalize_yaml_content(content: str) -> str:
    """Normalize YAML content for comparison by parsing and re-serializing."""
    try:
        # Parse YAML and re-serialize to normalize formatting
        parsed = yaml.safe_load(content)
        # Sort keys for consistent comparison
        normalized = yaml.dump(parsed, default_flow_style=False, sort_keys=True)
        return normalized.strip()
    except yaml.YAMLError:
        # If parsing fails, return original content for comparison
        return content.strip()


def _show_changes_summary(
    package_dir: Path, new_dbt_project: str, new_packages: str, new_groups: str
) -> None:
    """Show a summary of what changes will be made."""

    changes = []

    # Only check dbt_project.yml changes
    # Groups and packages are not updated during migration
    dbt_project_file = package_dir / "dbt_project.yml"
    if dbt_project_file.exists():
        existing_dbt_project = dbt_project_file.read_text().strip()
        if _normalize_yaml_content(existing_dbt_project) != _normalize_yaml_content(
            new_dbt_project
        ):
            changes.append(
                "• Update dbt_project.yml with latest template configuration"
            )
    else:
        changes.append("• Create dbt_project.yml")

    if changes:
        for change in changes:
            console.print(f"  {change}")
    else:
        console.print("  [dim]No changes detected[/dim]")


def _show_file_diffs(
    package_dir: Path, new_dbt_project: str, new_packages: str, new_groups: str
) -> None:
    """Show diffs between existing and new file contents."""

    # Only show diff for dbt_project.yml since that's the only file we migrate
    _print_diff(package_dir / "dbt_project.yml", new_dbt_project, "dbt_project.yml")


def _print_diff(file_path: Path, new_content: str, file_name: str) -> None:
    """Print unified diff for a file with new content."""
    if file_path.exists():
        existing_content = file_path.read_text().strip()
    else:
        existing_content = ""

    diff = list(
        difflib.unified_diff(
            existing_content.splitlines(),
            new_content.splitlines(),
            fromfile=f"a/{file_name}",
            tofile=f"b/{file_name}",
            lineterm="",
        )
    )

    if diff:
        console.print(f"\n[bold cyan]Diff for {file_name}:[/bold cyan]")
        for line in diff:
            if line.startswith("-"):
                console.print(f"[red]{line}[/red]")
            elif line.startswith("+"):
                console.print(f"[green]{line}[/green]")
            else:
                console.print(line)
    else:
        console.print(f"\n[dim]No changes for {file_name}[/dim]")


def _backup_files(package_dir: Path) -> None:
    """Create backup copies of existing files."""

    files_to_backup = ["dbt_project.yml", "packages.yml", "groups/_group.yml"]

    for file_path in files_to_backup:
        original_file = package_dir / file_path
        if original_file.exists():
            backup_file = package_dir / f"{file_path}.bak"
            backup_file.parent.mkdir(exist_ok=True)
            backup_file.write_text(original_file.read_text())
