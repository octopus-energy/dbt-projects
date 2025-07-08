"""
Team catalog system for managing and sharing dbt teams across packages.
Provides functionality to store, retrieve, and manage team configurations.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table

console = Console()

@dataclass
class TeamConfig:
    """
    Configuration for a dbt team.
    Teams own multiple domains and are mapped in the meta tag for groups. 
    """

    name: str
    description: str
    domains: List[str]
    owner_name: str
    owner_email: Optional[str] = None
    contact: Optional[str] = None


class TeamCatalog:
    """Manages a catalog of reusable team configurations."""

    def __init__(self):
        self.catalog_path = Path.cwd() / "teams-catalog.yml"
        self.catalog_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_catalog_exists()

    def _ensure_catalog_exists(self) -> None:
        """Ensure the catalog file exists with default teams."""
        if not self.catalog_path.exists():
            default_teams = {
                "data_platform": TeamConfig(
                    name="data_platform",
                    description="Team responsible for data platform infrastructure and utilities",
                    domains=["databricks", "bigquery"],
                    owner_name="Data Platform Team",
                    contact="Data Platform Team Lead",
                )
            }
            self._save_catalog(default_teams)

    def _load_catalog(self) -> Dict[str, TeamConfig]:
        """Load the team catalog from file."""
        try:
            with open(self.catalog_path, "r") as f:
                data = yaml.safe_load(f) or {}

            teams = {}

            if 'teams' in data:
                for team_data in data['teams']:
                    name = team_data['name']
                    config_meta = team_data.get('config', {}).get('meta', {})
                    teams[name] = TeamConfig(
                        name=name,
                        description=team_data.get('description', ''),
                        domains=team_data.get('domains', []),
                        owner_name=config_meta.get('data_owner', ''),
                        owner_email=team_data.get('owner', {}).get('email', ''),
                        contact=config_meta.get('contact'),
                    )
            else:
                # Handle legacy format
                for name, config in data.items():
                    if isinstance(config, dict) and 'name' in config:
                        teams[name] = TeamConfig(**config)

            return teams
        except Exception as e:
            console.print(f"[red]Error loading team catalog: {e}[/red]")
            return {}

    def _save_catalog(self, teams: Dict[str, TeamConfig]) -> None:
        """Save the team catalog to file."""
        try:
            data = {
                'teams': [
                    {
                        'name': config.name,
                        'domains': config.domains,
                        'owner': {
                            'email': config.owner_email
                        },
                        'description': config.description,
                        'config': {
                            'meta': {
                                'data_owner': config.owner_name
                            }
                        }
                    } for config in teams.values()
                ]
            }
            with open(self.catalog_path, "w") as f:
                yaml.dump(data, f, default_flow_style=False, sort_keys=True)
        except Exception as e:
            console.print(f"[red]Error saving team catalog: {e}[/red]")

    def list_teams(self) -> Dict[str, TeamConfig]:
        """List all available teams in the catalog."""
        return self._load_catalog()

    def get_team(self, name: str) -> Optional[TeamConfig]:
        """Get a specific team by name."""
        catalog = self._load_catalog()
        return catalog.get(name)

    def add_team(self, team: TeamConfig) -> None:
        """Add a new team to the catalog."""
        catalog = self._load_catalog()
        catalog[team.name] = team
        self._save_catalog(catalog)
        console.print(f"[green]✅ Added team '{team.name}' to catalog[/green]")

    def update_team(self, team: TeamConfig) -> None:
        """Update an existing team in the catalog."""
        catalog = self._load_catalog()
        if team.name not in catalog:
            console.print(f"[red]Team '{team.name}' not found in catalog[/red]")
            return

        catalog[team.name] = team
        self._save_catalog(catalog)
        console.print(f"[green]✅ Updated team '{team.name}' in catalog[/green]")

    def remove_team(self, name: str) -> bool:
        """Remove a team from the catalog."""
        catalog = self._load_catalog()
        if name not in catalog:
            console.print(f"[red]Team '{name}' not found in catalog[/red]")
            return False

        del catalog[name]
        self._save_catalog(catalog)
        console.print(f"[green]✅ Removed team '{name}' from catalog[/green]")
        return True

    def get_catalog_info(self) -> Dict[str, str]:
        """Get information about the catalog location and source."""
        return {
            "path": str(self.catalog_path),
            "exists": str(self.catalog_path.exists()),
            "writable": str(
                self.catalog_path.parent.exists()
                and self.catalog_path.parent.stat().st_mode & 0o200
            ),
        }

    def show_catalog(self) -> None:
        """Display the team catalog in a formatted table."""
        catalog = self._load_catalog()

        # Show catalog source information
        info = self.get_catalog_info()
        console.print(f"[dim]📁 Using catalog: {info['path']}[/dim]")

        if not catalog:
            console.print("[yellow]No teams found in catalog[/yellow]")
            return

        table = Table(title="Available Teams")
        table.add_column("Name", style="cyan")
        table.add_column("Description", style="green")
        table.add_column("Domains", style="magenta")
        table.add_column("Owner", style="yellow")
        table.add_column("Email", style="blue")

        for name, team in sorted(catalog.items()):
            table.add_row(
                team.name,
                team.description,
                ", ".join(team.domains),
                team.owner_name,
                team.owner_email or "",
            )

        console.print(table)

    def select_team_interactive(self) -> Optional[TeamConfig]:
        """Interactive team selection from catalog."""
        catalog = self._load_catalog()

        if not catalog:
            console.print("[yellow]No teams found in catalog[/yellow]")
            return None

        console.print("[bold blue]Available teams:[/bold blue]")

        # Display teams with numbers
        teams_list = list(catalog.values())
        for i, team in enumerate(teams_list, 1):
            console.print(f"{i}. [cyan]{team.name}[/cyan] - {team.description}")

        console.print(f"{len(teams_list) + 1}. [yellow]Create new team[/yellow]")

        while True:
            try:
                choice = Prompt.ask(
                    f"Select a team (1-{len(teams_list) + 1})", default="1"
                )
                choice_num = int(choice)

                if 1 <= choice_num <= len(teams_list):
                    selected_team = teams_list[choice_num - 1]
                    console.print(
                        f"[green]Selected team: {selected_team.name}[/green]"
                    )
                    return selected_team
                elif choice_num == len(teams_list) + 1:
                    return None  # User wants to create a new team
                else:
                    console.print(
                        f"[red]Please enter a number between 1 and "
                        f"{len(teams_list) + 1}[/red]"
                    )
            except ValueError:
                console.print("[red]Please enter a valid number[/red]")


def collect_team_info_interactive() -> TeamConfig:
    """Collect team information interactively from user."""
    console.print("[bold cyan]Creating new team configuration[/bold cyan]")

    name = Prompt.ask("[bold cyan]Team name[/bold cyan]")
    description = Prompt.ask("[bold cyan]Team description[/bold cyan]")
    domains = Prompt.ask("[bold cyan]Domains (comma separated)[/bold cyan]").split(',')
    owner_name = Prompt.ask("[bold cyan]Owner name[/bold cyan]")
    owner_email = Prompt.ask("[bold cyan]Owner email[/bold cyan]")
    contact = Prompt.ask("[bold cyan]Contact (optional)[/bold cyan]", default="")

    return TeamConfig(
        name=name,
        description=description,
        domains=[d.strip() for d in domains],
        owner_name=owner_name,
        owner_email=owner_email,
        contact=contact if contact else None,
    )


def get_team_config(
    team_name: Optional[str] = None,
    team_description: Optional[str] = None,
    team_domains: Optional[List[str]] = None,
    team_owner: Optional[str] = None,
    team_email: Optional[str] = None,
    team_contact: Optional[str] = None,
    team_from_catalog: Optional[str] = None,
    interactive: bool = True,
) -> TeamConfig:
    """
    Get team configuration from various sources.

    Priority: CLI args > catalog selection > interactive prompts
    """
    catalog = TeamCatalog()

    # If all required CLI args provided, use them
    if team_name and team_description and team_owner and team_email:
        team = TeamConfig(
            name=team_name,
            description=team_description,
            domains=team_domains or [],
            owner_name=team_owner,
            owner_email=team_email,
            contact=team_contact,
        )
        # Auto-save new team to catalog
        catalog.add_team(team)
        return team

    # If specific catalog team requested, use it
    if team_from_catalog:
        catalog_team = catalog.get_team(team_from_catalog)
        if catalog_team:
            console.print(
                f"[green]Using team '{team_from_catalog}' from catalog[/green]"
            )
            return catalog_team
        else:
            console.print(
                f"[red]Team '{team_from_catalog}' not found in catalog[/red]"
            )

    # Interactive mode
    if interactive:
        selected_team = catalog.select_team_interactive()
        if selected_team:
            return selected_team

        # User wants to create new team
        new_team = collect_team_info_interactive()
        # Auto-save to catalog
        catalog.add_team(new_team)
        return new_team

    # Default fallback
    console.print("[yellow]Using default data platform team[/yellow]")
    return TeamConfig(
        name="data_platform",
        description="Team responsible for data platform infrastructure and utilities",
        domains=["databricks", "bigquery"],
        owner_name="Data Platform Team",
        owner_email="data-platform@octopusenergy.com",
    )

