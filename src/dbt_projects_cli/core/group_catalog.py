"""
Group catalog system for managing and sharing dbt groups across packages.
Provides functionality to store, retrieve, and manage group configurations.
"""

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from rich.console import Console
from rich.prompt import Confirm, Prompt
from rich.table import Table

console = Console()


@dataclass
class GroupConfig:
    """Configuration for a dbt group."""
    
    name: str
    description: str
    owner_name: str
    owner_email: str
    team: Optional[str] = None
    contact: Optional[str] = None


class GroupCatalog:
    """Manages a catalog of reusable group configurations."""

    def __init__(self, catalog_locations: Optional[List[Path]] = None):
        if catalog_locations is None:
            # Define default catalog search priority
            # 1. Project-level catalog (commitable with repo)
            # 2. User-level catalog (personal overrides)
            # 3. System-level catalog (team defaults)
            catalog_locations = [
                Path.cwd() / "groups-catalog.yml",                 # Project level - highest priority
                Path.cwd() / ".dbt" / "groups.yml",                # Project .dbt folder
                Path.home() / "dbt-groups.yml",                   # User level - visible in home dir
                Path.home() / ".dbt-cli" / "groups.yml",          # User level - hidden folder
            ]

        self.catalog_path = None
        self.catalog_source = "default"
        
        for i, path in enumerate(catalog_locations):
            if path.exists():
                self.catalog_path = path
                if i == 0:
                    self.catalog_source = "project"
                elif i <= 2:
                    self.catalog_source = "user"
                else:
                    self.catalog_source = "system"
                break
        
        if self.catalog_path is None:
            # Default to project-level catalog for new catalogs
            self.catalog_path = catalog_locations[0]
            self.catalog_source = "project"

        self.catalog_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_catalog_exists()
    
    def _ensure_catalog_exists(self) -> None:
        """Ensure the catalog file exists with default groups."""
        if not self.catalog_path.exists():
            default_groups = {
                "analytics": GroupConfig(
                    name="analytics",
                    description="Analytics and business intelligence team",
                    owner_name="Analytics Team",
                    owner_email="analytics@octopusenergy.com",
                    team="Analytics",
                    contact="Analytics Team Lead"
                ),
                "data_engineering": GroupConfig(
                    name="data_engineering", 
                    description="Data engineering and platform team",
                    owner_name="Data Engineering Team",
                    owner_email="data-engineering@octopusenergy.com",
                    team="Data Engineering",
                    contact="Data Engineering Team Lead"
                ),
                "data_platform": GroupConfig(
                    name="data_platform",
                    description="Data platform infrastructure and utilities",
                    owner_name="Data Platform Team", 
                    owner_email="data-platform@octopusenergy.com",
                    team="Data Platform",
                    contact="Data Platform Team Lead"
                ),
                "marketing": GroupConfig(
                    name="marketing",
                    description="Marketing analytics and campaign data",
                    owner_name="Marketing Team",
                    owner_email="marketing@octopusenergy.com", 
                    team="Marketing",
                    contact="Marketing Team Lead"
                ),
                "finance": GroupConfig(
                    name="finance",
                    description="Financial reporting and analytics",
                    owner_name="Finance Team",
                    owner_email="finance@octopusenergy.com",
                    team="Finance", 
                    contact="Finance Team Lead"
                ),
                "operations": GroupConfig(
                    name="operations",
                    description="Operations and supply chain analytics",
                    owner_name="Operations Team",
                    owner_email="operations@octopusenergy.com",
                    team="Operations",
                    contact="Operations Team Lead"
                ),
                "customer_success": GroupConfig(
                    name="customer_success",
                    description="Customer success and support analytics", 
                    owner_name="Customer Success Team",
                    owner_email="customer-success@octopusenergy.com",
                    team="Customer Success",
                    contact="Customer Success Team Lead"
                ),
                "product": GroupConfig(
                    name="product",
                    description="Product analytics and metrics",
                    owner_name="Product Team",
                    owner_email="product@octopusenergy.com",
                    team="Product",
                    contact="Product Team Lead"
                )
            }
            self._save_catalog(default_groups)
    
    def _load_catalog(self) -> Dict[str, GroupConfig]:
        """Load the group catalog from file."""
        try:
            with open(self.catalog_path, "r") as f:
                data = yaml.safe_load(f) or {}
            
            groups = {}
            for name, config in data.items():
                groups[name] = GroupConfig(**config)
            return groups
        except Exception as e:
            console.print(f"[red]Error loading group catalog: {e}[/red]")
            return {}
    
    def _save_catalog(self, groups: Dict[str, GroupConfig]) -> None:
        """Save the group catalog to file."""
        try:
            data = {name: asdict(config) for name, config in groups.items()}
            with open(self.catalog_path, "w") as f:
                yaml.dump(data, f, default_flow_style=False, sort_keys=True)
        except Exception as e:
            console.print(f"[red]Error saving group catalog: {e}[/red]")
    
    def list_groups(self) -> Dict[str, GroupConfig]:
        """List all available groups in the catalog."""
        return self._load_catalog()
    
    def get_group(self, name: str) -> Optional[GroupConfig]:
        """Get a specific group by name."""
        catalog = self._load_catalog()
        return catalog.get(name)
    
    def add_group(self, group: GroupConfig) -> None:
        """Add a new group to the catalog."""
        catalog = self._load_catalog()
        catalog[group.name] = group
        self._save_catalog(catalog)
        console.print(f"[green]✅ Added group '{group.name}' to catalog[/green]")
    
    def update_group(self, group: GroupConfig) -> None:
        """Update an existing group in the catalog."""
        catalog = self._load_catalog()
        if group.name not in catalog:
            console.print(f"[red]Group '{group.name}' not found in catalog[/red]")
            return
        
        catalog[group.name] = group
        self._save_catalog(catalog)
        console.print(f"[green]✅ Updated group '{group.name}' in catalog[/green]")
    
    def remove_group(self, name: str) -> bool:
        """Remove a group from the catalog."""
        catalog = self._load_catalog()
        if name not in catalog:
            console.print(f"[red]Group '{name}' not found in catalog[/red]")
            return False
        
        del catalog[name]
        self._save_catalog(catalog)
        console.print(f"[green]✅ Removed group '{name}' from catalog[/green]")
        return True
    
    def get_catalog_info(self) -> Dict[str, str]:
        """Get information about the catalog location and source."""
        return {
            "path": str(self.catalog_path),
            "source": self.catalog_source,
            "exists": str(self.catalog_path.exists()),
            "writable": str(self.catalog_path.parent.exists() and self.catalog_path.parent.stat().st_mode & 0o200)
        }
    
    def show_catalog(self) -> None:
        """Display the group catalog in a formatted table."""
        catalog = self._load_catalog()
        
        # Show catalog source information
        info = self.get_catalog_info()
        title_suffix = f" ({info['source']} catalog)"
        console.print(f"[dim]📁 Using catalog: {info['path']}[/dim]")
        
        if not catalog:
            console.print("[yellow]No groups found in catalog[/yellow]")
            return
        
        table = Table(title=f"Available Groups{title_suffix}")
        table.add_column("Name", style="cyan")
        table.add_column("Description", style="green")
        table.add_column("Owner", style="yellow")
        table.add_column("Email", style="blue")
        table.add_column("Team", style="magenta")
        
        for name, group in sorted(catalog.items()):
            table.add_row(
                group.name,
                group.description,
                group.owner_name,
                group.owner_email,
                group.team or ""
            )
        
        console.print(table)
    
    def select_group_interactive(self) -> Optional[GroupConfig]:
        """Interactive group selection from catalog."""
        catalog = self._load_catalog()
        
        if not catalog:
            console.print("[yellow]No groups found in catalog[/yellow]")
            return None
        
        console.print("[bold blue]Available groups:[/bold blue]")
        
        # Display groups with numbers
        groups_list = list(catalog.values())
        for i, group in enumerate(groups_list, 1):
            console.print(f"{i}. [cyan]{group.name}[/cyan] - {group.description}")
        
        console.print(f"{len(groups_list) + 1}. [yellow]Create new group[/yellow]")
        
        while True:
            try:
                choice = Prompt.ask(
                    f"Select a group (1-{len(groups_list) + 1})", 
                    default="1"
                )
                choice_num = int(choice)
                
                if 1 <= choice_num <= len(groups_list):
                    selected_group = groups_list[choice_num - 1]
                    console.print(f"[green]Selected group: {selected_group.name}[/green]")
                    return selected_group
                elif choice_num == len(groups_list) + 1:
                    return None  # User wants to create new group
                else:
                    console.print(f"[red]Please enter a number between 1 and {len(groups_list) + 1}[/red]")
            except ValueError:
                console.print("[red]Please enter a valid number[/red]")


def collect_group_info_interactive() -> GroupConfig:
    """Collect group information interactively from user."""
    console.print("[bold cyan]Creating new group configuration[/bold cyan]")
    
    name = Prompt.ask("[bold cyan]Group name[/bold cyan]")
    description = Prompt.ask("[bold cyan]Group description[/bold cyan]")
    owner_name = Prompt.ask("[bold cyan]Owner name[/bold cyan]")
    owner_email = Prompt.ask("[bold cyan]Owner email[/bold cyan]")
    team = Prompt.ask("[bold cyan]Team (optional)[/bold cyan]", default="")
    contact = Prompt.ask("[bold cyan]Contact (optional)[/bold cyan]", default="")
    
    return GroupConfig(
        name=name,
        description=description,
        owner_name=owner_name,
        owner_email=owner_email,
        team=team if team else None,
        contact=contact if contact else None
    )


def get_group_config(
    group_name: Optional[str] = None,
    group_description: Optional[str] = None,
    group_owner: Optional[str] = None,
    group_email: Optional[str] = None,
    group_team: Optional[str] = None,
    group_contact: Optional[str] = None,
    group_from_catalog: Optional[str] = None,
    interactive: bool = True
) -> GroupConfig:
    """
    Get group configuration from various sources.
    
    Priority: CLI args > catalog selection > interactive prompts
    """
    catalog = GroupCatalog()
    
    # If all required CLI args provided, use them
    if group_name and group_description and group_owner and group_email:
        group = GroupConfig(
            name=group_name,
            description=group_description,
            owner_name=group_owner,
            owner_email=group_email,
            team=group_team,
            contact=group_contact
        )
        # Auto-save new group to catalog
        catalog.add_group(group)
        return group
    
    # If specific catalog group requested, use it
    if group_from_catalog:
        catalog_group = catalog.get_group(group_from_catalog)
        if catalog_group:
            console.print(f"[green]Using group '{group_from_catalog}' from catalog[/green]")
            return catalog_group
        else:
            console.print(f"[red]Group '{group_from_catalog}' not found in catalog[/red]")
    
    # Interactive mode
    if interactive:
        selected_group = catalog.select_group_interactive()
        if selected_group:
            return selected_group
        
        # User wants to create new group
        new_group = collect_group_info_interactive()
        # Auto-save to catalog
        catalog.add_group(new_group)
        return new_group
    
    # Default fallback
    console.print("[yellow]Using default data platform group[/yellow]")
    return GroupConfig(
        name="data_platform",
        description="Data platform infrastructure and utilities",
        owner_name="Data Platform Team",
        owner_email="data-platform@octopusenergy.com",
        team="Data Platform"
    )
