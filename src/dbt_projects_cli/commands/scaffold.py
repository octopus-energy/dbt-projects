"""
Command module for scaffolding new dbt projects and packages.
Enforces data mesh principles with domain-aligned packages.
"""

from pathlib import Path
from typing import Any, Dict, Optional

import click
from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table

from dbt_projects_cli.core.team_catalog import TeamCatalog, get_team_config
from dbt_projects_cli.core.template_engine import create_template_engine

console = Console()

# Data mesh domain patterns
DATA_MESH_ALIGNMENTS = {
    "source-aligned": {
        "description": (
            "Data products that extract and model data from specific source systems"
        ),
        "path_pattern": "packages/domains/source-aligned/{source_system}/{domain_name}",
        "naming_pattern": "{source_system}_{domain_name}",
        "examples": [
            "databricks_systems_data",
            "salesforce_customer_data",
            "stripe_payment_data",
        ],
    },
    "consumer-aligned": {
        "description": (
            "Data products that serve specific business use cases or consumer needs"
        ),
        "path_pattern": "packages/domains/consumer-aligned/{business_area}/{use_case}",
        "naming_pattern": "{business_area}_{use_case}",
        "examples": [
            "marketing_customer_analytics",
            "finance_revenue_reporting",
            "operations_supply_chain",
        ],
    },
    "utils": {
        "description": "Utility packages providing shared functionality across domains",
        "path_pattern": "packages/utils/{utility_type}",
        "naming_pattern": "utils_{utility_type}",
        "examples": ["utils_core", "utils_pii", "utils_testing"],
    },
}

# Known source systems
SOURCE_SYSTEMS = [
    "databricks",
    "salesforce",
    "stripe",
    "hubspot",
    "zendesk",
    "postgresql",
    "mongodb",
    "kafka",
    "elementary",
    "google_analytics",
    "facebook_ads",
    "google_ads",
    "intercom",
    "segment",
]

# Known business areas
BUSINESS_AREAS = [
    "marketing",
    "finance",
    "operations",
    "customer_success",
    "product",
    "engineering",
    "hr",
    "legal",
    "compliance",
]


@click.group()
def scaffold() -> None:
    """Commands for scaffolding new dbt projects and packages."""
    pass


@scaffold.command()
@click.option("--name", "-n", help="Name of the package")
@click.option("--description", "-d", help="Description of the package")
@click.option("--profile", "-p", help="dbt profile name (defaults to package name)")
def package(
    name: Optional[str], description: Optional[str], profile: Optional[str]
) -> None:
    """Create a new dbt package in the packages/ directory."""

    if not name:
        name = Prompt.ask("[bold cyan]Package name[/bold cyan]")

    if not description:
        description = Prompt.ask(
            "[bold cyan]Package description[/bold cyan]", default=""
        )

    if not profile:
        profile = name

    package_path = Path("packages") / name

    if package_path.exists():
        console.print(f"[red]Package '{name}' already exists at {package_path}[/red]")
        return

    console.print(
        f"[bold green]Creating package '{name}' at {package_path}[/bold green]"
    )

    # Create directory structure
    _create_package_structure(package_path, name, description, profile)

    console.print(f"[bold green]✅ Package '{name}' created successfully![/bold green]")
    console.print("[yellow]Next steps:[/yellow]")
    console.print(f"  1. cd {package_path}")
    console.print("  2. Configure your profile in profiles.yml")
    console.print("  3. Start building your models in models/")


@scaffold.command()
def info() -> None:
    """Show information about data mesh patterns and domain alignment."""

    console.print("[bold blue]📊 Data Mesh Domain Patterns[/bold blue]\n")

    for alignment, config in DATA_MESH_ALIGNMENTS.items():
        table = Table(title=f"{alignment.title().replace('-', ' ')} Domains")
        table.add_column("Pattern", style="cyan")
        table.add_column("Description", style="green")
        table.add_column("Examples", style="yellow")

        # Cast to str to satisfy Rich Table typing
        table.add_row(
            str(config["path_pattern"]),
            str(f"{config['description']} ({alignment})"),
            str(", ".join(config["examples"][:3]) + "..."),
        )

        console.print(table)
        console.print()


@scaffold.command()
@click.option(
    "--alignment",
    "-a",
    type=click.Choice(["source-aligned", "consumer-aligned", "utils"]),
    help="Data mesh alignment type",
)
@click.option("--domain-name", help="Domain or use case name")
@click.option("--description", "-d", help="Description of the domain package")
@click.option("--source-system", help="Source system for source-aligned packages")
@click.option("--business-area", help="Business area for consumer-aligned packages")
@click.option("--team-name", help="Name of the team that owns this domain")
@click.option("--team-description", help="Description of the team")
@click.option("--team-domains", help="Comma-separated list of domains the team owns")
@click.option("--team-owner", help="Name of the team owner")
@click.option("--team-email", help="Email of the team owner")
@click.option("--team-contact", help="Contact information for the team")
@click.option("--team-from-catalog", help="Use existing team from catalog by name")
def domain(
    alignment: Optional[str],
    domain_name: Optional[str],
    description: Optional[str],
    source_system: Optional[str],
    business_area: Optional[str],
    team_name: Optional[str],
    team_description: Optional[str],
    team_domains: Optional[str],
    team_owner: Optional[str],
    team_email: Optional[str],
    team_contact: Optional[str],
    team_from_catalog: Optional[str],
) -> None:
    """Create a new domain-aligned package following data mesh principles.

    The dbt-cli now supports team-based package creation where each package
    gets its own unique group configuration, but the team information is
    managed through a teams catalog. This ensures domain isolation while
    providing team ownership tracking.

    KEY FEATURES:
    - Team Catalog: Persistent storage of team configurations
    - Unique Groups Per Package: Each package gets its own group (not shared)
    - Team Ownership: Teams are assigned to group meta tags for tracking
    - Multiple Input Methods: CLI args, catalog selection, or interactive prompts

    TEAM PRIORITY SYSTEM:
    1. CLI arguments (highest priority) - All required fields provided
    2. Catalog selection - Use --team-from-catalog <team_name>
    3. Interactive prompts - Default behavior with team selection menu

    AVAILABLE CLI TEAM OPTIONS:
    --team-name               Name of the team that owns this domain
    --team-description        Description of the team
    --team-domains            Comma-separated list of domains the team owns
    --team-owner              Name of the team owner
    --team-email              Email of the team owner
    --team-contact            Contact information for the team (optional)
    --team-from-catalog       Use existing team from catalog by name

    Examples:
        # Use existing team from catalog
        dbt-cli scaffold domain --alignment source-aligned \
            --source-system databricks --domain-name systems-data \
            --team-from-catalog data_platform

        # Create custom team via CLI args
        dbt-cli scaffold domain --alignment consumer-aligned \
            --business-area marketing --domain-name customer-analytics \
            --team-name marketing \
            --team-description "Marketing analytics team" \
            --team-domains "marketing,campaigns" \
            --team-owner "Marketing Team" \
            --team-email "marketing@octopusenergy.com" \
            --team-contact "Marketing Team Lead"

        # Interactive team selection (default behavior)
        dbt-cli scaffold domain --alignment utils --domain-name core
        # Will show available teams and "Create new team" option

    View catalog:
        dbt-cli scaffold teams          # Show detailed team table
        dbt-cli scaffold teams --help   # Show comprehensive help
    """

    # Show guidance if no alignment specified
    if not alignment:
        console.print("[bold blue]📊 Choose your domain alignment:[/bold blue]\n")

        for align_type, config in DATA_MESH_ALIGNMENTS.items():
            console.print(
                f"[bold cyan]{align_type}[/bold cyan]: {config['description']}"
            )
            console.print(f"  Example: {config['examples'][0]}\n")

        alignment = Prompt.ask(
            "[bold cyan]Select alignment type[/bold cyan]",
            choices=list(DATA_MESH_ALIGNMENTS.keys()),
            default="source-aligned",
        )

    # Validate and collect required parameters based on alignment
    if not domain_name:
        domain_name = Prompt.ask(
            "[bold cyan]Domain name[/bold cyan]", default="my_domain"
        )

    # Handle alignment-specific path generation
    if alignment == "source-aligned":
        if not source_system:
            source_system = Prompt.ask(
                "[bold cyan]Source system[/bold cyan]", default="databricks"
            )
        package_name = f"{source_system}_{domain_name}".replace("-", "_")
        package_path = (
            Path(f"packages/domains/{alignment}") / source_system / domain_name
        )
    elif alignment == "consumer-aligned":
        if not business_area:
            business_area = Prompt.ask(
                "[bold cyan]Business area[/bold cyan]", default="analytics"
            )
        package_name = f"{business_area}_{domain_name}".replace("-", "_")
        package_path = (
            Path(f"packages/domains/{alignment}") / business_area / domain_name
        )
    else:  # utils
        package_name = f"utils_{domain_name}".replace("-", "_")
        package_path = Path("packages/utils") / domain_name

    if not description:
        description = Prompt.ask(
            "[bold cyan]Package description[/bold cyan]",
            default=f"Data domain package for {alignment} alignment",
        )

    # Check if package already exists
    if package_path.exists():
        console.print(f"[red]Domain package already exists at {package_path}[/red]")
        raise click.ClickException(f"Domain package already exists at {package_path}")

    # Check if package already exists
    if package_path.exists():
        console.print(f"[red]Domain package already exists at {package_path}[/red]")
        raise click.ClickException(f"Domain package already exists at {package_path}")

    console.print(
        f"[bold green]Creating {alignment} domain package '{package_name}' "
        f"at {package_path}[/bold green]"
    )

    # Get team configuration from various sources
    team_config = get_team_config(
        team_name=team_name,
        team_description=team_description,
        team_domains=team_domains.split(",") if team_domains else None,
        team_owner=team_owner,
        team_email=team_email,
        team_contact=team_contact,
        team_from_catalog=team_from_catalog,
        interactive=True,
    )

    # Generate group name based on alignment and team configuration
    if alignment == "utils":
        group_name = "data_platform"  # Utils packages use data_platform group
    else:
        group_name = package_name  # Other packages use unique group per package

    # Create the domain package structure with full context for templates
    template_context = {
        "package_name": package_name,
        "alignment": alignment,
        "domain_name": domain_name,
        "description": description or f"Data domain package for {alignment} alignment",
        "group_name": group_name,  # Unique group per package
        "group_description": f"{alignment} domain for {package_name}",
        "owner_name": team_config.owner_name,
        "owner_email": team_config.owner_email,
        "team_name": team_config.name,
        "team_description": team_config.description,
        "team_contact": team_config.contact,
        "team_domains": team_config.domains,
    }

    # Add alignment-specific context
    if alignment == "source-aligned":
        template_context["source_system"] = source_system or "unknown"
    elif alignment == "consumer-aligned":
        template_context["business_area"] = business_area or "analytics"

    _create_domain_structure(
        package_path,
        package_name,
        description,
        alignment,
        template_context,
    )

    console.print(
        f"[bold green]✅ Domain package '{package_name}' "
        f"created successfully![/bold green]"
    )
    console.print("[yellow]Next steps:[/yellow]")
    console.print(f"  1. cd {package_path}")
    console.print("  2. Review and customize the generated configuration")
    console.print("  3. Start building your domain models in models/")
    console.print(
        f"[dim]💡 This package follows {alignment} data mesh principles[/dim]"
    )


@scaffold.command()
@click.option("--fabric", "-f", help="Fabric name (e.g., oeuk-data-prod)")
@click.option("--project", "-p", help="Project name (e.g., analytics)")
@click.option("--component", "-c", help="Component name (e.g., core, ml, reporting)")
@click.option("--catalog", help="Databricks catalog name")
@click.option("--workspace-id", help="Databricks workspace ID")
def fabric(
    fabric: Optional[str],
    project: Optional[str],
    component: Optional[str],
    catalog: Optional[str],
    workspace_id: Optional[str],
) -> None:
    """Create a new fabric project in the fabrics/ directory."""

    if not fabric:
        fabric = Prompt.ask(
            "[bold cyan]Fabric name[/bold cyan]", default="oeuk-data-prod"
        )

    if not project:
        project = Prompt.ask("[bold cyan]Project name[/bold cyan]", default="analytics")

    if not component:
        component = Prompt.ask("[bold cyan]Component name[/bold cyan]", default="core")

    if not catalog:
        catalog = Prompt.ask(
            "[bold cyan]Databricks catalog[/bold cyan]",
            default="octoenergy_data_prod_prod",
        )

    if not workspace_id:
        workspace_id = Prompt.ask(
            "[bold cyan]Databricks workspace ID[/bold cyan]", default="337210421809158"
        )

    fabric_path = Path("fabrics") / fabric / "projects" / project / component

    if fabric_path.exists():
        console.print(f"[red]Fabric project already exists at {fabric_path}[/red]")
        return

    console.print(f"[bold green]Creating fabric project at {fabric_path}[/bold green]")

    # Create directory structure
    _create_fabric_structure(
        fabric_path, fabric, project, component, catalog, workspace_id
    )

    console.print("[bold green]✅ Fabric project created successfully![/bold green]")
    console.print("[yellow]Next steps:[/yellow]")
    console.print(f"  1. cd {fabric_path}")
    console.print("  2. Configure your Databricks connection")
    console.print("  3. Start building your models in models/")


@scaffold.command()
def teams() -> None:
    """View and manage the teams catalog for domain ownership tracking.

    The teams catalog stores reusable team configurations used when
    scaffolding domain packages. Each package gets a unique group, but
    team information is tracked via meta tags for organizational clarity.

    PURPOSE:
    - Consistent Team Configurations: Standardized across all scaffolded packages
    - Team Collaboration: Easy sharing through version control
    - Reduced Manual Work: No need to edit hardcoded content post-scaffolding
    - Organizational Standards: Enforced naming conventions and contacts

    USAGE:
    - Automatically used when scaffolding new domain packages.
    - Interactive selection: 'dbt-cli scaffold domain --alignment utils --domain core'
    - Team from catalog: 'dbt-cli scaffold domain --team-from-catalog data_platform'

    CATALOG LOCATION PRIORITY:
    1. `teams-catalog.yml` (project level)
    2. `.dbt/teams.yml` (project .dbt folder)
    3. `~/dbt-teams.yml` (user level - visible)
    4. `~/.dbt-cli/teams.yml` (user level - hidden)

    TEAM SCHEMA:
    - name: Unique identifier (required)
    - description: Team description (required)
    - owner_name: Team or owner name (required)
    - owner_email: Contact email (required)
    - domains: List of owned domains (required)
    - contact: Specific contact person (optional)

    BENEFITS:
    1. **Version Control**
    2. **Collaboration**
    3. **Visibility**
    4. **Consistency**
    5. **Maintenance**

    GENERATED OUTPUT:
    - Teams generate `groups/_group.yml` files for consistent configurations

    Examples:
        dbt-cli scaffold teams                    # Show all teams
        dbt-cli scaffold domain --alignment source-aligned --domain-name my-domain
        dbt-cli scaffold domain --team-from-catalog data_platform
    """
    # Show the catalog table
    catalog = TeamCatalog()
    catalog.show_catalog()


def _create_package_structure(
    package_path: Path, name: str, description: str, profile: str
) -> None:
    """Create the directory structure for a package."""
    package_path.mkdir(parents=True, exist_ok=True)

    # Create directories
    directories = ["models/staging", "models/marts", "macros", "tests", "seeds"]

    for directory in directories:
        (package_path / directory).mkdir(parents=True, exist_ok=True)

    # Create dbt_project.yml
    dbt_project_content = f"""name: {name}

profile: {profile}

seed-paths: ["seeds"]
model-paths: ["models"]
macro-paths: ["macros"]
clean-targets:
  - "target"
  - "dbt_packages"

models:
  {name}:
    # Materialize staging models as views, and marts as tables
    staging:
      +materialized: view
    marts:
      +materialized: table
"""

    (package_path / "dbt_project.yml").write_text(dbt_project_content)

    # Create packages.yml
    packages_content = """packages:
  - package: dbt-labs/dbt_utils
    version: ">=1.3.0"
"""
    (package_path / "packages.yml").write_text(packages_content)

    # Create README.md
    readme_content = f"""# {name}

{description}

## Overview

This dbt package contains models and macros for {name}.

## Getting Started

1. Install dependencies:
   ```bash
   dbt deps
   ```

2. Run the models:
   ```bash
   dbt run
   ```

## Structure

- `models/staging/` - Staging models (views)
- `models/marts/` - Mart models (tables)
- `macros/` - Reusable macros
- `tests/` - Custom tests
- `seeds/` - Seed data files

## Dependencies

- dbt-utils
"""
    (package_path / "README.md").write_text(readme_content)

    # Create example staging model
    staging_example = f"""{{{{ config(materialized='view') }}}}

select
    1 as id,
    'example' as name,
    current_timestamp() as created_at

-- This is an example staging model for {name}
-- Replace this with your actual data transformation logic
"""
    (package_path / "models/staging/stg_example.sql").write_text(staging_example)

    # Create .user.yml placeholder
    (package_path / ".user.yml").write_text(
        "# Add your user-specific configurations here\n"
    )


def _create_fabric_structure(
    fabric_path: Path,
    fabric: str,
    project: str,
    component: str,
    catalog: str,
    workspace_id: str,
) -> None:
    """Create the directory structure for a fabric project."""
    fabric_path.mkdir(parents=True, exist_ok=True)

    # Create directories
    directories = [
        "models/staging",
        "macros",
        "tests",
        "groups",
        "seeds",
        "snapshots",
        "analyses",
    ]

    for directory in directories:
        (fabric_path / directory).mkdir(parents=True, exist_ok=True)

    project_name = f"{fabric.replace('-', '_')}_{project}_{component}"

    # Create dbt_project.yml
    dbt_project_content = f"""name: '{project_name}'
version: '1.0.0'
config-version: 2

# This setting configures which "profile" from the profiles.yml dbt uses for
# this project.
profile: '{project_name}'

# These configurations specify where dbt should look for different types of files.
model-paths:
  - models
  # Although the groups directory doesn't contain any models its required
  # for dbt to find the yml file for groups
  - groups
analysis-paths:
  - analyses
test-paths:
  - tests
seed-paths:
  - seeds
macro-paths:
  - macros
snapshot-paths:
  - snapshots

clean-targets:         # directories to be removed by `dbt clean`
  - dbt_packages
  - state
  - target

# Configuring models
models:
  +persist_docs:
    relation: true
    columns: true
  {project_name}:
    +schema: {project}_{component}
    +materialized: table
    staging:
      +materialized: view

vars:
  PROD_CATALOG: "{catalog}"
  DATABRICKS_WORKSPACE_ID: "{workspace_id}"
  # Elementary Variables
  disable_dbt_artifacts_autoupload: "{{{{ target.name != 'prod' }}}}"
  disable_dbt_columns_autoupload: "{{{{ target.name != 'prod' }}}}"
  disable_run_results: "{{{{ target.name != 'prod' }}}}"
  disable_tests_results: "{{{{ target.name != 'prod' }}}}"
  disable_dbt_invocation_autoupload: "{{{{ target.name != 'prod' }}}}"
"""

    (fabric_path / "dbt_project.yml").write_text(dbt_project_content)

    # Create packages.yml
    packages_content = """packages:
  - package: dbt-labs/dbt_utils
    version: ">=1.3.0"
  - package: elementary-data/elementary
    version: ">=0.15.0"
"""
    (fabric_path / "packages.yml").write_text(packages_content)

    # Create profiles.yml template
    profiles_content = f"""# Example profiles.yml configuration
# Copy this to your ~/.dbt/profiles.yml and customize

{project_name}:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: {catalog}
      schema: dev_{project}_{component}
      host: your-workspace.cloud.databricks.com
      http_path: /sql/1.0/warehouses/your-warehouse-id
      token: {{{{ env_var('DATABRICKS_TOKEN') }}}}
    prod:
      type: databricks
      catalog: {catalog}
      schema: {project}_{component}
      host: your-workspace.cloud.databricks.com
      http_path: /sql/1.0/warehouses/your-warehouse-id
      token: {{{{ env_var('DATABRICKS_TOKEN') }}}}
"""
    (fabric_path / "profiles.yml").write_text(profiles_content)

    # Create requirements.txt
    (fabric_path / "requirements.txt").write_text("dbt-databricks>=1.7.0\n")

    # Create README.md
    readme_content = f"""# {fabric} - {project} - {component}

This is a dbt project for the {component} component of {project} in the {fabric} fabric.

## Setup

1. Install dependencies:
   ```bash
   dbt deps
   ```

2. Configure your profile in `~/.dbt/profiles.yml` (see `profiles.yml` for template)

3. Set environment variables:
   ```bash
   export DATABRICKS_TOKEN=your_token_here
   ```

4. Run the project:
   ```bash
   dbt run
   ```

## Structure

- `models/` - Data transformation models
- `macros/` - Reusable SQL macros
- `tests/` - Custom data tests
- `groups/` - dbt groups configuration
- `seeds/` - Static data files
- `snapshots/` - SCD Type 2 snapshots
- `analyses/` - Analytical queries

## Configuration

- **Catalog**: {catalog}
- **Workspace ID**: {workspace_id}
- **Schema**: {project}_{component}
"""
    (fabric_path / "README.md").write_text(readme_content)

    # Create example group file
    groups_content = f"""version: 2

groups:
  - name: {component}
    owner:
      name: Data Team
      email: data@octopusenergy.com
"""
    (fabric_path / "groups/_group.yml").write_text(groups_content)

    # Create .user.yml placeholder
    (fabric_path / ".user.yml").write_text(
        "# Add your user-specific configurations here\n"
    )


def _create_domain_structure(
    package_path: Path,
    package_name: str,
    description: str,
    alignment: str,
    context: Dict[str, Any],
) -> None:
    """Create the directory structure for a domain-aligned package."""

    # Initialize template engine
    engine = create_template_engine()

    # Validate context
    errors = engine.validate_context(context)
    if errors:
        for error in errors:
            console.print(f"[red]Error: {error}[/red]")
        return

    # Create directories
    package_path.mkdir(parents=True, exist_ok=True)
    directories = engine.get_directory_structure(alignment)
    for directory in directories:
        (package_path / directory).mkdir(parents=True, exist_ok=True)

    # Generate configuration files using templates
    dbt_project_content = engine.generate_dbt_project_yml(context)
    (package_path / "dbt_project.yml").write_text(dbt_project_content)

    packages_content = engine.generate_packages_yml(context)
    (package_path / "packages.yml").write_text(packages_content)

    groups_content = engine.generate_group_yml(context)
    (package_path / "groups/_group.yml").write_text(groups_content)

    # Create README.md using template
    readme_content = _generate_domain_readme(
        package_name, description, alignment, context
    )
    (package_path / "README.md").write_text(readme_content)

    # Create example models
    _create_domain_example_models(package_path, package_name, alignment, context)

    # Additional placeholders or configurations can be added as needed.
    (package_path / ".user.yml").write_text(
        "# Add your user-specific configurations here\n"
    )

    console.print(
        f"[bold green]✅ Domain structure for '{package_name}' "
        f"created successfully![/bold green]"
    )


def _generate_domain_dbt_project(
    package_name: str, alignment: str, context: Dict[str, Any]
) -> str:
    """Generate dbt_project.yml content for domain packages."""

    base_config = f"""name: '{package_name}'
version: '1.0.0'
config-version: 2

# This setting configures which "profile" from the profiles.yml dbt uses for
# this project.
profile: '{package_name}'

# These configurations specify where dbt should look for different types of files.
model-paths:
  - models
  # Although the groups directory doesn't contain any models its required
  # for dbt to find the yml file for groups
  - groups
analysis-paths:
  - analyses
test-paths:
  - tests
seed-paths:
  - seeds
macro-paths:
  - macros
snapshot-paths:
  - snapshots

clean-targets:         # directories to be removed by `dbt clean`
  - dbt_packages
  - state
  - target

# Configuring models
models:
  +persist_docs:
    relation: true
    columns: true"""

    # Add alignment-specific model configuration
    if alignment == "source-aligned":
        schema_name = context.get("source_system", "unknown")
        model_config = f"""
  {package_name}:
    +group: {context.get('source_system', 'data_platform')}
    +schema: {schema_name}
    +materialized: table
    +access: public  # Source-aligned domains typically expose data publicly
    staging:
      +materialized: view
      +access: private  # Staging models are private implementation details"""

    elif alignment == "consumer-aligned":
        schema_name = (
            f"{context.get('business_area', 'analytics')}_"
            f"{context.get('domain_name', 'data').replace('-', '_')}"
        )
        model_config = f"""
  {package_name}:
    +group: {context.get('business_area', 'analytics')}
    +schema: {schema_name}
    +materialized: table
    +access: public  # Consumer-aligned domains serve specific use cases
    staging:
      +materialized: view
      +access: private
    marts:
      +materialized: table
      +access: public  # Marts are the main interface for consumers"""

    elif alignment == "utils":
        model_config = f"""
  {package_name}:
    +group: data_platform
    +materialized: table
    +access: public  # Utility functions should be accessible across domains
    staging:
      +materialized: view
    macro_tests:
      +materialized: ephemeral"""

    # Add standard variables
    vars_section = """
vars:
  PROD_CATALOG: "octoenergy_data_prod_prod"
  DATABRICKS_WORKSPACE_ID: "337210421809158"
  # Elementary Variables
  disable_dbt_artifacts_autoupload: "{{ target.name != 'prod' }}"
  disable_dbt_columns_autoupload: "{{ target.name != 'prod' }}"
  disable_run_results: "{{ target.name != 'prod' }}"
  disable_tests_results: "{{ target.name != 'prod' }}"
  disable_dbt_invocation_autoupload: "{{ target.name != 'prod' }}"
  distant_future_timestamp: "to_utc_timestamp('2099-12-31 23:59:59','UTC')"
  local_timezone: "UTC"""

    return base_config + model_config + vars_section


def _generate_domain_packages(alignment: str) -> str:
    """Generate packages.yml content for domain packages."""

    base_packages = """packages:
  - package: dbt-labs/dbt_utils
    version: ">=1.3.0"
  - package: elementary-data/elementary
    version: ">=0.15.0"""

    # Add alignment-specific packages
    if alignment == "source-aligned":
        # Source-aligned packages might need additional extraction tools
        additional = """
  # Add source-specific packages as needed
  # - package: dbt-labs/dbt_external_tables
  #   version: ">=0.8.0"""

    elif alignment == "consumer-aligned":
        # Consumer-aligned packages might need analytics tools
        additional = """
  # Add analytics-specific packages as needed
  # - package: dbt-labs/metrics
  #   version: ">=1.6.0"""

    elif alignment == "utils":
        # Utils packages might need more development tools
        additional = """
  # Add utility-specific packages as needed
  # - package: dbt-labs/codegen
  #   version: ">=0.12.1"""

    return base_packages + additional


def _generate_domain_readme(
    package_name: str, description: str, alignment: str, context: Dict[str, Any]
) -> str:
    """Generate README.md content for domain packages."""

    # Alignment-specific content
    alignment_info = {
        "source-aligned": {
            "purpose": (
                f"This is a **source-aligned** data product that extracts "
                f"and models data from the "
                f"{context.get('source_system', 'unknown')} system."
            ),
            "principles": """### Data Mesh Principles

- **Domain Ownership**: This package is owned by the team responsible for the
  {source_system} system
- **Data as a Product**: Provides clean, documented, and reliable data products
  from {source_system}
- **Self-Serve Infrastructure**: Can be independently deployed and maintained
- **Federated Governance**: Follows Octopus Energy's data standards and
  governance""".format(
                source_system=context.get("source_system", "source system")
            ),
        },
        "consumer-aligned": {
            "purpose": (
                f"This is a **consumer-aligned** data product that serves "
                f"the {context.get('business_area', 'business')} team's "
                f"{context.get('domain_name', 'analytics')} use case."
            ),
            "principles": """### Data Mesh Principles

- **Domain Ownership**: This package is owned by the {business_area} team
- **Data as a Product**: Provides business-specific analytics and insights
- **Self-Serve Infrastructure**: Can be independently deployed and maintained
- **Federated Governance**: Follows Octopus Energy's data standards and
  governance""".format(
                business_area=context.get("business_area", "business")
            ),
        },
        "utils": {
            "purpose": (
                "This is a **utility package** that provides shared "
                "functionality across multiple data domains."
            ),
            "principles": """### Data Mesh Principles

- **Domain Ownership**: This package is owned by the Data Platform team
- **Shared Infrastructure**: Provides common utilities and standards
- **Self-Serve Infrastructure**: Enables other domains to be self-sufficient
- **Federated Governance**: Enforces data standards and best practices""",
        },
    }

    info = alignment_info.get(alignment, alignment_info["utils"])

    return f"""# {package_name}

{description}

## Overview

{info['purpose']}

{info['principles']}

## Getting Started

1. Install dependencies:
   ```bash
   dbt deps
   ```

2. Run the models:
   ```bash
   dbt run
   ```

3. Test the models:
   ```bash
   dbt test
   ```

## Structure

- `models/staging/` - Staging models (views, private access)
- `models/marts/` - Mart models (tables, public access) *(consumer-aligned only)*
- `macros/` - Reusable SQL macros
- `tests/` - Custom data tests
- `groups/` - Domain ownership configuration
- `seeds/` - Static reference data
- `snapshots/` - Slowly changing dimensions
- `analyses/` - Ad-hoc analytical queries

## Data Contracts

### Public Models

This domain exposes the following public data products:

*(Document your public models and their contracts here)*

### Dependencies

This domain depends on:

*(Document your upstream dependencies here)*

## Ownership

- **Domain**: {alignment.replace('-', ' ').title()}
- **Owner**: Data Team
- **Contact**: data@octopusenergy.com

## Development

### Local Development

1. Set up your profiles.yml (see `profiles.yml` template)
2. Install dependencies: `dbt deps`
3. Run in development: `dbt run --target dev`
4. Test your changes: `dbt test --target dev`

### Deployment

This package is deployed as part of the Octopus Energy data mesh infrastructure.
"""


def _generate_domain_groups(
    package_name: str, alignment: str, context: Dict[str, Any]
) -> str:
    """Generate groups.yml content for domain packages."""

    if alignment == "source-aligned":
        group_name = context.get("source_system", "data_platform")
        owner_name = f"{context.get('source_system', 'Data').title()} Data Team"
    elif alignment == "consumer-aligned":
        group_name = context.get("business_area", "analytics")
        owner_name = f"{context.get('business_area', 'Business').title()} Team"
    elif alignment == "utils":
        group_name = "data_platform"
        owner_name = "Data Platform Team"

    return f"""version: 2

groups:
  - name: {group_name}
    owner:
      name: {owner_name}
      email: data@octopusenergy.com
    description: |
      {alignment.replace('-', ' ').title()} domain group for {package_name}
"""


def _generate_domain_profiles(
    package_name: str, alignment: str, context: Dict[str, Any]
) -> str:
    """Generate profiles.yml template for domain packages."""

    if alignment == "source-aligned":
        schema_prefix = context.get("source_system", "unknown")
    elif alignment == "consumer-aligned":
        schema_prefix = (
            f"{context.get('business_area', 'analytics')}_"
            f"{context.get('domain_name', 'data').replace('-', '_')}"
        )
    elif alignment == "utils":
        schema_prefix = "utils"

    return f"""# Example profiles.yml configuration for {package_name}
# Copy this to your ~/.dbt/profiles.yml and customize

{package_name}:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: octoenergy_data_prod_prod
      schema: dev_{schema_prefix}
      host: your-workspace.cloud.databricks.com
      http_path: /sql/1.0/warehouses/your-warehouse-id
      token: {{{{ env_var('DATABRICKS_TOKEN') }}}}
    prod:
      type: databricks
      catalog: octoenergy_data_prod_prod
      schema: {schema_prefix}
      host: your-workspace.cloud.databricks.com
      http_path: /sql/1.0/warehouses/your-warehouse-id
      token: {{{{ env_var('DATABRICKS_TOKEN') }}}}
"""


def _create_domain_example_models(
    package_path: Path,
    package_name: str,
    alignment: str,
    context: Dict[str, Any],
) -> None:
    """Create example models based on domain alignment."""

    if alignment == "source-aligned":
        # Create a staging model that extracts from source
        staging_model = f"""{{{{ config(
    materialized='view',
    group='{context.get('source_system', 'data_platform')}',
    access='private'
) }}}}

with source_data as (
    -- Replace this with actual source data extraction
    select
        'example_id' as id,
        'example_data' as data_field,
        current_timestamp() as extracted_at,
        current_timestamp() as updated_at
)

select * from source_data

-- This is an example staging model for {package_name}
-- It should extract and lightly transform data from \
--   {context.get('source_system', 'the source system')}
-- Following source-aligned data mesh principles
"""
        (package_path / "models/staging/stg_example_data.sql").write_text(staging_model)

    elif alignment == "consumer-aligned":
        # Create both staging and mart models
        staging_model = f"""{{{{ config(
    materialized='view',
    group='{context.get('business_area', 'analytics')}',
    access='private'
) }}}}

-- Staging model for {context.get('business_area', 'business')} \
--   {context.get('domain_name', 'analytics')}
select
    'example_id' as id,
    'example_metric' as metric_value,
    current_timestamp() as created_at

-- This model stages data for the \
--   {context.get('domain_name', 'analytics')} use case
"""
        (package_path / "models/staging/stg_example_data.sql").write_text(staging_model)

        # Create mart model
        mart_model = f"""{{{{ config(
    materialized='table',
    group='{context.get('business_area', 'analytics')}',
    access='public'
) }}}}

-- Mart model for {context.get('business_area', 'business')} \
--   {context.get('domain_name', 'analytics')}
with staged_data as (
    select * from {{{{ ref('stg_example_data') }}}}
)

select
    id,
    metric_value,
    created_at,
    -- Add business logic here
    'processed' as status
from staged_data

-- This mart serves the {context.get('domain_name', 'analytics')} \
--   use case for the {context.get('business_area', 'business')} team
"""
        (package_path / "models/marts/mart_example_analytics.sql").write_text(
            mart_model
        )

    elif alignment == "utils":
        # Create utility models/macros
        macro_example = f"""-- Utility macro for {package_name}
{{% macro example_utility_function() %}}
    -- Add your utility logic here
    current_timestamp()
{{% endmacro %}}
"""
        (package_path / "macros/example_utility.sql").write_text(macro_example)
