"""
Command module for managing descriptions in dbt projects.
"""

import os
from pathlib import Path
from typing import Optional

import click
from rich.console import Console

from ..core.model_parser import ModelParser

# Import all required modules at the top level for better testability
from ..core.project_discovery import ProjectDiscovery
from ..integrations.databricks import create_databricks_connector
from ..integrations.llm import (
    LLMDescriptionGenerator,
    LLMProvider,
    ModelContext,
)
from ..security.pii_protection import create_pii_detector
from ..utils.logging import setup_logging

console = Console()


@click.group()
def descriptions() -> None:
    """Commands for managing dbt model descriptions."""
    pass


@descriptions.command()
def models() -> None:
    """Show available LLM models and current configuration."""
    console.print("[bold blue]LLM Configuration[/bold blue]\n")


@descriptions.command()
@click.option(
    "--level",
    type=click.Choice(
        ["strict", "high", "medium", "schema_only"], case_sensitive=False
    ),
    default="high",
    help="PII protection level to test",
)
def test_pii(level: str) -> None:
    """Test PII protection capabilities with sample data."""

    console.print("[bold blue]🔒 PII Protection Test[/bold blue]\n")

    # Create test data with various PII types
    test_data = {
        "source_table": "test.customers",
        "columns": [
            {"name": "customer_id", "type": "bigint"},
            {"name": "first_name", "type": "varchar"},
            {"name": "email_address", "type": "varchar"},
            {"name": "phone_number", "type": "varchar"},
            {"name": "account_balance", "type": "decimal"},
            {"name": "created_date", "type": "timestamp"},
        ],
        "sample_rows": [
            {
                "customer_id": 12345,
                "first_name": "John",
                "email_address": "john.doe@example.com",
                "phone_number": "+1-555-123-4567",
                "account_balance": 1250.75,
                "created_date": "2023-01-15",
            },
            {
                "customer_id": 67890,
                "first_name": "Jane",
                "email_address": "jane.smith@company.org",
                "phone_number": "(555) 987-6543",
                "account_balance": 890.25,
                "created_date": "2023-02-20",
            },
            {
                "customer_id": 11111,
                "first_name": "Bob",
                "email_address": "bob.wilson@test.net",
                "phone_number": "555.456.7890",
                "account_balance": 2100.00,
                "created_date": "2023-03-10",
            },
        ],
    }

    console.print("[bold]Original Data:[/bold]")
    console.print(f"Table: {test_data['source_table']}")
    console.print("Sample rows:")
    for i, row in enumerate(test_data["sample_rows"]):
        console.print(f"  Row {i+1}: {row}")

    console.print(f"\n[bold]Testing PII Protection (level: {level}):[/bold]")

    # Test PII protection
    pii_detector = create_pii_detector(level)
    protected_data = pii_detector.sanitize_sample_data(test_data)

    console.print("\n[bold]Protected Data:[/bold]")
    console.print(f"Table: {protected_data['source_table']}")

    if protected_data.get("sample_rows"):
        console.print("Protected sample rows:")
        for i, row in enumerate(protected_data["sample_rows"]):
            console.print(f"  Row {i+1}: {row}")
    else:
        console.print("Sample rows: [REMOVED - schema_only mode]")

    # Show protection summary
    protection_info = protected_data.get("pii_protection", {})
    console.print("[bold]Protection Summary:[/bold]")
    console.print(f"  Method: {protection_info.get('method', 'unknown')}")
    console.print(f"  Columns analyzed: {protection_info.get('columns_analyzed', 0)}")
    console.print(f"  High-risk columns: {protection_info.get('high_risk_columns', 0)}")
    console.print(
        f"  Medium-risk columns: " f"{protection_info.get('medium_risk_columns', 0)}"
    )
    console.print(f"  Low-risk columns: {protection_info.get('low_risk_columns', 0)}")
    console.print(
        f"  Protection applied: " f"{protection_info.get('protection_applied', False)}"
    )

    console.print("\n[dim]💡 PII protection levels:[/dim]")
    console.print("[dim]  • strict: Excludes all potentially sensitive data[/dim]")
    console.print("[dim]  • high: Hashes high-risk PII (emails, names, phones)[/dim]")
    console.print("[dim]  • medium: Masks PII while preserving some structure[/dim]")
    console.print("[dim]  • schema_only: Only includes column names, no data[/dim]")


@descriptions.command()
@click.option("--profile", help="Specific dbt profile to test")
@click.option("--target", help="Specific dbt target to test")
def test_databricks(profile: Optional[str], target: Optional[str]) -> None:
    """Test Databricks SQL warehouse connection using dbt profiles."""

    console.print("[bold blue]🔌 Testing Databricks Connection[/bold blue]\n")

    # Test the connection using dbt profiles (preferred) or environment
    # variables
    try:
        # For testing, we need to find a dbt project directory
        # Look for dbt_project.yml in current directory or subdirectories
        # Path is already imported at module level

        current_dir = Path.cwd()
        project_path = None

        # Check if current directory is a dbt project
        if (current_dir / "dbt_project.yml").exists():
            project_path = current_dir
        else:
            # Look for dbt projects in subdirectories
            for dbt_project_file in current_dir.rglob("dbt_project.yml"):
                potential_path = dbt_project_file.parent
                profiles_path = potential_path / "profiles.yml"
                if profiles_path.exists():
                    project_path = potential_path
                    console.print(
                        f"[dim]Found dbt project with profiles.yml: "
                        f"{project_path}[/dim]"
                    )
                    break

        if not project_path:
            console.print("[yellow]⚠️  No dbt project with profiles.yml found[/yellow]")
            console.print(
                "[yellow]   Run this command from a dbt project directory "
                "or ensure profiles.yml exists[/yellow]"
            )
            return

        connector = create_databricks_connector(project_path, profile, target)
        if connector:
            console.print("  🔌 Connector created successfully")

            # Test basic connectivity
            if connector.test_connection():
                console.print("  ✅ Connection test passed!")

                # Try to list some tables
                console.print("\n[bold]Available Tables (sample):[/bold]")
                try:
                    tables = connector.list_tables()
                    if tables:
                        for i, table in enumerate(tables[:5]):
                            console.print(f"  📋 {table}")
                        if len(tables) > 5:
                            console.print(f"  ... and {len(tables) - 5} more tables")
                    else:
                        console.print("  (No tables found or access limited)")
                except Exception as e:
                    console.print(f"  ⚠️  Could not list tables: {e}")

                console.print("\n[green]🎉 Databricks connection is working![/green]")
                console.print(
                    "[green]You can now use real sample data in "
                    "description generation.[/green]"
                )
            else:
                console.print("  ❌ Connection test failed")
        else:
            console.print("  ❌ Could not create connector")
            console.print("\n[yellow]💡 Setup Instructions:[/yellow]")
            console.print("\n[bold]Option 1: Use dbt profiles.yml (Recommended)[/bold]")
            console.print(
                "  1. Ensure you have a Databricks profile in " "~/.dbt/profiles.yml"
            )
            console.print("  2. Configure OAuth or token authentication")
            console.print("  3. Example profile:")
            console.print("     ```yaml")
            console.print("     my_profile:")
            console.print("       target: dev")
            console.print("       outputs:")
            console.print("         dev:")
            console.print("           type: databricks")
            console.print("           host: your-workspace.cloud.databricks.com")
            console.print("           http_path: /sql/1.0/warehouses/your-warehouse-id")
            console.print("           auth_type: oauth")
            console.print("           client_id: your-client-id")
            console.print("           client_secret: your-client-secret")
            console.print("           catalog: your-catalog")
            console.print("           schema: your-schema")
            console.print("     ```")

            console.print("\n[bold]Option 2: Environment Variables (Fallback)[/bold]")
            console.print(
                "  export DATABRICKS_SERVER_HOSTNAME="
                "your-workspace.cloud.databricks.com"
            )
            console.print(
                "  export DATABRICKS_HTTP_PATH=" "/sql/1.0/warehouses/your-warehouse-id"
            )
            console.print("  export DATABRICKS_ACCESS_TOKEN=your-access-token")

    except Exception as e:
        console.print(f"  ❌ Connection error: {e}")


@descriptions.command()
def show_models() -> None:
    """Show available LLM models and current configuration."""

    console.print("[bold blue]LLM Configuration[/bold blue]\n")

    # Load and show config file models
    generator = LLMDescriptionGenerator()
    config = generator.config

    if config and "providers" in config:
        console.print("[bold]Models from config file:[/bold]")
        for provider_name, provider_config in config["providers"].items():
            default_model = provider_config.get("default_model", "Not specified")
            console.print(f"  {provider_name}: {default_model}")

            available = provider_config.get("available_models", [])
            if available:
                console.print(f"    Available: {', '.join(available)}")
    else:
        console.print("[bold]Default Models (fallback):[/bold]")
        for provider, model in LLMDescriptionGenerator.DEFAULT_MODELS.items():
            console.print(f"  {provider.value}: {model}")

    console.print("\n[bold]Environment Variables:[/bold]")
    env_vars = {
        "OPENAI_API_KEY": "🔑" if os.getenv("OPENAI_API_KEY") else "❌",
        "ANTHROPIC_API_KEY": "🔑" if os.getenv("ANTHROPIC_API_KEY") else "❌",
        "OPENAI_MODEL": os.getenv("OPENAI_MODEL", "(not set)"),
        "ANTHROPIC_MODEL": os.getenv("ANTHROPIC_MODEL", "(not set)"),
    }

    for var, value in env_vars.items():
        console.print(f"  {var}: {value}")

    console.print("\n[bold]Usage Examples:[/bold]")
    console.print("  # Use default models")
    console.print("  dbt-cli descriptions generate -p my_project")
    console.print("\n  # Override with environment variable")
    console.print("  export OPENAI_MODEL=gpt-4o-mini")
    console.print("  dbt-cli descriptions generate -p my_project")
    console.print("\n  # Override with command line option")
    console.print(
        "  dbt-cli descriptions generate -p my_project " "--llm-model gpt-4o-mini"
    )
    console.print("\n  # Use Anthropic with specific model")
    console.print(
        "  dbt-cli descriptions generate -p my_project --provider anthropic "
        "--llm-model claude-3-5-sonnet-20241022"
    )


@descriptions.command()
@click.option("--project", "-p", required=True, help="Specify the dbt project by name")
@click.option("--model", "-m", help="Specific model name to process (optional)")
@click.option("--expand", is_flag=True, help="Expand existing descriptions if any")
@click.option(
    "--provider",
    "-pr",
    type=click.Choice(["openai", "anthropic"], case_sensitive=False),
    default="openai",
    help="LLM provider to use",
)
@click.option(
    "--llm-model",
    help="Specific LLM model to use (overrides defaults and env vars)",
)
@click.option(
    "--pii-protection",
    type=click.Choice(
        ["strict", "high", "medium", "schema_only"], case_sensitive=False
    ),
    default="high",
    help="Level of PII protection for sample data",
)
@click.option(
    "--dbt-profile",
    help="dbt profile name to use for Databricks connection",
)
@click.option(
    "--dbt-target",
    help="dbt target to use (defaults to profile default)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show generated descriptions without writing to files",
)
@click.option(
    "--interactive",
    "-i",
    is_flag=True,
    help="Ask for confirmation before updating each model",
)
@click.option(
    "--quiet",
    "-q",
    is_flag=True,
    help="Suppress detailed progress messages (only show final results)",
)
@click.pass_context
def generate(
    ctx: click.Context,
    project: str,
    model: Optional[str],
    expand: bool,
    provider: str,
    llm_model: Optional[str],
    pii_protection: str,
    dbt_profile: Optional[str],
    dbt_target: Optional[str],
    dry_run: bool,
    interactive: bool,
    quiet: bool,
) -> None:
    """Generate or expand descriptions for dbt models using LLMs."""

    # Get verbosity settings
    verbose = ctx.obj.get("verbose", False) if ctx.obj else False
    show_progress = not quiet
    show_debug = verbose and not quiet

    # Setup logging with appropriate level
    setup_logging(verbose=verbose, quiet=quiet)

    discovery = ProjectDiscovery()
    dbt_project = discovery.get_project_by_name(project)

    if not dbt_project:
        console.print(f"[red]Project {project} not found[/red]")
        return

    try:
        generator = LLMDescriptionGenerator(LLMProvider(provider), model=llm_model)

        # Show which model is being used
        console.print(f"[dim]Using {provider.upper()} model: {generator.model}[/dim]")

    except ValueError as e:
        console.print(f"[red]Error setting up LLM provider: {e}[/red]")
        console.print(
            "[yellow]Make sure to set the appropriate environment variable "
            "(OPENAI_API_KEY or ANTHROPIC_API_KEY)[/yellow]"
        )
        return

    parser = ModelParser(dbt_project.path)
    models_to_process = discovery.list_models_in_project(project)

    # Filter to specific model if requested
    if model:
        models_to_process = [m for m in models_to_process if m.stem == model]
        if not models_to_process:
            console.print(
                f"[red]Model '{model}' not found in project '{project}'[/red]"
            )
            return

    if show_progress:
        console.print(
            f"[bold blue]Processing {len(models_to_process)} model(s) in "
            f"project '{project}'[/bold blue]\n"
        )

    for model_path in models_to_process:
        if show_progress:
            console.print(f"[bold cyan]📋 Processing: {model_path.name}[/bold cyan]")

        # Parse the model to get existing schema information
        model_info = parser.parse_model(model_path)

        # Create context for LLM
        context = ModelContext(
            name=model_info.name,
            sql_content=model_info.sql_content,
            existing_description=model_info.existing_description,
            columns=model_info.columns,
            project_name=dbt_project.name,
        )

        try:
            # Generate descriptions with PII protection
            with console.status(
                f"[bold green]Generating descriptions for " f"{model_info.name}..."
            ):
                descriptions = generator.generate_descriptions(
                    context,
                    expand_existing=expand,
                    pii_protection_level=pii_protection,
                    project_path=dbt_project.path,
                    verbose=show_debug,
                )

            # Display results
            console.print(
                f"[bold green]✅ Generated descriptions for "
                f"{model_info.name}[/bold green]"
            )
            console.print("[bold]Model Description:[/bold]")
            console.print(f"  {descriptions.model_description}\n")

            if descriptions.column_descriptions:
                console.print("[bold]Column Descriptions:[/bold]")
                for col, desc in descriptions.column_descriptions.items():
                    console.print(f"  [blue]{col}[/blue]: {desc}")
                console.print()

            # Handle file writing
            if not dry_run:
                should_update = True

                if interactive:
                    should_update = click.confirm(
                        f"Update descriptions for {model_info.name}?"
                    )

                if should_update:
                    success = parser.update_model_descriptions(
                        model_info,
                        descriptions.model_description,
                        descriptions.column_descriptions,
                    )

                    if success:
                        console.print(
                            f"[green]✅ Updated descriptions for "
                            f"{model_info.name}[/green]\n"
                        )
                    else:
                        console.print(
                            f"[red]❌ Failed to update descriptions for "
                            f"{model_info.name}[/red]\n"
                        )
                else:
                    console.print(
                        f"[yellow]⏭️  Skipped updating " f"{model_info.name}[/yellow]\n"
                    )
            else:
                console.print("[yellow]🔍 Dry run - no files modified[/yellow]\n")

        except Exception as e:
            console.print(f"[red]❌ Error processing {model_info.name}: {e}[/red]\n")
            continue

    if show_progress:
        if dry_run:
            console.print(
                "[bold yellow]Dry run completed. Use --no-dry-run to "
                "actually update files.[/bold yellow]"
            )
        else:
            console.print("[bold green]Description generation completed![/bold green]")


# Register the command
descriptions.add_command(generate)
