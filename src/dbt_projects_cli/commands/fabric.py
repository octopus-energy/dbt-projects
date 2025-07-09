"""
CLI commands for lightweight fabric deployment.

This module provides Click commands for managing lightweight fabric deployments,
including configuration validation, project generation, and deployment.
"""

import os
import subprocess
from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from ..core.fabric import FabricDeployer
from ..utils.templates import (
    MULTI_FABRIC_EXAMPLE,
    SCHEMA_EXAMPLE,
    load_template,
)

console = Console()


def _find_default_config_file() -> Path:
    """Find the default fabric configuration file in current directory."""
    default_files = [
        "fabrics.yml",
        "fabrics.yaml",
        "fabrics.json",
        "fabric.yml",
        "fabric.yaml",
    ]

    for filename in default_files:
        config_path = Path(filename)
        if config_path.exists():
            return config_path

    raise click.ClickException(
        f"No fabric configuration file found. Expected one of: "
        f"{', '.join(default_files)}\n"
        f"Create one with: dbt-cli fabric init"
    )


@click.group()
def fabric() -> None:
    """🧱 Lightweight fabric deployment commands.

    Deploy dbt fabrics from simple configuration files instead of
    requiring full dbt project structures.

    Perfect for package-only deployments that don't contain custom models.
    """
    pass


@fabric.command()
@click.argument(
    "config_path", type=click.Path(exists=True, path_type=Path), required=False
)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(path_type=Path),
    help="Output directory for generated dbt project",
)
@click.option(
    "--fabric",
    "-f",
    type=str,
    help="Fabric name (required for multi-fabric configuration files)",
)
@click.option(
    "--project",
    "-p",
    type=str,
    help="Project name to deploy (required for multi-project fabrics)",
)
@click.option(
    "--environment",
    "-e",
    type=click.Choice(["dev", "prod", "source"]),
    default="dev",
    help="Target environment (affects catalog naming)",
)
@click.option(
    "--dry-run", is_flag=True, help="Validate configuration without generating project"
)
@click.option("--verbose", "-v", is_flag=True, help="Show detailed output")
def deploy(
    config_path: Path,
    output_dir: Path,
    fabric: str,
    project: str,
    environment: str,
    dry_run: bool,
    verbose: bool,
) -> None:
    """Deploy a project from fabric configuration.

    Creates dbt project files and installs dependencies (dbt deps) to make the project
    deployment-ready. Use 'generate' if you only want to create the project files.

    CONFIG_PATH defaults to 'fabrics.yml' or 'fabrics.json' in current directory.
    For multi-fabric files, use --fabric to specify which fabric.
    For multi-project fabrics, use --project to specify which project to deploy.

    Example: dbt-cli fabric deploy --fabric analytics --project core --environment prod
    """
    deployer = FabricDeployer(console)

    try:
        # Use default config file if not provided
        if not config_path:
            config_path = _find_default_config_file()

        # Check if this is a multi-fabric config and handle fabric selection
        if not fabric:
            # Try to determine if this is a multi-fabric config
            try:
                fabrics_info = deployer.list_fabrics(config_path)
                if len(fabrics_info) > 1:
                    console.print("📋 Multiple fabrics available:")
                    for i, fabric_info in enumerate(fabrics_info, 1):
                        console.print(
                            f"   {i}. {fabric_info['name']} - "
                            f"{fabric_info['description']}"
                        )

                    while True:
                        try:
                            choice = click.prompt("Select fabric number", type=int)
                            if 1 <= choice <= len(fabrics_info):
                                fabric = fabrics_info[choice - 1]["name"]
                                console.print(f"🎯 Selected fabric: {fabric}")
                                break
                            else:
                                console.print(
                                    f"❌ Please enter a number between 1 and "
                                    f"{len(fabrics_info)}"
                                )
                        except click.Abort:
                            raise
                        except Exception:
                            console.print("❌ Please enter a valid number")
                elif len(fabrics_info) == 1:
                    fabric = fabrics_info[0]["name"]
                    console.print(f"🎯 Auto-selected fabric: {fabric}")
            except Exception:
                # Not a multi-fabric config or error occurred, continue as single fabric
                pass

        # Load the appropriate configuration
        if fabric:
            config = deployer.load_fabric_from_multiple(config_path, fabric)
        else:
            config = deployer.load_config(config_path)

        # Determine project to deploy
        if not project:
            if len(config.projects) == 1:
                project = list(config.projects.keys())[0]
                console.print(f"🎯 Auto-selected project: {project}")
            else:
                console.print("📁 Multiple projects available:")
                project_list = list(config.projects.keys())
                for i, proj_name in enumerate(project_list, 1):
                    proj_config = config.projects[proj_name]
                    description = proj_config.description or "No description"
                    console.print(f"   {i}. {proj_name} - {description}")

                while True:
                    try:
                        choice = click.prompt("Select project number", type=int)
                        if 1 <= choice <= len(project_list):
                            project = project_list[choice - 1]
                            console.print(f"🎯 Selected project: {project}")
                            break
                        else:
                            console.print(
                                f"❌ Please enter a number between 1 and "
                                f"{len(project_list)}"
                            )
                    except click.Abort:
                        raise
                    except Exception:
                        console.print("❌ Please enter a valid number")

        if project not in config.projects:
            available_projects = list(config.projects.keys())
            console.print(
                f"❌ Project '{project}' not found. Available projects: "
                f"{', '.join(available_projects)}"
            )
            raise click.Abort()

        # Generate the project files
        result_path = deployer.deploy_fabric_config(
            config, project, output_dir, dry_run, environment
        )

        if not dry_run:
            console.print(f"\n✅ Project '{project}' files generated successfully!")
            console.print(f"📁 Location: {result_path}")
            console.print(f"🏗️ Environment: {environment}")

            if verbose:
                console.print("\n📋 Generated files:")
                for file_path in result_path.glob("*"):
                    if file_path.is_file():
                        console.print(f"   - {file_path.name}")

            # Change to project directory for dbt deps
            original_cwd = os.getcwd()
            os.chdir(result_path)

            try:
                # Install dependencies to make deployment-ready
                console.print("\n📦 Installing dbt dependencies...")
                result = subprocess.run(["dbt", "deps"], capture_output=True, text=True)
                if result.returncode == 0:
                    console.print("✅ Dependencies installed successfully!")
                    console.print("\n🚀 Project is deployment-ready! Next steps:")
                    console.print(f"   cd {result_path}")
                    console.print("   dbt debug")
                    console.print("   dbt run")
                else:
                    console.print(
                        f"❌ Failed to install dependencies: {result.stderr}",
                        style="red",
                    )
                    console.print("\n📝 Manual steps required:")
                    console.print(f"   cd {result_path}")
                    console.print("   dbt deps")
                    console.print("   dbt debug")
                    console.print("   dbt run")

            finally:
                # Restore original directory
                os.chdir(original_cwd)

    except Exception as e:
        console.print(f"❌ Error: {e}", style="red")
        raise click.Abort()


@fabric.command()
@click.argument(
    "config_path", type=click.Path(exists=True, path_type=Path), required=False
)
@click.option(
    "--fabric",
    "-f",
    type=str,
    help="Fabric name (required for multi-fabric configuration files)",
)
@click.option(
    "--project",
    "-p",
    type=str,
    help="Project name to validate (shows details for specific project)",
)
def validate(config_path: Path, fabric: str, project: str) -> None:
    """Validate a fabric configuration file.

    CONFIG_PATH defaults to 'fabrics.yml' or 'fabrics.json' in current directory.
    For multi-fabric files, use --fabric to specify which fabric to validate.
    Use --project to show details for a specific project.
    """
    deployer = FabricDeployer(console)

    try:
        # Use default config file if not provided
        if not config_path:
            config_path = _find_default_config_file()

        # Load the appropriate configuration
        if fabric:
            config = deployer.load_fabric_from_multiple(config_path, fabric)
        else:
            config = deployer.load_config(config_path)

        console.print("✅ Configuration is valid!", style="green")

        # Show configuration summary
        table = Table(title="Configuration Summary")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="magenta")

        table.add_row("Fabric Name", config.fabric.name)
        table.add_row("Databricks Host", config.databricks.host)
        table.add_row("Projects", str(len(config.projects)))

        # Calculate total packages across all projects
        total_packages = sum(len(proj.packages) for proj in config.projects.values())
        table.add_row("Total Packages", str(total_packages))

        if config.fabric.description:
            table.add_row("Description", config.fabric.description)

        console.print(table)

        # Show projects summary
        console.print("\n📁 Projects:")
        for proj_name, proj_config in config.projects.items():
            if project and proj_name != project:
                continue

            console.print(f"\n   📦 {proj_name}:")
            console.print(f"      Schema: {proj_config.schema_name}")
            console.print(f"      Packages: {len(proj_config.packages)}")

            if proj_config.description:
                console.print(f"      Description: {proj_config.description}")

            # Show packages for this project
            if proj_config.packages:
                console.print("      Package details:")
                for i, package in enumerate(proj_config.packages, 1):
                    package_info = deployer._package_summary(package)
                    console.print(f"         {i}. {package_info}")

            # Show variables if any
            if proj_config.vars:
                console.print("      Variables:")
                for key, value in proj_config.vars.items():
                    console.print(f"         - {key}: {value}")

    except Exception as e:
        console.print(f"❌ Validation failed: {e}", style="red")
        raise click.Abort()


@fabric.command("list-fabrics")
@click.argument(
    "config_path", type=click.Path(exists=True, path_type=Path), required=False
)
def list_fabrics(config_path: Path) -> None:
    """List all fabrics in a multi-fabric configuration file.

    CONFIG_PATH defaults to 'fabrics.yml' or 'fabrics.json' in current directory.
    """
    deployer = FabricDeployer(console)

    try:
        # Use default config file if not provided
        if not config_path:
            config_path = _find_default_config_file()

        fabrics_info = deployer.list_fabrics(config_path)

        console.print("📋 Available Fabrics", style="bold blue")

        table = Table()
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("Description", style="yellow")
        table.add_column("Host", style="magenta")
        table.add_column("Projects", justify="right", style="blue")
        table.add_column("Packages", justify="right", style="white")

        for fabric_info in fabrics_info:
            table.add_row(
                fabric_info["name"],
                fabric_info["description"],
                fabric_info["host"],
                fabric_info["projects"],
                fabric_info["packages"],
            )

        console.print(table)

        console.print("🚀 Usage examples:")
        console.print("   dbt-cli fabric validate --fabric <fabric-name>")
        console.print(
            "   dbt-cli fabric deploy --fabric <fabric-name> --project <project-name>"
        )

    except Exception as e:
        console.print(f"❌ Error: {e}", style="red")
        raise click.Abort()


@fabric.command()
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=Path("fabrics.yml"),
    help="Output file path (default: fabrics.yml)",
)
@click.option("--name", prompt="Fabric name", help="Name of the fabric")
@click.option("--description", help="Description of the fabric")
@click.option(
    "--databricks-host", prompt="Databricks host", help="Databricks workspace hostname"
)
@click.option(
    "--project-name",
    prompt="Project name",
    default="core",
    help="Name of the default project (default: core)",
)
def init(
    output: Path, name: str, description: str, databricks_host: str, project_name: str
) -> None:
    """Initialize a new multi-fabric configuration file.

    Creates a template fabrics.yml file with multi-fabric configuration format.
    """
    config_template = {
        "fabrics": {
            name: {
                "fabric": {"name": name},
                "databricks": {"host": databricks_host},
                "projects": {
                    project_name: {
                        "description": f"{project_name.title()} data transformations",
                        "packages": [
                            {
                                "package": "elementary-data/elementary",
                                "version": ">=0.16.4",
                            },
                        ],
                        "vars": [],
                    }
                },
            }
        }
    }

    import yaml

    if output.exists():
        if not click.confirm(f"File {output} already exists. Overwrite?"):
            console.print("❌ Operation cancelled.")
            return

    with open(output, "w") as f:
        yaml.dump(config_template, f, default_flow_style=False, sort_keys=False)

    console.print(f"✅ Created multi-fabric configuration: {output}")
    console.print(f"\n📝 Edit {output} to customize your deployment:")
    console.print(
        f"   - Add/remove packages in the "
        f"'fabrics.{name}.projects.{project_name}.packages' section"
    )
    console.print(
        f"   - Modify variables in the "
        f"'fabrics.{name}.projects.{project_name}.vars' section"
    )
    console.print(f"   - Add more projects by copying the {project_name} section")
    console.print(f"   - Add more fabrics by copying the {name} section")
    console.print(
        "\n💡 Schema will be set via DATABRICKS_SCHEMA environment variable "
        "during deployment"
    )
    console.print(
        f"\n🚀 Deploy with: dbt-cli fabric deploy --fabric {name} "
        f"--project {project_name}"
    )
    console.print("🔍 List fabrics with: dbt-cli fabric list-fabrics")


@fabric.command()
def schema() -> None:
    """Show the fabric configuration schema and examples."""

    console.print(
        Panel.fit("📋 Fabric Configuration Schema", style="blue", padding=(1, 2))
    )

    schema_example = load_template(SCHEMA_EXAMPLE)

    console.print(Syntax(schema_example, "yaml", theme="monokai", line_numbers=True))

    console.print("\n📝 Package Types Supported:")
    console.print("   • Git repositories (with optional subdirectory and revision)")
    console.print("   • Local file paths (relative or absolute)")
    console.print("   • dbt Hub packages (with version specification)")

    console.print("\n🔧 Advanced Features:")
    console.print("   • Project-level package and configuration management")
    console.print(
        "   • Automatic catalog naming with environment support (dev/prod/source)"
    )
    console.print("   • Automatic profiles.yml generation for Databricks")
    console.print("   • Validation with detailed error messages")
    console.print("   • Dry-run mode for testing configurations")
    console.print("   • Multiple fabrics in single JSON file")

    console.print("\n📄 Multiple Fabrics JSON Format:")
    multiple_fabrics_example = load_template(MULTI_FABRIC_EXAMPLE)

    console.print(
        Syntax(multiple_fabrics_example, "json", theme="monokai", line_numbers=True)
    )

    console.print("\n🚀 Multi-Fabric Usage:")
    console.print("   dbt-cli fabric list-fabrics fabrics.json")
    console.print("   dbt-cli fabric validate fabrics.json --fabric data-quality")
    console.print("   dbt-cli fabric deploy fabrics.json --fabric analytics")


@fabric.command()
@click.argument(
    "config_path", type=click.Path(exists=True, path_type=Path), required=False
)
@click.option(
    "--fabric",
    "-f",
    type=str,
    help="Fabric name (required for multi-fabric configuration files)",
)
@click.option(
    "--project",
    "-p",
    type=str,
    help="Project name to generate (required for multi-project fabrics)",
)
@click.option(
    "--environment",
    "-e",
    type=click.Choice(["dev", "prod", "source"]),
    default="dev",
    help="Target environment (affects catalog naming)",
)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(path_type=Path),
    help="Output directory for generated dbt project",
)
def generate(
    config_path: Path, fabric: str, project: str, environment: str, output_dir: Path
) -> None:
    """Generate a dbt project from fabric configuration.

    Creates the dbt project files only, without installing dependencies.
    Use 'deploy' if you want to create files and install dependencies.

    CONFIG_PATH defaults to 'fabrics.yml' or 'fabrics.json' in current directory.
    """
    deployer = FabricDeployer(console)

    try:
        # Use default config file if not provided
        if not config_path:
            config_path = _find_default_config_file()

        # Check if this is a multi-fabric config and handle fabric selection
        if not fabric:
            # Try to determine if this is a multi-fabric config
            try:
                fabrics_info = deployer.list_fabrics(config_path)
                if len(fabrics_info) > 1:
                    console.print("📋 Multiple fabrics available:")
                    for i, fabric_info in enumerate(fabrics_info, 1):
                        console.print(
                            f"   {i}. {fabric_info['name']} - "
                            f"{fabric_info['description']}"
                        )

                    while True:
                        try:
                            choice = click.prompt("Select fabric number", type=int)
                            if 1 <= choice <= len(fabrics_info):
                                fabric = fabrics_info[choice - 1]["name"]
                                console.print(f"🎯 Selected fabric: {fabric}")
                                break
                            else:
                                console.print(
                                    f"❌ Please enter a number between 1 and "
                                    f"{len(fabrics_info)}"
                                )
                        except click.Abort:
                            raise
                        except Exception:
                            console.print("❌ Please enter a valid number")
                elif len(fabrics_info) == 1:
                    fabric = fabrics_info[0]["name"]
                    console.print(f"🎯 Auto-selected fabric: {fabric}")
            except Exception:
                # Not a multi-fabric config or error occurred, continue as single fabric
                pass

        # Load the appropriate configuration
        if fabric:
            config = deployer.load_fabric_from_multiple(config_path, fabric)
        else:
            config = deployer.load_config(config_path)

        # Determine project to generate
        if not project:
            if len(config.projects) == 1:
                project = list(config.projects.keys())[0]
                console.print(f"🎯 Auto-selected project: {project}")
            else:
                console.print("📁 Multiple projects available:")
                project_list = list(config.projects.keys())
                for i, proj_name in enumerate(project_list, 1):
                    proj_config = config.projects[proj_name]
                    description = proj_config.description or "No description"
                    console.print(f"   {i}. {proj_name} - {description}")

                while True:
                    try:
                        choice = click.prompt("Select project number", type=int)
                        if 1 <= choice <= len(project_list):
                            project = project_list[choice - 1]
                            console.print(f"🎯 Selected project: {project}")
                            break
                        else:
                            console.print(
                                f"❌ Please enter a number between 1 and "
                                f"{len(project_list)}"
                            )
                    except click.Abort:
                        raise
                    except Exception:
                        console.print("❌ Please enter a valid number")

        if project not in config.projects:
            available_projects = list(config.projects.keys())
            console.print(
                f"❌ Project '{project}' not found. Available projects: "
                f"{', '.join(available_projects)}"
            )
            raise click.Abort()

        # Generate the project files only
        result_path = deployer.deploy_fabric_config(
            config, project, output_dir, dry_run=False, environment=environment
        )

        console.print("✅ dbt project files generated successfully!")
        console.print(f"📁 Location: {result_path}")
        console.print(f"🏗️ Environment: {environment}")

        console.print("\n🚀 Next steps:")
        console.print(f"   cd {result_path}")
        console.print("   dbt deps")
        console.print("   dbt debug")
        console.print("   dbt run")

        console.print(
            "\n💡 Tip: Use 'dbt-cli fabric deploy' to create project files "
            "and install dependencies in one step"
        )

    except Exception as e:
        console.print(f"❌ Error: {e}", style="red")
        raise click.Abort()


@fabric.command("add-fabric")
@click.argument(
    "config_path", type=click.Path(exists=True, path_type=Path), required=False
)
@click.option("--name", prompt="Fabric name", help="Name of the new fabric")
@click.option("--description", help="Description of the fabric")
@click.option(
    "--databricks-host", prompt="Databricks host", help="Databricks workspace hostname"
)
def add_fabric(
    config_path: Path, name: str, description: str, databricks_host: str
) -> None:
    """Add a new fabric to an existing multi-fabric configuration file.

    Creates a new fabric with a default 'core' project.
    CONFIG_PATH defaults to 'fabrics.yml' or 'fabrics.json' in current directory.
    """
    import yaml

    try:
        # Use default config file if not provided
        if not config_path:
            config_path = _find_default_config_file()

        # Load existing configuration
        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f)

        # Ensure it's a multi-fabric config
        if "fabrics" not in config_data:
            console.print(
                "❌ Configuration file is not in multi-fabric format. "
                "Use 'dbt-cli fabric init' to create a new multi-fabric "
                "configuration.",
                style="red",
            )
            raise click.Abort()

        # Check if fabric already exists
        if name in config_data["fabrics"]:
            if not click.confirm(f"Fabric '{name}' already exists. Overwrite?"):
                console.print("❌ Operation cancelled.")
                return

        # Add new fabric with default 'core' project
        config_data["fabrics"][name] = {
            "fabric": {"name": name},
            "databricks": {"host": databricks_host},
            "projects": {
                "core": {
                    "description": "Core data transformations",
                    "packages": [
                        {"package": "elementary-data/elementary", "version": ">=0.16.4"}
                    ],
                    "vars": [],
                }
            },
        }

        # Write updated configuration
        with open(config_path, "w") as f:
            yaml.dump(config_data, f, default_flow_style=False, sort_keys=False)

        console.print(f"✅ Added fabric '{name}' to {config_path}")
        console.print(
            f"🚀 Deploy with: dbt-cli fabric deploy --fabric {name} --project core"
        )

    except Exception as e:
        console.print(f"❌ Error: {e}", style="red")
        raise click.Abort()


@fabric.command("add-project")
@click.argument(
    "config_path", type=click.Path(exists=True, path_type=Path), required=False
)
@click.option(
    "--fabric",
    "-f",
    help="Fabric name to add project to (will prompt to select if multiple "
    "fabrics exist)",
)
@click.option("--name", prompt="Project name", help="Name of the new project")
@click.option("--description", help="Description of the project")
def add_project(config_path: Path, fabric: str, name: str, description: str) -> None:
    """Add a new project to an existing fabric.

    CONFIG_PATH defaults to 'fabrics.yml' or 'fabrics.json' in current directory.
    """
    import yaml

    try:
        # Use default config file if not provided
        if not config_path:
            config_path = _find_default_config_file()

        # Load existing configuration
        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f)

        # Handle both single and multi-fabric configs
        if "fabrics" in config_data:
            # Multi-fabric config
            if not fabric:
                if len(config_data["fabrics"]) == 1:
                    fabric = list(config_data["fabrics"].keys())[0]
                    console.print(f"🎯 Auto-selected fabric: {fabric}")
                else:
                    console.print(
                        "❌ Multiple fabrics available. Please specify --fabric:"
                    )
                    for fabric_name in config_data["fabrics"].keys():
                        console.print(f"   - {fabric_name}")
                    raise click.Abort()

            if fabric not in config_data["fabrics"]:
                available_fabrics = list(config_data["fabrics"].keys())
                console.print(
                    f"❌ Fabric '{fabric}' not found. Available fabrics: "
                    f"{', '.join(available_fabrics)}"
                )
                raise click.Abort()

            # Check if project already exists
            if name in config_data["fabrics"][fabric]["projects"]:
                if not click.confirm(
                    f"Project '{name}' already exists in fabric '{fabric}'. Overwrite?"
                ):
                    console.print("❌ Operation cancelled.")
                    return

            # Add project to fabric
            fabric_config = config_data["fabrics"][fabric]
        else:
            # Single fabric config - convert to multi-fabric
            if not click.confirm(
                "This appears to be a single fabric configuration. "
                "Convert to multi-fabric format?"
            ):
                console.print("❌ Operation cancelled.")
                return

            # Get existing fabric name
            existing_fabric_name = config_data.get("fabric", {}).get("name", "default")

            # Convert to multi-fabric format
            config_data = {"fabrics": {existing_fabric_name: config_data}}

            fabric = existing_fabric_name
            fabric_config = config_data["fabrics"][fabric]
            console.print(
                f"✅ Converted to multi-fabric format. Using fabric: {fabric}"
            )

        # Add new project
        fabric_config["projects"][name] = {
            "description": description or f"{name.title()} data transformations",
            "packages": [
                {"package": "elementary-data/elementary", "version": ">=0.16.4"}
            ],
            "vars": [],
        }

        # Write updated configuration
        with open(config_path, "w") as f:
            yaml.dump(config_data, f, default_flow_style=False, sort_keys=False)

        console.print(
            f"✅ Added project '{name}' to fabric '{fabric}' in {config_path}"
        )
        console.print(
            f"🚀 Deploy with: dbt-cli fabric deploy --fabric {fabric} --project {name}"
        )

    except Exception as e:
        console.print(f"❌ Error: {e}", style="red")
        raise click.Abort()
