"""
Core fabric deployment functionality.

This module handles the lightweight fabric deployment logic including
configuration validation, temporary project generation, and deployment.
"""

import os
import tempfile
import shutil
import yaml
import json
from pathlib import Path
from typing import Dict, Any, Optional, Union, List
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from ..schemas.fabric import FabricConfig, MultipleFabricsConfig, ProjectConfig, GitPackage, LocalPackage, HubPackage, LegacyFabricConfig


class FabricDeployer:
    """Handles lightweight fabric deployment operations."""
    
    def __init__(self, console: Optional[Console] = None):
        """Initialize the fabric deployer.
        
        Args:
            console: Rich console instance for output
        """
        self.console = console or Console()
    
    def _is_json_file(self, file_path: Path) -> bool:
        """Check if file is JSON based on extension or content."""
        return file_path.suffix.lower() in ['.json']
    
    def _load_file_data(self, config_path: Path) -> Dict[str, Any]:
        """Load configuration data from YAML or JSON file."""
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        try:
            with open(config_path, 'r') as f:
                if self._is_json_file(config_path):
                    return json.load(f)
                else:
                    return yaml.safe_load(f)
        except (yaml.YAMLError, json.JSONDecodeError) as e:
            file_type = "JSON" if self._is_json_file(config_path) else "YAML"
            raise ValueError(f"Invalid {file_type} in {config_path}: {e}")
    
    def _is_multiple_fabrics_config(self, config_data: Dict[str, Any]) -> bool:
        """Check if configuration contains multiple fabrics."""
        return 'fabrics' in config_data and isinstance(config_data.get('fabrics'), dict)
    
    def load_config(self, config_path: Path) -> FabricConfig:
        """Load and validate fabric configuration from file.
        
        Args:
            config_path: Path to the fabric configuration file
            
        Returns:
            Validated fabric configuration
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If file format is invalid
            pydantic.ValidationError: If configuration is invalid
        """
        config_data = self._load_file_data(config_path)
        
        # Check if this is a multiple fabrics config
        if self._is_multiple_fabrics_config(config_data):
            raise ValueError(
                f"Configuration file contains multiple fabrics. "
                f"Use 'list-fabrics' command to see available fabrics, "
                f"then specify a fabric name with the '--fabric' option."
            )
        
        # Try to load as new format first
        try:
            return FabricConfig(**config_data)
        except Exception:
            # If that fails, try legacy format and convert
            try:
                legacy_config = LegacyFabricConfig(**config_data)
                return legacy_config.to_new_format()
            except Exception:
                # If both fail, re-raise the original error
                return FabricConfig(**config_data)
    
    def load_multiple_fabrics_config(self, config_path: Path) -> MultipleFabricsConfig:
        """Load and validate multiple fabrics configuration from file.
        
        Args:
            config_path: Path to the fabrics configuration file
            
        Returns:
            Validated multiple fabrics configuration
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If file format is invalid
            pydantic.ValidationError: If configuration is invalid
        """
        config_data = self._load_file_data(config_path)
        
        # Validate as multiple fabrics configuration
        return MultipleFabricsConfig(**config_data)
    
    def load_fabric_from_multiple(self, config_path: Path, fabric_name: str) -> FabricConfig:
        """Load a specific fabric from a multiple fabrics configuration file.
        
        Args:
            config_path: Path to the fabrics configuration file
            fabric_name: Name of the fabric to load
            
        Returns:
            Validated fabric configuration
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If fabric name not found
            pydantic.ValidationError: If configuration is invalid
        """
        config_data = self._load_file_data(config_path)
        
        if not self._is_multiple_fabrics_config(config_data):
            raise ValueError(
                f"Configuration file does not contain multiple fabrics. "
                f"Use the standard commands without '--fabric' option."
            )
        
        # Load and validate the multiple fabrics config
        multiple_config = MultipleFabricsConfig(**config_data)
        
        # Extract the specific fabric
        if fabric_name not in multiple_config.fabrics:
            available_fabrics = list(multiple_config.fabrics.keys())
            raise ValueError(
                f"Fabric '{fabric_name}' not found. "
                f"Available fabrics: {', '.join(available_fabrics)}"
            )
        
        return multiple_config.fabrics[fabric_name]
    
    def list_fabrics(self, config_path: Path) -> List[Dict[str, str]]:
        """List all fabrics in a multiple fabrics configuration file.
        
        Args:
            config_path: Path to the fabrics configuration file
            
        Returns:
            List of fabric information dictionaries
        """
        config_data = self._load_file_data(config_path)
        
        if not self._is_multiple_fabrics_config(config_data):
            raise ValueError(
                f"Configuration file does not contain multiple fabrics. "
                f"This appears to be a single fabric configuration."
            )
        
        # Load and validate the multiple fabrics config
        multiple_config = MultipleFabricsConfig(**config_data)
        
        # Extract fabric information
        fabrics_info = []
        for name, fabric_config in multiple_config.fabrics.items():
            # Count total packages across all projects
            total_packages = sum(len(project.packages) for project in fabric_config.projects.values())
            fabrics_info.append({
                'name': name,
                'description': fabric_config.fabric.description or 'No description',
                'version': fabric_config.fabric.version,
                'host': fabric_config.databricks.host,
                'projects': str(len(fabric_config.projects)),
                'packages': str(total_packages)
            })
        
        return fabrics_info
    
    def generate_dbt_project(self, config: FabricConfig, project_name: str, output_dir: Path, 
                            environment: str = "dev") -> None:
        """Generate a temporary dbt project structure from fabric config for a specific project.
        
        Args:
            config: Validated fabric configuration
            project_name: Name of the project to deploy
            output_dir: Directory to create the dbt project in
            environment: Target environment (dev, prod, source)
        """
        if project_name not in config.projects:
            available_projects = list(config.projects.keys())
            raise ValueError(
                f"Project '{project_name}' not found. "
                f"Available projects: {', '.join(available_projects)}"
            )
        
        project_config = config.projects[project_name]
        
        # Create directory structure
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create basic directories
        (output_dir / "models").mkdir(exist_ok=True)
        (output_dir / "macros").mkdir(exist_ok=True)
        (output_dir / "tests").mkdir(exist_ok=True)
        (output_dir / "analyses").mkdir(exist_ok=True)
        (output_dir / "seeds").mkdir(exist_ok=True)
        (output_dir / "snapshots").mkdir(exist_ok=True)
        
        # Generate dbt_project.yml
        self._generate_dbt_project_yml(config, project_config, output_dir)
        
        # Generate packages.yml
        self._generate_packages_yml(project_config, output_dir)
        
        # Generate profiles.yml
        self._generate_profiles_yml(config, project_config, output_dir, environment)
        
        # Create a basic README
        self._generate_readme(config, project_config, output_dir)
    
    def _generate_dbt_project_yml(self, config: FabricConfig, project: ProjectConfig, 
                                 output_dir: Path) -> None:
        """Generate dbt_project.yml file for a specific project."""
        fabric_name_safe = config.fabric.name.replace('-', '_').replace(' ', '_')
        project_name_safe = project.name.replace('-', '_').replace(' ', '_')
        
        dbt_project = {
            'name': f"{fabric_name_safe}_{project_name_safe}",
            'version': config.fabric.version,
            'config-version': 2,
            'profile': fabric_name_safe,
            'model-paths': ['models'],
            'analysis-paths': ['analyses'],
            'test-paths': ['tests'],
            'seed-paths': ['seeds'],
            'macro-paths': ['macros'],
            'snapshot-paths': ['snapshots'],
            'clean-targets': ['dbt_packages', 'target'],
        }
        
        # Add model configurations if provided
        if project.models:
            models_config = {f"{fabric_name_safe}_{project_name_safe}": project.models}
            dbt_project['models'] = models_config
        
        # Add variables if provided
        if project.vars:
            dbt_project['vars'] = project.vars
        
        with open(output_dir / "dbt_project.yml", 'w') as f:
            yaml.dump(dbt_project, f, default_flow_style=False, sort_keys=False)
    
    def _generate_packages_yml(self, project: ProjectConfig, output_dir: Path) -> None:
        """Generate packages.yml file for a specific project."""
        packages_list = []
        
        for package in project.packages:
            package_dict = {}
            
            if isinstance(package, GitPackage):
                package_dict['git'] = package.git
                if package.subdirectory:
                    package_dict['subdirectory'] = package.subdirectory
                if package.revision:
                    package_dict['revision'] = package.revision
                if package.warn_unpinned is not None:
                    package_dict['warn-unpinned'] = package.warn_unpinned
            
            elif isinstance(package, LocalPackage):
                package_dict['local'] = package.local
            
            elif isinstance(package, HubPackage):
                package_dict['package'] = package.package
                package_dict['version'] = package.version
            
            packages_list.append(package_dict)
        
        packages_config = {'packages': packages_list}
        
        with open(output_dir / "packages.yml", 'w') as f:
            yaml.dump(packages_config, f, default_flow_style=False, sort_keys=False)
    
    def _generate_profiles_yml(self, config: FabricConfig, project: ProjectConfig, 
                             output_dir: Path, environment: str = "dev") -> None:
        """Generate profiles.yml file for Databricks connection with environment-specific settings."""
        fabric_name_safe = config.fabric.name.replace('-', '_').replace(' ', '_')
        
        # Determine catalog name based on environment and project settings
        catalog_name = project.catalog if project.catalog else config.get_catalog_name(environment)
        
        profile_config = {
            fabric_name_safe: {
                'target': environment,
                'outputs': {
                    environment: {
                        'type': 'databricks',
                        'host': config.databricks.host,
                        'catalog': catalog_name,
                        'schema': project.schema_name,
                        'auth_type': config.databricks.auth_type or 'oauth'
                    }
                }
            }
        }
        
        # Add production-specific M2M OAuth settings
        if environment == "prod":
            profile_config[fabric_name_safe]['outputs'][environment].update({
                'auth_type': 'oauth',
                'token': '{{ env_var("DBT_DATABRICKS_TOKEN") }}',
                'client_id': '{{ env_var("DBT_DATABRICKS_CLIENT_ID") }}',
                'client_secret': '{{ env_var("DBT_DATABRICKS_CLIENT_SECRET") }}'
            })
        
        # Add http_path if provided
        if config.databricks.http_path:
            profile_config[fabric_name_safe]['outputs'][environment]['http_path'] = config.databricks.http_path
        
        with open(output_dir / "profiles.yml", 'w') as f:
            yaml.dump(profile_config, f, default_flow_style=False, sort_keys=False)
    
    def _generate_readme(self, config: FabricConfig, project: ProjectConfig, output_dir: Path) -> None:
        """Generate a basic README.md file for a specific project."""
        readme_content = f"""# {config.fabric.name} - {project.name}

{project.description or config.fabric.description or 'Lightweight fabric deployment'}

## Generated Project

This dbt project was automatically generated from a lightweight fabric configuration.

### Configuration Summary

- **Fabric Name**: {config.fabric.name}
- **Project Name**: {project.name}
- **Version**: {config.fabric.version}
- **dbt Version**: {config.fabric.dbt_version}
- **Databricks Host**: {config.databricks.host}
- **Project Schema**: {project.schema_name}

### Packages Included

{chr(10).join([f"- {self._package_summary(pkg)}" for pkg in project.packages])}

### Usage

1. Install dependencies: `dbt deps`
2. Test connection: `dbt debug`
3. Run models: `dbt run` (if any models are included in packages)

### Notes

This is a temporary deployment structure. The original configuration is managed 
separately and this project should not be modified directly.

Catalog naming follows the convention:
- Dev: {config.fabric.name}_data_prod_test
- Prod: {config.fabric.name}_data_prod_prod
- Source: {config.fabric.name}_data_prod_source
"""
        
        with open(output_dir / "README.md", 'w') as f:
            f.write(readme_content)
    
    def _package_summary(self, package) -> str:
        """Generate a summary string for a package."""
        if isinstance(package, GitPackage):
            base = f"Git: {package.git}"
            if package.subdirectory:
                base += f" (subdirectory: {package.subdirectory})"
            if package.revision:
                base += f" @ {package.revision}"
            return base
        elif isinstance(package, LocalPackage):
            return f"Local: {package.local}"
        elif isinstance(package, HubPackage):
            return f"Hub: {package.package} @ {package.version}"
        return str(package)
    
    def deploy_fabric_config(self, config: FabricConfig, project_name: str, 
                            output_dir: Optional[Path] = None, 
                            dry_run: bool = False, environment: str = "dev") -> Path:
        """Deploy a lightweight fabric project from a configuration object.
        
        Args:
            config: Validated fabric configuration
            project_name: Name of the project to deploy
            output_dir: Output directory (uses temp if not provided)
            dry_run: If True, only validate configuration without deploying
            environment: Target environment (dev, prod, source)
            
        Returns:
            Path to the generated dbt project
        """
        if project_name not in config.projects:
            available_projects = list(config.projects.keys())
            raise ValueError(
                f"Project '{project_name}' not found. "
                f"Available projects: {', '.join(available_projects)}"
            )
        
        project_config = config.projects[project_name]
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=self.console,
        ) as progress:
            
            task = progress.add_task("Processing configuration...", total=None)
            progress.update(task, description="✅ Configuration validated")
            
            if dry_run:
                self.console.print("✅ Configuration is valid!")
                self.console.print(f"Fabric: {config.fabric.name}")
                self.console.print(f"Project: {project_name}")
                self.console.print(f"Environment: {environment}")
                self.console.print(f"Packages: {len(project_config.packages)} configured")
                catalog_name = project_config.catalog if project_config.catalog else config.get_catalog_name(environment)
                self.console.print(f"Catalog: {catalog_name}")
                self.console.print(f"Schema: {project_config.schema_name}")
                return Path(".")  # Return current directory for dry run
            
            # Generate project structure
            if output_dir is None:
                output_dir = Path(tempfile.mkdtemp(prefix=f"fabric_{config.fabric.name}_{project_name}_"))
            
            progress.update(task, description="Generating dbt project structure...")
            self.generate_dbt_project(config, project_name, output_dir, environment)
            progress.update(task, description="✅ dbt project generated")
            
        return output_dir
    
    def deploy(self, config_path: Path, project_name: str, output_dir: Optional[Path] = None, 
               dry_run: bool = False, environment: str = "dev") -> Path:
        """Deploy a lightweight fabric project from a configuration file.
        
        Args:
            config_path: Path to fabric configuration file
            project_name: Name of the project to deploy
            output_dir: Output directory (uses temp if not provided)
            dry_run: If True, only validate configuration without deploying
            environment: Target environment (dev, prod, source)
            
        Returns:
            Path to the generated dbt project
        """
        config = self.load_config(config_path)
        return self.deploy_fabric_config(config, project_name, output_dir, dry_run, environment)
