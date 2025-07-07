"""
Unit tests for fabric deployment functionality.

Tests the FabricDeployer class and related deployment logic.
"""

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from pydantic import ValidationError

from dbt_projects_cli.core.fabric import FabricDeployer
from dbt_projects_cli.schemas.fabric import FabricConfig


class TestFabricDeployer:
    """Test the FabricDeployer class."""

    def get_valid_config_dict(self):
        """Return a valid configuration dictionary for testing."""
        return {
            "fabric": {
                "name": "test-fabric",
                "description": "A test fabric",
                "version": "1.0.0",
            },
            "databricks": {
                "host": "test-workspace.cloud.databricks.com",
                "auth_type": "oauth",
            },
            "projects": {
                "default": {
                    "name": "default",
                    "description": "Default test project",
                    "schema": "test_schema",
                    "packages": [
                        {
                            "git": "https://github.com/dbt-labs/dbt-utils.git",
                            "revision": "1.0.0",
                        },
                        {"local": "../packages/utils/core"},
                        {
                            "package": "elementary-data/elementary",
                            "version": ">=0.16.4",
                        },
                    ],
                    "vars": {"PROD_CATALOG": "prod_catalog", "debug_mode": False},
                    "models": {
                        "+materialized": "table",
                        "staging": {"+materialized": "view"},
                    },
                }
            },
        }

    def test_fabric_deployer_initialization(self):
        """Test that FabricDeployer can be initialized."""
        deployer = FabricDeployer()
        assert deployer.console is not None

    def test_load_config_success(self, tmp_path):
        """Test successful configuration loading."""
        config_dict = self.get_valid_config_dict()
        config_file = tmp_path / "fabric.yml"

        with open(config_file, "w") as f:
            yaml.dump(config_dict, f)

        deployer = FabricDeployer()
        config = deployer.load_config(config_file)

        assert isinstance(config, FabricConfig)
        assert config.fabric.name == "test-fabric"
        assert config.databricks.host == "test-workspace.cloud.databricks.com"
        assert len(config.projects) == 1
        assert "default" in config.projects
        assert len(config.projects["default"].packages) == 3

    def test_load_config_file_not_found(self, tmp_path):
        """Test error handling when config file doesn't exist."""
        config_file = tmp_path / "nonexistent.yml"

        deployer = FabricDeployer()

        with pytest.raises(FileNotFoundError) as exc_info:
            deployer.load_config(config_file)

        assert "Configuration file not found" in str(exc_info.value)

    def test_load_config_invalid_yaml(self, tmp_path):
        """Test error handling for invalid YAML."""
        config_file = tmp_path / "invalid.yml"

        with open(config_file, "w") as f:
            f.write("invalid: yaml: content: [")

        deployer = FabricDeployer()

        with pytest.raises(ValueError) as exc_info:
            deployer.load_config(config_file)

        assert "Invalid YAML" in str(exc_info.value)

    def test_load_config_invalid_schema(self, tmp_path):
        """Test error handling for invalid configuration schema."""
        invalid_config = {"invalid": "configuration"}
        config_file = tmp_path / "invalid_config.yml"

        with open(config_file, "w") as f:
            yaml.dump(invalid_config, f)

        deployer = FabricDeployer()

        with pytest.raises(ValidationError):
            deployer.load_config(config_file)

    def test_generate_dbt_project(self, tmp_path):
        """Test dbt project generation."""
        config_dict = self.get_valid_config_dict()
        config = FabricConfig(**config_dict)

        output_dir = tmp_path / "generated_project"

        deployer = FabricDeployer()
        deployer.generate_dbt_project(config, "default", output_dir)

        # Check that directories were created
        assert (output_dir / "models").is_dir()
        assert (output_dir / "macros").is_dir()
        assert (output_dir / "tests").is_dir()
        assert (output_dir / "analyses").is_dir()
        assert (output_dir / "seeds").is_dir()
        assert (output_dir / "snapshots").is_dir()

        # Check that files were created
        assert (output_dir / "dbt_project.yml").is_file()
        assert (output_dir / "packages.yml").is_file()
        assert (output_dir / "profiles.yml").is_file()
        assert (output_dir / "README.md").is_file()

    def test_generate_dbt_project_yml(self, tmp_path):
        """Test dbt_project.yml generation."""
        config_dict = self.get_valid_config_dict()
        config = FabricConfig(**config_dict)

        output_dir = tmp_path / "generated_project"
        output_dir.mkdir()

        deployer = FabricDeployer()
        deployer._generate_dbt_project_yml(
            config, config.projects["default"], output_dir
        )

        dbt_project_file = output_dir / "dbt_project.yml"
        assert dbt_project_file.is_file()

        with open(dbt_project_file, "r") as f:
            dbt_project = yaml.safe_load(f)

        assert dbt_project["name"] == "test_fabric_default"  # Sanitized name
        assert dbt_project["version"] == "1.0.0"
        assert dbt_project["config-version"] == 2
        assert dbt_project["profile"] == "test_fabric"
        assert "models" in dbt_project
        assert "model-paths" in dbt_project
        assert "vars" in dbt_project

    def test_generate_packages_yml(self, tmp_path):
        """Test packages.yml generation."""
        config_dict = self.get_valid_config_dict()
        config = FabricConfig(**config_dict)

        output_dir = tmp_path / "generated_project"
        output_dir.mkdir()

        deployer = FabricDeployer()
        deployer._generate_packages_yml(config.projects["default"], output_dir)

        packages_file = output_dir / "packages.yml"
        assert packages_file.is_file()

        with open(packages_file, "r") as f:
            packages_config = yaml.safe_load(f)

        assert "packages" in packages_config
        packages = packages_config["packages"]
        assert len(packages) == 3

        # Check git package
        git_package = packages[0]
        assert git_package["git"] == "https://github.com/dbt-labs/dbt-utils.git"
        assert git_package["revision"] == "1.0.0"

        # Check local package
        local_package = packages[1]
        assert local_package["local"] == "../packages/utils/core"

        # Check hub package
        hub_package = packages[2]
        assert hub_package["package"] == "elementary-data/elementary"
        assert hub_package["version"] == ">=0.16.4"

    def test_generate_profiles_yml(self, tmp_path):
        """Test profiles.yml generation."""
        config_dict = self.get_valid_config_dict()
        config = FabricConfig(**config_dict)

        output_dir = tmp_path / "generated_project"
        output_dir.mkdir()

        deployer = FabricDeployer()
        deployer._generate_profiles_yml(config, config.projects["default"], output_dir)

        profiles_file = output_dir / "profiles.yml"
        assert profiles_file.is_file()

        with open(profiles_file, "r") as f:
            profiles_config = yaml.safe_load(f)

        profile_name = "test_fabric"  # Sanitized name
        assert profile_name in profiles_config

        profile = profiles_config[profile_name]
        assert profile["target"] == "dev"

        dev_output = profile["outputs"]["dev"]
        assert dev_output["type"] == "databricks"
        assert dev_output["host"] == "test-workspace.cloud.databricks.com"
        assert (
            dev_output["catalog"] == "test-fabric_data_prod_test"
        )  # Auto-generated catalog name
        assert dev_output["schema"] == "test_schema"
        assert dev_output["auth_type"] == "oauth"

    def test_generate_readme(self, tmp_path):
        """Test README.md generation."""
        config_dict = self.get_valid_config_dict()
        config = FabricConfig(**config_dict)

        output_dir = tmp_path / "generated_project"
        output_dir.mkdir()

        deployer = FabricDeployer()
        deployer._generate_readme(config, config.projects["default"], output_dir)

        readme_file = output_dir / "README.md"
        assert readme_file.is_file()

        with open(readme_file, "r") as f:
            readme_content = f.read()

        assert "test-fabric" in readme_content
        assert (
            "Default test project" in readme_content
        )  # Project description, not fabric description
        assert "test_schema" in readme_content
        assert "dbt deps" in readme_content

    def test_package_summary_methods(self):
        """Test package summary generation for different package types."""
        from dbt_projects_cli.schemas.fabric import GitPackage, HubPackage, LocalPackage

        deployer = FabricDeployer()

        # Test git package summary
        git_package = GitPackage(
            git="https://github.com/owner/repo.git",
            subdirectory="path/to/package",
            revision="v1.0.0",
        )
        git_summary = deployer._package_summary(git_package)
        assert "Git: https://github.com/owner/repo.git" in git_summary
        assert "subdirectory: path/to/package" in git_summary
        assert "@ v1.0.0" in git_summary

        # Test local package summary
        local_package = LocalPackage(local="../packages/my-package")
        local_summary = deployer._package_summary(local_package)
        assert "Local: ../packages/my-package" in local_summary

        # Test hub package summary
        hub_package = HubPackage(package="owner/package", version="1.0.0")
        hub_summary = deployer._package_summary(hub_package)
        assert "Hub: owner/package @ 1.0.0" in hub_summary

    def test_deploy_dry_run(self, tmp_path):
        """Test deployment in dry-run mode."""
        config_dict = self.get_valid_config_dict()
        config_file = tmp_path / "fabric.yml"

        with open(config_file, "w") as f:
            yaml.dump(config_dict, f)

        deployer = FabricDeployer()
        result_path = deployer.deploy(config_file, "default", dry_run=True)

        # In dry run, it should return Path(".") for current directory
        assert result_path == Path(".")

    @patch("tempfile.mkdtemp")
    def test_deploy_with_temp_directory(self, mock_mkdtemp, tmp_path):
        """Test deployment using temporary directory."""
        config_dict = self.get_valid_config_dict()
        config_file = tmp_path / "fabric.yml"

        with open(config_file, "w") as f:
            yaml.dump(config_dict, f)

        # Mock tempfile.mkdtemp to return our test directory
        temp_dir = tmp_path / "temp_fabric"
        temp_dir.mkdir()
        mock_mkdtemp.return_value = str(temp_dir)

        deployer = FabricDeployer()
        result_path = deployer.deploy(
            config_file, "default", output_dir=None, dry_run=False
        )

        assert result_path == temp_dir
        mock_mkdtemp.assert_called_once()

        # Check that project was generated
        assert (temp_dir / "dbt_project.yml").is_file()
        assert (temp_dir / "packages.yml").is_file()

    def test_deploy_with_specified_output_dir(self, tmp_path):
        """Test deployment with specified output directory."""
        config_dict = self.get_valid_config_dict()
        config_file = tmp_path / "fabric.yml"

        with open(config_file, "w") as f:
            yaml.dump(config_dict, f)

        output_dir = tmp_path / "my_fabric_deployment"

        deployer = FabricDeployer()
        result_path = deployer.deploy(
            config_file, "default", output_dir=output_dir, dry_run=False
        )

        assert result_path == output_dir

        # Check that project was generated
        assert (output_dir / "dbt_project.yml").is_file()
        assert (output_dir / "packages.yml").is_file()
        assert (output_dir / "profiles.yml").is_file()
        assert (output_dir / "README.md").is_file()
