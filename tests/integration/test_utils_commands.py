"""
Integration tests for utils CLI commands.

Tests the CLI commands end-to-end with real dbt projects.
"""

from unittest.mock import patch

import yaml
from click.testing import CliRunner

from dbt_projects_cli.commands.utils import utils


class TestUtilsCommands:
    """Test utils CLI commands."""

    def create_test_dbt_project(self, tmp_path, project_name="test_project"):
        """Create a minimal dbt project for testing."""
        project_dir = tmp_path / project_name
        project_dir.mkdir()

        # Create dbt_project.yml
        dbt_project = {
            "name": project_name,
            "version": "1.0.0",
            "profile": "test_profile",
            "model-paths": ["models"],
            "target-path": "target",
        }

        with open(project_dir / "dbt_project.yml", "w") as f:
            yaml.dump(dbt_project, f)

        # Create target directory (to be cleaned)
        target_dir = project_dir / "target"
        target_dir.mkdir()

        with open(target_dir / "manifest.json", "w") as f:
            f.write('{"version": "1.0.0"}')

        # Create dbt_packages directory (to be cleaned)
        packages_dir = project_dir / "dbt_packages"
        packages_dir.mkdir()

        with open(packages_dir / "package.yml", "w") as f:
            f.write("name: test_package")

        return project_dir

    def test_utils_command_group(self):
        """Test that utils command group is accessible."""
        runner = CliRunner()
        result = runner.invoke(utils, ["--help"])

        assert result.exit_code == 0
        assert "Utility commands for dbt projects" in result.output
        assert "validate" in result.output
        assert "clean" in result.output

    def test_validate_projects_all_valid(self, tmp_path):
        """Test validating projects when all are valid."""
        # Create test projects
        self.create_test_dbt_project(tmp_path, "project1")
        self.create_test_dbt_project(tmp_path, "project2")

        runner = CliRunner()
        with patch(
            "dbt_projects_cli.core.project_discovery.ProjectDiscovery"
        ) as mock_discovery:
            # Mock project discovery
            mock_instance = mock_discovery.return_value
            mock_instance.discover_all_projects.return_value = {
                "packages": [
                    {
                        "name": "project1",
                        "model_count": 2,
                        "config": {"profile": "test_profile"},
                    },
                    {
                        "name": "project2",
                        "model_count": 1,
                        "config": {"profile": "another_profile"},
                    },
                ],
                "fabrics": [],
            }

            result = runner.invoke(utils, ["validate"])

        assert result.exit_code == 0
        assert "Validating dbt projects..." in result.output
        assert "2/2 projects validated successfully" in result.output

    def test_validate_projects_some_invalid(self, tmp_path):
        """Test validating projects when some are invalid."""
        runner = CliRunner()
        with patch(
            "dbt_projects_cli.core.project_discovery.ProjectDiscovery"
        ) as mock_discovery:
            # Mock project discovery
            mock_instance = mock_discovery.return_value
            mock_instance.discover_all_projects.return_value = {
                "packages": [
                    {
                        "name": "valid_project",
                        "model_count": 2,
                        "config": {"profile": "test_profile"},
                    },
                    {
                        "name": "no_profile_project",
                        "model_count": 1,
                        "config": {},  # Missing profile
                    },
                    {
                        "name": "no_models_project",
                        "model_count": 0,
                        "config": {"profile": "test_profile"},
                    },
                ],
                "fabrics": [],
            }

            result = runner.invoke(utils, ["validate"])

        assert result.exit_code == 0
        assert "Validating dbt projects..." in result.output
        assert "2/3 projects validated successfully" in result.output
        assert "Warning: No profile specified" in result.output
        assert "Info: No models found" in result.output

    def test_validate_projects_empty(self, tmp_path):
        """Test validating when no projects exist."""
        runner = CliRunner()
        with patch(
            "dbt_projects_cli.core.project_discovery.ProjectDiscovery"
        ) as mock_discovery:
            # Mock project discovery to return empty results
            mock_instance = mock_discovery.return_value
            mock_instance.discover_all_projects.return_value = {
                "packages": [],
                "fabrics": [],
            }

            result = runner.invoke(utils, ["validate"])

        assert result.exit_code == 0
        assert "Validating dbt projects..." in result.output
        assert "0/0 projects validated successfully" in result.output

    def test_clean_projects_with_directories_to_clean(self, tmp_path):
        """Test cleaning projects when there are directories to clean."""
        # Create test projects with directories to clean
        project1_dir = self.create_test_dbt_project(tmp_path, "project1")
        project2_dir = self.create_test_dbt_project(tmp_path, "project2")

        runner = CliRunner()
        with patch(
            "dbt_projects_cli.core.project_discovery.ProjectDiscovery"
        ) as mock_discovery:
            # Mock project discovery
            mock_instance = mock_discovery.return_value
            mock_instance.root_path = tmp_path
            mock_instance.discover_all_projects.return_value = {
                "packages": [
                    {"name": "project1", "path": "project1"},
                    {"name": "project2", "path": "project2"},
                ],
                "fabrics": [],
            }

            result = runner.invoke(utils, ["clean"])

        assert result.exit_code == 0
        assert "Cleaning dbt projects..." in result.output
        assert "Successfully cleaned" in result.output

        # Check that directories were actually cleaned
        assert not (project1_dir / "target").exists()
        assert not (project1_dir / "dbt_packages").exists()
        assert not (project2_dir / "target").exists()
        assert not (project2_dir / "dbt_packages").exists()

    def test_clean_projects_no_directories_to_clean(self, tmp_path):
        """Test cleaning projects when there are no directories to clean."""
        # Create test projects without target/dbt_packages directories
        project1_dir = tmp_path / "project1"
        project1_dir.mkdir()

        # Create dbt_project.yml but no target/dbt_packages directories
        dbt_project = {
            "name": "project1",
            "version": "1.0.0",
            "profile": "test_profile",
        }

        with open(project1_dir / "dbt_project.yml", "w") as f:
            yaml.dump(dbt_project, f)

        runner = CliRunner()
        with patch(
            "dbt_projects_cli.core.project_discovery.ProjectDiscovery"
        ) as mock_discovery:
            # Mock project discovery
            mock_instance = mock_discovery.return_value
            mock_instance.root_path = tmp_path
            mock_instance.discover_all_projects.return_value = {
                "packages": [
                    {"name": "project1", "path": "project1"},
                ],
                "fabrics": [],
            }

            result = runner.invoke(utils, ["clean"])

        assert result.exit_code == 0
        assert "Cleaning dbt projects..." in result.output
        assert "No directories to clean" in result.output

    def test_clean_projects_empty(self, tmp_path):
        """Test cleaning when no projects exist."""
        runner = CliRunner()
        with patch(
            "dbt_projects_cli.core.project_discovery.ProjectDiscovery"
        ) as mock_discovery:
            # Mock project discovery to return empty results
            mock_instance = mock_discovery.return_value
            mock_instance.root_path = tmp_path
            mock_instance.discover_all_projects.return_value = {
                "packages": [],
                "fabrics": [],
            }

            result = runner.invoke(utils, ["clean"])

        assert result.exit_code == 0
        assert "Cleaning dbt projects..." in result.output
        assert "No directories to clean" in result.output

    def test_clean_projects_with_fabric_projects(self, tmp_path):
        """Test cleaning both packages and fabric projects."""
        # Create test package project
        package_dir = self.create_test_dbt_project(tmp_path, "package1")

        # Create test fabric project
        fabric_dir = tmp_path / "fabric1"
        fabric_dir.mkdir()

        # Create fabric.yml
        fabric_config = {
            "fabric": {"name": "fabric1"},
            "projects": {"default": {"name": "default"}},
        }

        with open(fabric_dir / "fabric.yml", "w") as f:
            yaml.dump(fabric_config, f)

        # Create directories to clean in fabric
        fabric_target = fabric_dir / "target"
        fabric_target.mkdir()
        with open(fabric_target / "profiles.yml", "w") as f:
            f.write("test: config")

        runner = CliRunner()
        with patch(
            "dbt_projects_cli.core.project_discovery.ProjectDiscovery"
        ) as mock_discovery:
            # Mock project discovery
            mock_instance = mock_discovery.return_value
            mock_instance.root_path = tmp_path
            mock_instance.discover_all_projects.return_value = {
                "packages": [
                    {"name": "package1", "path": "package1"},
                ],
                "fabrics": [
                    {"name": "fabric1", "path": "fabric1"},
                ],
            }

            result = runner.invoke(utils, ["clean"])

        assert result.exit_code == 0
        assert "Cleaning dbt projects..." in result.output
        assert "Successfully cleaned" in result.output

        # Check that directories were cleaned
        assert not (package_dir / "target").exists()
        assert not (package_dir / "dbt_packages").exists()
        assert not (fabric_dir / "target").exists()

    def test_validate_projects_with_fabrics(self, tmp_path):
        """Test validating both packages and fabric projects."""
        runner = CliRunner()
        with patch(
            "dbt_projects_cli.core.project_discovery.ProjectDiscovery"
        ) as mock_discovery:
            # Mock project discovery
            mock_instance = mock_discovery.return_value
            mock_instance.discover_all_projects.return_value = {
                "packages": [
                    {
                        "name": "package1",
                        "model_count": 2,
                        "config": {"profile": "test_profile"},
                    }
                ],
                "fabrics": [
                    {
                        "name": "fabric1",
                        "model_count": 0,
                        "config": {"databricks": {"host": "test.com"}},
                    }
                ],
            }

            result = runner.invoke(utils, ["validate"])

        assert result.exit_code == 0
        assert "Validating dbt projects..." in result.output
        assert "1/2 projects validated successfully" in result.output
