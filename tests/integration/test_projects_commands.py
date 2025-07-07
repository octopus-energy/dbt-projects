"""
Integration tests for projects CLI commands.

Tests the CLI commands end-to-end with real dbt projects.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml
from click.testing import CliRunner

from dbt_projects_cli.commands.projects import projects


class TestProjectsCommands:
    """Test projects CLI commands."""

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
        
        # Create models directory with some test models
        models_dir = project_dir / "models"
        models_dir.mkdir()
        
        with open(models_dir / "test_model.sql", "w") as f:
            f.write("select 1 as id")
        
        # Create macros directory with some test macros
        macros_dir = project_dir / "macros"
        macros_dir.mkdir()
        
        with open(macros_dir / "test_macro.sql", "w") as f:
            f.write("{% macro test_macro() %} select 1 {% endmacro %}")
        
        # Create tests directory
        tests_dir = project_dir / "tests"
        tests_dir.mkdir()
        
        with open(tests_dir / "test_check.sql", "w") as f:
            f.write("select count(*) from {{ ref('test_model') }}")
        
        return project_dir

    def create_test_fabric_project(self, tmp_path, fabric_name="test_fabric"):
        """Create a minimal fabric configuration for testing."""
        fabric_dir = tmp_path / fabric_name
        fabric_dir.mkdir()
        
        # Create fabric.yml
        fabric_config = {
            "fabric": {
                "name": fabric_name,
                "description": "Test fabric",
                "version": "1.0.0"
            },
            "databricks": {
                "host": "test.cloud.databricks.com",
                "auth_type": "oauth"
            },
            "projects": {
                "default": {
                    "name": "default",
                    "description": "Default project",
                    "schema": "test_schema",
                    "packages": []
                }
            }
        }
        
        with open(fabric_dir / "fabric.yml", "w") as f:
            yaml.dump(fabric_config, f)
        
        return fabric_dir

    def test_projects_command_group(self):
        """Test that projects command group is accessible."""
        runner = CliRunner()
        result = runner.invoke(projects, ["--help"])

        assert result.exit_code == 0
        assert "Commands for managing dbt projects" in result.output
        assert "list" in result.output
        assert "info" in result.output

    def test_list_projects_with_packages_and_fabrics(self, tmp_path):
        """Test listing all projects including both packages and fabrics."""
        # Create test projects
        project1_dir = self.create_test_dbt_project(tmp_path, "project1")
        project2_dir = self.create_test_dbt_project(tmp_path, "project2") 
        fabric1_dir = self.create_test_fabric_project(tmp_path, "fabric1")
        
        runner = CliRunner()
        with patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery") as mock_discovery:
            # Mock project discovery
            mock_instance = mock_discovery.return_value
            mock_instance.discover_all_projects.return_value = {
                "packages": [
                    {
                        "name": "project1",
                        "path": str(project1_dir),
                        "model_count": 1,
                        "macro_count": 1,
                        "test_count": 1,
                    },
                    {
                        "name": "project2", 
                        "path": str(project2_dir),
                        "model_count": 1,
                        "macro_count": 1,
                        "test_count": 1,
                    }
                ],
                "fabrics": [
                    {
                        "name": "fabric1",
                        "path": str(fabric1_dir),
                        "model_count": 0,
                        "macro_count": 0,
                        "test_count": 0,
                    }
                ]
            }
            
            result = runner.invoke(projects, ["list"])

        assert result.exit_code == 0
        assert "All dbt Projects" in result.output
        assert "project1" in result.output
        assert "project2" in result.output
        assert "fabric1" in result.output
        assert "Packages" in result.output
        assert "Fabrics" in result.output

    def test_list_projects_empty(self, tmp_path):
        """Test listing projects when no projects exist."""
        runner = CliRunner()
        with patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery") as mock_discovery:
            # Mock project discovery to return empty results
            mock_instance = mock_discovery.return_value
            mock_instance.discover_all_projects.return_value = {
                "packages": [],
                "fabrics": []
            }
            
            result = runner.invoke(projects, ["list"])

        assert result.exit_code == 0
        assert "All dbt Projects" in result.output

    def test_project_info_exists(self, tmp_path):
        """Test getting information about an existing project."""
        # Create a test project
        project_dir = self.create_test_dbt_project(tmp_path, "test_project")
        
        runner = CliRunner()
        with patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery") as mock_discovery:
            # Mock project discovery
            mock_instance = mock_discovery.return_value
            
            # Create a mock project object
            mock_project = MagicMock()
            mock_project.name = "test_project"
            mock_project.project_type = "package"
            mock_project.path = str(project_dir)
            mock_project.model_count = 1
            mock_project.macro_count = 1
            mock_project.test_count = 1
            mock_project.config = {
                "profile": "test_profile",
                "version": "1.0.0"
            }
            
            mock_instance.get_project_by_name.return_value = mock_project
            
            result = runner.invoke(projects, ["info", "test_project"])

        assert result.exit_code == 0
        assert "Project: test_project" in result.output
        assert "Type: package" in result.output
        assert "Models: 1" in result.output
        assert "Macros: 1" in result.output
        assert "Tests: 1" in result.output
        assert "Profile: test_profile" in result.output
        assert "Version: 1.0.0" in result.output

    def test_project_info_not_exists(self, tmp_path):
        """Test getting information about a non-existent project."""
        runner = CliRunner()
        with patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery") as mock_discovery:
            # Mock project discovery to return None
            mock_instance = mock_discovery.return_value
            mock_instance.get_project_by_name.return_value = None
            
            result = runner.invoke(projects, ["info", "nonexistent_project"])

        assert result.exit_code == 0
        assert "Project 'nonexistent_project' not found" in result.output

    def test_project_info_missing_optional_fields(self, tmp_path):
        """Test getting information about a project with minimal config."""
        # Create a test project
        project_dir = self.create_test_dbt_project(tmp_path, "minimal_project")
        
        runner = CliRunner()
        with patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery") as mock_discovery:
            # Mock project discovery
            mock_instance = mock_discovery.return_value
            
            # Create a mock project object with minimal config
            mock_project = MagicMock()
            mock_project.name = "minimal_project"
            mock_project.project_type = "package"
            mock_project.path = str(project_dir)
            mock_project.model_count = 0
            mock_project.macro_count = 0
            mock_project.test_count = 0
            mock_project.config = {}  # Empty config
            
            mock_instance.get_project_by_name.return_value = mock_project
            
            result = runner.invoke(projects, ["info", "minimal_project"])

        assert result.exit_code == 0
        assert "Project: minimal_project" in result.output
        assert "Type: package" in result.output
        assert "Models: 0" in result.output
        assert "Macros: 0" in result.output
        assert "Tests: 0" in result.output
        # Should not show profile or version lines if not present
        assert "Profile:" not in result.output
        assert "Version:" not in result.output

    def test_list_projects_with_fabric_only(self, tmp_path):
        """Test listing projects when only fabrics exist."""
        # Create test fabric
        fabric1_dir = self.create_test_fabric_project(tmp_path, "fabric1")
        
        runner = CliRunner()
        with patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery") as mock_discovery:
            # Mock project discovery
            mock_instance = mock_discovery.return_value
            mock_instance.discover_all_projects.return_value = {
                "packages": [],
                "fabrics": [
                    {
                        "name": "fabric1",
                        "path": str(fabric1_dir),
                        "model_count": 0,
                        "macro_count": 0,
                        "test_count": 0,
                    }
                ]
            }
            
            result = runner.invoke(projects, ["list"])

        assert result.exit_code == 0
        assert "All dbt Projects" in result.output
        assert "fabric1" in result.output
        assert "Fabrics" in result.output

    def test_list_projects_with_packages_only(self, tmp_path):
        """Test listing projects when only packages exist."""
        # Create test package
        project1_dir = self.create_test_dbt_project(tmp_path, "project1")
        
        runner = CliRunner()
        with patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery") as mock_discovery:
            # Mock project discovery
            mock_instance = mock_discovery.return_value
            mock_instance.discover_all_projects.return_value = {
                "packages": [
                    {
                        "name": "project1",
                        "path": str(project1_dir),
                        "model_count": 1,
                        "macro_count": 1,
                        "test_count": 1,
                    }
                ],
                "fabrics": []
            }
            
            result = runner.invoke(projects, ["list"])

        assert result.exit_code == 0
        assert "All dbt Projects" in result.output
        assert "project1" in result.output
        assert "Packages" in result.output
