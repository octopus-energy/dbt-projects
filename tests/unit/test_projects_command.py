"""
Unit tests for the projects command module.
"""

from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from dbt_projects_cli.commands.projects import info
from dbt_projects_cli.commands.projects import list as list_projects
from dbt_projects_cli.commands.projects import projects


class TestProjectsCommand:
    """Test cases for projects command functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    def test_projects_command_group_exists(self):
        """Test that the projects command group exists."""
        result = self.runner.invoke(projects, ["--help"])
        assert result.exit_code == 0
        assert "Commands for managing dbt projects" in result.output

    @patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery")
    def test_list_projects(self, mock_discovery_class):
        """Test listing all projects."""
        mock_discovery = MagicMock()
        mock_discovery_class.return_value = mock_discovery
        mock_discovery.discover_all_projects.return_value = {
            "packages": [
                {
                    "name": "project1",
                    "path": "/projects/project1",
                    "model_count": 5,
                    "macro_count": 3,
                    "test_count": 2,
                },
                {
                    "name": "project2",
                    "path": "/projects/project2",
                    "model_count": 0,
                },
            ]
        }

        result = self.runner.invoke(list_projects)

        assert result.exit_code == 0
        assert "All dbt Projects" in result.output
        assert "project1" in result.output
        assert "project2" in result.output
        mock_discovery.discover_all_projects.assert_called_once()

    @patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery")
    def test_project_info_exists(self, mock_discovery_class):
        """Test retrieving information about a specific existing project."""
        mock_discovery = MagicMock()
        mock_discovery_class.return_value = mock_discovery
        mock_project = MagicMock()
        mock_project.name = "project1"
        mock_project.project_type = "package"
        mock_project.path = "/projects/project1"
        mock_project.model_count = 5
        mock_project.macro_count = 3
        mock_project.test_count = 2
        mock_project.config = {"profile": "default", "version": "1.0.0"}

        mock_discovery.get_project_by_name.return_value = mock_project

        result = self.runner.invoke(info, ["project1"])

        assert result.exit_code == 0
        assert "Project: project1" in result.output
        assert "Type: package" in result.output
        mock_discovery.get_project_by_name.assert_called_once_with("project1")

    @patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery")
    def test_project_info_not_exists(self, mock_discovery_class):
        """Test the output when a project does not exist."""
        mock_discovery = MagicMock()
        mock_discovery_class.return_value = mock_discovery
        mock_discovery.get_project_by_name.return_value = None

        result = self.runner.invoke(info, ["nonexistent_project"])

        assert result.exit_code == 0
        assert "Project 'nonexistent_project' not found" in result.output
        mock_discovery.get_project_by_name.assert_called_once_with(
            "nonexistent_project"
        )
