"""
Unit tests for the utils command module.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from dbt_projects_cli.commands.utils import clean, utils, validate


class TestUtilsCommand:
    """Test cases for utils command functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    def test_utils_command_group_exists(self):
        """Test that the utils command group exists."""
        result = self.runner.invoke(utils, ["--help"])
        assert result.exit_code == 0
        assert "Utility commands for dbt projects" in result.output

    @patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery")
    def test_validate_projects_all_valid(self, mock_discovery_class):
        """Test validating projects when all are valid."""
        mock_discovery = MagicMock()
        mock_discovery_class.return_value = mock_discovery
        mock_discovery.discover_all_projects.return_value = {
            "packages": [
                {
                    "name": "project1",
                    "model_count": 5,
                    "config": {"profile": "default"},
                },
                {
                    "name": "project2",
                    "model_count": 3,
                    "config": {"profile": "test"},
                },
            ]
        }

        result = self.runner.invoke(validate)

        assert result.exit_code == 0
        assert "Validating dbt projects..." in result.output
        assert "2/2 projects validated successfully" in result.output
        mock_discovery.discover_all_projects.assert_called_once()

    @patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery")
    def test_validate_projects_some_invalid(self, mock_discovery_class):
        """Test validating projects when some are invalid."""
        mock_discovery = MagicMock()
        mock_discovery_class.return_value = mock_discovery
        mock_discovery.discover_all_projects.return_value = {
            "packages": [
                {
                    "name": "valid_project",
                    "model_count": 5,
                    "config": {"profile": "default"},
                },
                {
                    "name": "invalid_project",
                    "model_count": 0,
                    "config": {},
                },
                {
                    "name": "no_profile_project",
                    "model_count": 2,
                    "config": {},
                },
            ]
        }

        result = self.runner.invoke(validate)

        assert result.exit_code == 0
        assert "Validating dbt projects..." in result.output
        assert "1/3 projects validated successfully" in result.output
        assert "Warning: No profile specified" in result.output
        assert "Info: No models found" in result.output
        mock_discovery.discover_all_projects.assert_called_once()

    @patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery")
    @patch("shutil.rmtree")
    def test_clean_projects_with_dirs_to_clean(self, mock_rmtree, mock_discovery_class):
        """Test cleaning projects when there are directories to clean."""
        mock_discovery = MagicMock()
        mock_discovery_class.return_value = mock_discovery
        mock_discovery.root_path = Path("/test")
        mock_discovery.discover_all_projects.return_value = {
            "packages": [
                {"name": "project1", "path": "project1"},
                {"name": "project2", "path": "project2"},
            ]
        }

        # Mock directory existence
        def mock_exists():
            return True

        with patch("pathlib.Path.exists", side_effect=mock_exists):
            result = self.runner.invoke(clean)

        assert result.exit_code == 0
        assert "Cleaning dbt projects..." in result.output
        assert "Successfully cleaned" in result.output
        # Should call rmtree for each existing directory
        assert mock_rmtree.call_count > 0
        mock_discovery.discover_all_projects.assert_called_once()

    @patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery")
    @patch("shutil.rmtree")
    def test_clean_projects_no_dirs_to_clean(self, mock_rmtree, mock_discovery_class):
        """Test cleaning projects when there are no directories to clean."""
        mock_discovery = MagicMock()
        mock_discovery_class.return_value = mock_discovery
        mock_discovery.root_path = Path("/test")
        mock_discovery.discover_all_projects.return_value = {
            "packages": [
                {"name": "project1", "path": "project1"},
            ]
        }

        # Mock no directories exist
        with patch("pathlib.Path.exists", return_value=False):
            result = self.runner.invoke(clean)

        assert result.exit_code == 0
        assert "Cleaning dbt projects..." in result.output
        assert "No directories to clean" in result.output
        # Should not call rmtree
        mock_rmtree.assert_not_called()
        mock_discovery.discover_all_projects.assert_called_once()

    @patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery")
    @patch("shutil.rmtree")
    def test_clean_projects_with_errors(self, mock_rmtree, mock_discovery_class):
        """Test cleaning projects when there are errors during cleanup."""
        mock_discovery = MagicMock()
        mock_discovery_class.return_value = mock_discovery
        mock_discovery.root_path = Path("/test")
        mock_discovery.discover_all_projects.return_value = {
            "packages": [
                {"name": "project1", "path": "project1"},
            ]
        }

        # Mock rmtree to raise an exception
        mock_rmtree.side_effect = PermissionError("Permission denied")

        # Mock directory existence
        with patch("pathlib.Path.exists", return_value=True):
            result = self.runner.invoke(clean)

        assert result.exit_code == 0
        assert "Cleaning dbt projects..." in result.output
        assert "Failed to clean" in result.output
        assert "Permission denied" in result.output
        mock_discovery.discover_all_projects.assert_called_once()
