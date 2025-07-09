"""
Unit tests for the models command module.
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from dbt_projects_cli.commands.models import analyze
from dbt_projects_cli.commands.models import list as list_models
from dbt_projects_cli.commands.models import models


class TestModelsCommand:
    """Test cases for models command functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    def test_models_command_group_exists(self):
        """Test that the models command group exists."""
        result = self.runner.invoke(models, ["--help"])
        assert result.exit_code == 0
        assert "Commands for managing dbt models" in result.output

    @patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery")
    def test_list_models_specific_project_with_models(self, mock_discovery_class):
        """Test listing models for a specific project that has models."""
        mock_discovery = MagicMock()
        mock_discovery_class.return_value = mock_discovery
        mock_discovery.root_path = Path("/test")
        mock_discovery.list_models_in_project.return_value = [
            Path("/test/models/model1.sql"),
            Path("/test/models/model2.sql"),
        ]

        result = self.runner.invoke(list_models, ["--project", "test_project"])

        assert result.exit_code == 0
        assert "Models in project 'test_project'" in result.output
        assert "models/model1.sql" in result.output
        assert "models/model2.sql" in result.output
        mock_discovery.list_models_in_project.assert_called_once_with("test_project")

    @patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery")
    def test_list_models_specific_project_no_models(self, mock_discovery_class):
        """Test listing models for a project with no models."""
        mock_discovery = MagicMock()
        mock_discovery_class.return_value = mock_discovery
        mock_discovery.list_models_in_project.return_value = []

        result = self.runner.invoke(list_models, ["--project", "empty_project"])

        assert result.exit_code == 0
        assert "No models found for project 'empty_project'" in result.output
        mock_discovery.list_models_in_project.assert_called_once_with("empty_project")

    @patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery")
    def test_list_all_models(self, mock_discovery_class):
        """Test listing all models across all projects."""
        mock_discovery = MagicMock()
        mock_discovery_class.return_value = mock_discovery
        mock_discovery.root_path = Path("/test")
        mock_discovery.discover_all_projects.return_value = {
            "packages": [{"name": "project1"}, {"name": "project2"}],
            "fabrics": [{"name": "fabric1"}],
        }

        def mock_list_models(project_name):
            if project_name == "project1":
                return [Path("/test/project1/models/model1.sql")]
            elif project_name == "project2":
                return [Path("/test/project2/models/model2.sql")]
            elif project_name == "fabric1":
                return []
            return []

        mock_discovery.list_models_in_project.side_effect = mock_list_models

        result = self.runner.invoke(list_models)

        assert result.exit_code == 0
        assert "All dbt Models" in result.output
        mock_discovery.discover_all_projects.assert_called_once()

    def test_analyze_model_file_exists(self):
        """Test analyzing a model file that exists."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write(
                """
-- Simple test model
SELECT
    id,
    name,
    email
FROM {{ ref('source_table') }}
WHERE status = 'active'
GROUP BY id, name, email
"""
            )
            f.flush()

            result = self.runner.invoke(analyze, [f.name])

            assert result.exit_code == 0
            assert f"Analyzing model: {Path(f.name).name}" in result.output
            assert "Lines of code:" in result.output
            assert "✓ Contains SELECT statements" in result.output
            assert "✓ Contains FROM clauses" in result.output
            assert "✓ Contains WHERE conditions" in result.output
            assert "✓ Contains GROUP BY clauses" in result.output
            assert "✓ Contains dbt Jinja templating" in result.output
            assert "✓ Contains dbt ref() functions" in result.output

        # Clean up
        Path(f.name).unlink()

    def test_analyze_model_file_not_exists(self):
        """Test analyzing a model file that doesn't exist."""
        result = self.runner.invoke(analyze, ["/nonexistent/model.sql"])

        assert result.exit_code == 0
        assert "Model file '/nonexistent/model.sql' not found" in result.output

    def test_analyze_model_with_joins_and_sources(self):
        """Test analyzing a complex model with JOINs and source() functions."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write(
                """
SELECT
    u.id,
    u.name,
    p.title
FROM {{ source('raw', 'users') }} u
LEFT JOIN {{ ref('products') }} p ON u.product_id = p.id
WHERE u.created_at > '2023-01-01'
"""
            )
            f.flush()

            result = self.runner.invoke(analyze, [f.name])

            assert result.exit_code == 0
            assert "✓ Contains SELECT statements" in result.output
            assert "✓ Contains FROM clauses" in result.output
            assert "✓ Contains JOIN operations" in result.output
            assert "✓ Contains WHERE conditions" in result.output
            assert "✓ Contains dbt Jinja templating" in result.output
            assert "✓ Contains dbt ref() functions" in result.output
            assert "✓ Contains dbt source() functions" in result.output

        # Clean up
        Path(f.name).unlink()

    def test_analyze_simple_model(self):
        """Test analyzing a simple model without complex features."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("SELECT 1 as simple_column")
            f.flush()

            result = self.runner.invoke(analyze, [f.name])

            assert result.exit_code == 0
            assert "✓ Contains SELECT statements" in result.output
            # Should not contain other complex features
            assert "✓ Contains JOIN operations" not in result.output
            assert "✓ Contains WHERE conditions" not in result.output
            assert "✓ Contains GROUP BY clauses" not in result.output
            assert "✓ Contains dbt Jinja templating" not in result.output

        # Clean up
        Path(f.name).unlink()
