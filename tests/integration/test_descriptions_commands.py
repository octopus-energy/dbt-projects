"""
Integration tests for descriptions CLI commands.

Tests the CLI commands end-to-end with real dbt projects.
"""

from unittest.mock import patch

import yaml
from click.testing import CliRunner

from dbt_projects_cli.commands.descriptions import descriptions


class TestDescriptionsCommands:
    """Test descriptions CLI commands."""

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
            "analysis-paths": ["analysis"],
            "test-paths": ["tests"],
            "seed-paths": ["data"],
            "macro-paths": ["macros"],
            "snapshot-paths": ["snapshots"],
            "target-path": "target",
            "clean-targets": ["target", "dbt_packages"],
        }

        with open(project_dir / "dbt_project.yml", "w") as f:
            yaml.dump(dbt_project, f)

        # Create models directory and a test model
        models_dir = project_dir / "models"
        models_dir.mkdir()

        # Create a simple model
        test_model = """
select
    id,
    name,
    email,
    created_at
from {{ source('raw', 'users') }}
where status = 'active'
"""

        with open(models_dir / "test_model.sql", "w") as f:
            f.write(test_model)

        return project_dir

    def test_descriptions_command_group(self):
        """Test that descriptions command group is accessible."""
        runner = CliRunner()
        result = runner.invoke(descriptions, ["--help"])

        assert result.exit_code == 0
        assert "Commands for managing dbt model descriptions" in result.output
        assert "generate" in result.output
        assert "show-models" in result.output
        assert "test-pii" in result.output
        assert "test-databricks" in result.output

    def test_generate_command_missing_project(self, tmp_path):
        """Test generate command with missing project."""
        runner = CliRunner()
        with patch(
            "dbt_projects_cli.commands.descriptions.ProjectDiscovery"
        ) as mock_discovery:
            # Mock project discovery to return empty results
            mock_instance = mock_discovery.return_value
            mock_instance.get_project_by_name.return_value = None

            result = runner.invoke(
                descriptions, ["generate", "--project", "nonexistent"]
            )

        assert (
            result.exit_code == 0
        )  # The function returns instead of raising SystemExit
        assert "Project nonexistent not found" in result.output

    def test_generate_command_dry_run(self, tmp_path):
        """Test generate command in dry-run mode."""
        # Create a test dbt project
        project_dir = self.create_test_dbt_project(tmp_path)

        runner = CliRunner()
        with (
            patch(
                "dbt_projects_cli.commands.descriptions.ProjectDiscovery"
            ) as mock_discovery,
            patch(
                "dbt_projects_cli.commands.descriptions.LLMDescriptionGenerator"
            ) as mock_generator,
        ):

            # Mock project discovery
            mock_instance = mock_discovery.return_value
            from dbt_projects_cli.core.project_discovery import DbtProject

            dbt_project = DbtProject(
                name="test_project", path=project_dir, config={}, project_type="package"
            )
            mock_instance.get_project_by_name.return_value = dbt_project
            mock_instance.list_models_in_project.return_value = [
                project_dir / "models" / "test_model.sql"
            ]

            # Mock LLM generator
            mock_gen_instance = mock_generator.return_value
            mock_gen_instance.model = "gpt-3.5-turbo"

            result = runner.invoke(
                descriptions,
                [
                    "generate",
                    "--project",
                    "test_project",
                    "--dry-run",
                    "--provider",
                    "openai",
                ],
            )

        assert result.exit_code == 0
        assert "Processing 1 model(s) in project" in result.output

    def test_generate_command_no_models(self, tmp_path):
        """Test generate command when no models are found."""
        # Create a test dbt project without models
        project_dir = self.create_test_dbt_project(tmp_path)
        (project_dir / "models" / "test_model.sql").unlink()  # Remove the model

        runner = CliRunner()
        with (
            patch(
                "dbt_projects_cli.commands.descriptions.ProjectDiscovery"
            ) as mock_discovery,
            patch(
                "dbt_projects_cli.commands.descriptions.LLMDescriptionGenerator"
            ) as mock_generator,
        ):

            # Mock project discovery
            mock_instance = mock_discovery.return_value
            from dbt_projects_cli.core.project_discovery import DbtProject

            dbt_project = DbtProject(
                name="test_project", path=project_dir, config={}, project_type="package"
            )
            mock_instance.get_project_by_name.return_value = dbt_project
            mock_instance.list_models_in_project.return_value = []

            # Mock LLM generator
            mock_gen_instance = mock_generator.return_value
            mock_gen_instance.model = "gpt-3.5-turbo"

            result = runner.invoke(
                descriptions,
                ["generate", "--project", "test_project", "--provider", "openai"],
            )

        assert result.exit_code == 0
        assert "Processing 0 model(s) in project" in result.output

    def test_generate_command_invalid_provider(self, tmp_path):
        """Test generate command with invalid provider."""
        runner = CliRunner()
        result = runner.invoke(
            descriptions,
            ["generate", "--project", "test_project", "--provider", "invalid_provider"],
        )

        assert result.exit_code != 0
        assert "Invalid value for '--provider'" in result.output

    def test_show_models_command(self, tmp_path):
        """Test show-models command."""
        runner = CliRunner()
        with patch(
            "dbt_projects_cli.commands.descriptions.LLMDescriptionGenerator"
        ) as mock_generator:
            # Mock LLM generator
            mock_gen_instance = mock_generator.return_value
            mock_gen_instance.config = {
                "providers": {"openai": {"default_model": "gpt-3.5-turbo"}}
            }

            result = runner.invoke(descriptions, ["show-models"])

        assert result.exit_code == 0
        assert "LLM Configuration" in result.output

    def test_test_pii_command_basic(self, tmp_path):
        """Test test-pii command basic functionality."""
        runner = CliRunner()
        result = runner.invoke(descriptions, ["test-pii"])

        assert result.exit_code == 0
        assert "PII Protection Test" in result.output

    def test_test_pii_command_schema_only(self, tmp_path):
        """Test test-pii command with schema-only mode."""
        runner = CliRunner()
        result = runner.invoke(descriptions, ["test-pii", "--level", "schema_only"])

        assert result.exit_code == 0
        assert "PII Protection Test" in result.output
        assert "schema_only" in result.output

    def test_test_databricks_command_no_project(self, tmp_path):
        """Test test-databricks command without project."""
        runner = CliRunner()
        with patch(
            "dbt_projects_cli.commands.descriptions.create_databricks_connector"
        ) as mock_connector:
            # Mock databricks connector creation failure (no project found)
            mock_connector.return_value = None

            result = runner.invoke(descriptions, ["test-databricks"])

        assert result.exit_code == 0  # Command runs but may fail connection
        assert "Testing Databricks Connection" in result.output

    def test_test_databricks_command_with_project(self, tmp_path):
        """Test test-databricks command with project."""
        # Create a test dbt project with profiles.yml
        project_dir = self.create_test_dbt_project(tmp_path)
        profiles_yml = project_dir / "profiles.yml"
        profiles_yml.write_text(
            "test_profile:\n  target: dev\n  outputs:\n    dev:\n      type: databricks"
        )

        runner = CliRunner()
        with patch(
            "dbt_projects_cli.commands.descriptions.create_databricks_connector"
        ) as mock_connector:
            # Mock databricks connector creation failure
            mock_connector.return_value = None

            # Change to the project directory for the command to find it
            with runner.isolated_filesystem(temp_dir=project_dir):
                result = runner.invoke(descriptions, ["test-databricks"])

        assert result.exit_code == 0
        assert "Testing Databricks Connection" in result.output

    def test_generate_command_help_message(self):
        """Test generate command shows help message."""
        runner = CliRunner()
        result = runner.invoke(descriptions, ["generate", "--help"])

        assert result.exit_code == 0
        assert (
            "Generate or expand descriptions for dbt models using LLMs" in result.output
        )
        assert "--project" in result.output
        assert "--provider" in result.output
        assert "--dry-run" in result.output
        assert "--expand" in result.output

    def test_generate_command_project_not_found(self, tmp_path):
        """Test generate command when project is not found."""
        runner = CliRunner()
        with patch(
            "dbt_projects_cli.commands.descriptions.ProjectDiscovery"
        ) as mock_discovery:
            # Mock project discovery to return empty results
            mock_instance = mock_discovery.return_value
            mock_instance.get_project_by_name.return_value = None

            result = runner.invoke(
                descriptions,
                [
                    "generate",
                    "--project",
                    "nonexistent_project",
                    "--provider",
                    "openai",
                ],
            )

        assert (
            result.exit_code == 0
        )  # The function returns instead of raising SystemExit
        assert "Project nonexistent_project not found" in result.output

    def test_models_command_placeholder(self):
        """Test models subcommand placeholder."""
        runner = CliRunner()
        result = runner.invoke(descriptions, ["models", "--help"])

        assert result.exit_code == 0
        assert "Show available LLM models and current configuration" in result.output
