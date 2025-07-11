"""
Unit tests for the descriptions command module.
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from dbt_projects_cli.commands.descriptions import generate


class TestDescriptionsCommand:
    """Test cases for descriptions command."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.runner = CliRunner()

    def test_generate_command_exists(self):
        """Test that the generate command exists and is callable."""
        # Just test that the command is importable and can be invoked
        result = self.runner.invoke(generate, ["--help"])
        assert "Generate or expand descriptions" in result.output

    def test_generate_command_missing_project(self):
        """Test generate command with missing required project parameter."""
        result = self.runner.invoke(generate, [])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "Error" in result.output

    def test_generate_command_options(self):
        """Test that the generate command accepts expected options."""
        result = self.runner.invoke(generate, ["--help"])

        # Check that important options are documented
        assert "--project" in result.output or "-p" in result.output
        assert "--model" in result.output or "-m" in result.output
        assert "--provider" in result.output
        assert "--dry-run" in result.output
        assert "--interactive" in result.output

    def test_generate_command_invalid_provider(self):
        """Test generate command with invalid provider option."""
        result = self.runner.invoke(
            generate, ["-p", "test_project", "--provider", "invalid_provider"]
        )

        assert result.exit_code != 0
        assert "Invalid value" in result.output or "not supported" in result.output

    def test_generate_command_help_message(self):
        """Test that help message contains expected information."""
        result = self.runner.invoke(generate, ["--help"])

        assert result.exit_code == 0
        assert "project" in result.output.lower()
        assert "model" in result.output.lower()
        assert "provider" in result.output.lower()
        assert "dry-run" in result.output.lower()

    @patch("dbt_projects_cli.commands.descriptions.ProjectDiscovery")
    def test_generate_command_project_not_found(self, mock_discovery_class):
        """Test generate command when project is not found."""
        mock_discovery = MagicMock()
        mock_discovery_class.return_value = mock_discovery
        mock_discovery.get_project_by_name.return_value = None

        result = self.runner.invoke(generate, ["-p", "nonexistent_project"])

        assert "Project nonexistent_project not found" in result.output

    @patch("dbt_projects_cli.commands.descriptions.ProjectDiscovery")
    @patch("dbt_projects_cli.commands.descriptions.LLMDescriptionGenerator")
    def test_generate_command_llm_setup_error(
        self, mock_llm_class, mock_discovery_class
    ):
        """Test generate command when LLM setup fails."""
        # Mock project discovery to return a valid project
        mock_discovery = MagicMock()
        mock_discovery_class.return_value = mock_discovery

        mock_project = MagicMock()
        mock_project.name = "test_project"
        mock_project.path = self.temp_dir
        mock_discovery.get_project_by_name.return_value = mock_project

        # Mock LLM setup to fail
        mock_llm_class.side_effect = ValueError("API key not found")

        result = self.runner.invoke(generate, ["-p", "test_project"])

        assert "Error setting up LLM provider" in result.output
        assert "API key" in result.output

    @patch("dbt_projects_cli.commands.descriptions.ProjectDiscovery")
    @patch("dbt_projects_cli.commands.descriptions.LLMDescriptionGenerator")
    @patch("dbt_projects_cli.commands.descriptions.ModelParser")
    def test_generate_command_no_models_found(
        self, mock_parser_class, mock_llm_class, mock_discovery_class
    ):
        """Test generate command when no models are found."""
        # Mock project discovery
        mock_discovery = MagicMock()
        mock_discovery_class.return_value = mock_discovery

        mock_project = MagicMock()
        mock_project.name = "test_project"
        mock_project.path = self.temp_dir
        mock_discovery.get_project_by_name.return_value = mock_project
        mock_discovery.list_models_in_project.return_value = []

        # Mock LLM generator
        mock_llm = MagicMock()
        mock_llm_class.return_value = mock_llm
        mock_llm.model = "gpt-4"

        result = self.runner.invoke(generate, ["-p", "test_project"])

        assert result.exit_code == 0
        assert "Processing 0 model(s)" in result.output

    @patch("dbt_projects_cli.commands.descriptions.ProjectDiscovery")
    @patch("dbt_projects_cli.commands.descriptions.LLMDescriptionGenerator")
    @patch("dbt_projects_cli.commands.descriptions.ModelParser")
    def test_generate_command_specific_model_not_found(
        self, mock_parser_class, mock_llm_class, mock_discovery_class
    ):
        """Test generate command when specific model is not found."""
        # Mock project discovery
        mock_discovery = MagicMock()
        mock_discovery_class.return_value = mock_discovery

        mock_project = MagicMock()
        mock_project.name = "test_project"
        mock_discovery.get_project_by_name.return_value = mock_project
        mock_discovery.list_models_in_project.return_value = []  # No models found

        # Mock LLM generator
        mock_llm = MagicMock()
        mock_llm_class.return_value = mock_llm

        result = self.runner.invoke(
            generate, ["-p", "test_project", "-m", "nonexistent_model"]
        )

        assert "Model 'nonexistent_model' not found" in result.output

    @patch("dbt_projects_cli.commands.descriptions.LLMDescriptionGenerator")
    def test_show_models_command_with_config(self, mock_llm_class):
        """Test show_models command with configuration available."""
        from dbt_projects_cli.commands.descriptions import show_models

        # Mock LLM generator with config
        mock_generator = MagicMock()
        mock_llm_class.return_value = mock_generator
        mock_generator.config = {
            "providers": {
                "openai": {
                    "default_model": "gpt-4",
                    "available_models": ["gpt-4", "gpt-3.5-turbo"],
                },
                "anthropic": {
                    "default_model": "claude-3-5-sonnet-20241022",
                    "available_models": [
                        "claude-3-5-sonnet-20241022",
                        "claude-3-haiku-20240307",
                    ],
                },
            }
        }

        result = self.runner.invoke(show_models)

        assert result.exit_code == 0
        assert "LLM Configuration" in result.output
        assert "openai" in result.output
        assert "anthropic" in result.output
        assert "gpt-4" in result.output

    @patch("dbt_projects_cli.commands.descriptions.LLMDescriptionGenerator")
    @patch("dbt_projects_cli.commands.descriptions.os")
    def test_show_models_command_no_config(self, mock_os, mock_llm_class):
        """Test show_models command without configuration."""
        from dbt_projects_cli.commands.descriptions import show_models
        from dbt_projects_cli.integrations.llm import LLMProvider

        # Mock LLM generator without config
        mock_generator = MagicMock()
        mock_llm_class.return_value = mock_generator
        mock_generator.config = None

        # Mock the DEFAULT_MODELS class attribute properly
        mock_llm_class.DEFAULT_MODELS = {
            LLMProvider.OPENAI: "gpt-4",
            LLMProvider.ANTHROPIC: "claude-3-5-sonnet-20241022",
        }

        # Mock os.getenv to return None (no env vars set)
        mock_os.getenv.return_value = None

        result = self.runner.invoke(show_models)

        assert result.exit_code == 0
        assert "Default Models (fallback)" in result.output

    @patch("dbt_projects_cli.commands.descriptions.create_pii_detector")
    def test_test_pii_command_basic(self, mock_pii_detector_factory):
        """Test test_pii command with basic functionality."""
        from dbt_projects_cli.commands.descriptions import test_pii

        # Mock PII detector
        mock_detector = MagicMock()
        mock_pii_detector_factory.return_value = mock_detector

        mock_protected_data = {
            "source_table": "test.customers",
            "sample_rows": [
                {
                    "customer_id": "HASH123",
                    "first_name": "HASH456",
                    "email_address": "HASH789",
                }
            ],
            "pii_protection": {
                "method": "hashing",
                "columns_analyzed": 6,
                "high_risk_columns": 3,
                "medium_risk_columns": 1,
                "low_risk_columns": 2,
                "protection_applied": True,
            },
        }
        mock_detector.sanitize_sample_data.return_value = mock_protected_data

        result = self.runner.invoke(test_pii, ["--level", "high"])

        assert result.exit_code == 0
        assert "PII Protection Test" in result.output
        assert "Protected Data" in result.output
        assert "Protection Summary" in result.output

    @patch("dbt_projects_cli.commands.descriptions.create_pii_detector")
    def test_test_pii_command_schema_only(self, mock_pii_detector_factory):
        """Test test_pii command with schema_only level."""
        from dbt_projects_cli.commands.descriptions import test_pii

        # Mock PII detector for schema_only mode
        mock_detector = MagicMock()
        mock_pii_detector_factory.return_value = mock_detector

        mock_protected_data = {
            "source_table": "test.customers",
            # No sample_rows for schema_only
            "pii_protection": {
                "method": "schema_only",
                "columns_analyzed": 6,
                "protection_applied": True,
            },
        }
        mock_detector.sanitize_sample_data.return_value = mock_protected_data

        result = self.runner.invoke(test_pii, ["--level", "schema_only"])

        assert result.exit_code == 0
        assert "REMOVED - schema_only mode" in result.output

    @patch("dbt_projects_cli.commands.descriptions.create_databricks_connector")
    @patch("pathlib.Path.cwd")
    def test_test_databricks_command_success(self, mock_cwd, mock_connector_factory):
        """Test test_databricks command with successful connection."""
        from dbt_projects_cli.commands.descriptions import test_databricks

        # Mock current directory to have a dbt project
        mock_cwd.return_value = self.temp_dir

        # Create dbt_project.yml and profiles.yml
        (self.temp_dir / "dbt_project.yml").write_text("name: test_project")
        (self.temp_dir / "profiles.yml").write_text("test_profile: {}")

        # Mock successful connector
        mock_connector = MagicMock()
        mock_connector_factory.return_value = mock_connector
        mock_connector.test_connection.return_value = True
        mock_connector.list_tables.return_value = ["table1", "table2", "table3"]

        result = self.runner.invoke(test_databricks)

        assert result.exit_code == 0
        assert "Testing Databricks Connection" in result.output
        assert "Connector created successfully" in result.output
        assert "Connection test passed" in result.output
        assert "Available Tables" in result.output

    @patch("dbt_projects_cli.commands.descriptions.create_databricks_connector")
    @patch("pathlib.Path.cwd")
    def test_test_databricks_command_connection_failed(
        self, mock_cwd, mock_connector_factory
    ):
        """Test test_databricks command with failed connection."""
        from dbt_projects_cli.commands.descriptions import test_databricks

        # Mock current directory to have a dbt project
        mock_cwd.return_value = self.temp_dir

        # Create dbt_project.yml and profiles.yml
        (self.temp_dir / "dbt_project.yml").write_text("name: test_project")
        (self.temp_dir / "profiles.yml").write_text("test_profile: {}")

        # Mock failed connector
        mock_connector = MagicMock()
        mock_connector_factory.return_value = mock_connector
        mock_connector.test_connection.return_value = False

        result = self.runner.invoke(test_databricks)

        assert result.exit_code == 0
        assert "Connection test failed" in result.output

    @patch("pathlib.Path.cwd")
    def test_test_databricks_command_no_project(self, mock_cwd):
        """Test test_databricks command when no dbt project is found."""
        from dbt_projects_cli.commands.descriptions import test_databricks

        # Mock current directory without dbt project
        mock_cwd.return_value = self.temp_dir

        result = self.runner.invoke(test_databricks)

        assert result.exit_code == 0
        assert "No dbt project with profiles.yml found" in result.output
        assert "Run this command from a dbt project directory" in result.output

    @patch("dbt_projects_cli.commands.descriptions.create_databricks_connector")
    @patch("pathlib.Path.cwd")
    def test_test_databricks_command_exception(self, mock_cwd, mock_connector_factory):
        """Test test_databricks command when an exception occurs."""
        from dbt_projects_cli.commands.descriptions import test_databricks

        # Mock current directory to have a dbt project
        mock_cwd.return_value = self.temp_dir

        # Create dbt_project.yml and profiles.yml
        (self.temp_dir / "dbt_project.yml").write_text("name: test_project")
        (self.temp_dir / "profiles.yml").write_text("test_profile: {}")

        # Mock connector factory to raise exception
        mock_connector_factory.side_effect = Exception("Connection error")

        result = self.runner.invoke(test_databricks)

        assert result.exit_code == 0
        assert "Connection error" in result.output

    def test_models_command_placeholder(self):
        """Test models command (currently just passes)."""
        from dbt_projects_cli.commands.descriptions import models

        result = self.runner.invoke(models)

        assert result.exit_code == 0
        assert "LLM Configuration" in result.output

    @patch("dbt_projects_cli.commands.descriptions.ProjectDiscovery")
    @patch("dbt_projects_cli.commands.descriptions.LLMDescriptionGenerator")
    @patch("dbt_projects_cli.commands.descriptions.ModelParser")
    @patch("dbt_projects_cli.commands.descriptions.setup_logging")
    def test_generate_command_successful_dry_run(
        self,
        mock_setup_logging,
        mock_parser_class,
        mock_llm_class,
        mock_discovery_class,
    ):
        """Test successful generate command with dry run."""
        # Mock project discovery
        mock_discovery = MagicMock()
        mock_discovery_class.return_value = mock_discovery

        mock_project = MagicMock()
        mock_project.name = "test_project"
        mock_project.path = self.temp_dir
        mock_discovery.get_project_by_name.return_value = mock_project

        # Create a mock model path
        mock_model_path = MagicMock()
        mock_model_path.name = "test_model.sql"
        mock_model_path.stem = "test_model"
        mock_discovery.list_models_in_project.return_value = [mock_model_path]

        # Mock LLM generator
        mock_llm = MagicMock()
        mock_llm_class.return_value = mock_llm
        mock_llm.model = "gpt-4"

        # Mock parser and model info
        mock_parser = MagicMock()
        mock_parser_class.return_value = mock_parser

        mock_model_info = MagicMock()
        mock_model_info.name = "test_model"
        mock_model_info.sql_content = "SELECT * FROM table"
        mock_model_info.existing_description = None
        mock_model_info.columns = []
        mock_parser.parse_model.return_value = mock_model_info

        # Mock LLM descriptions result
        mock_descriptions = MagicMock()
        mock_descriptions.model_description = "Test model description"
        mock_descriptions.column_descriptions = {"col1": "Test column description"}
        mock_llm.generate_descriptions.return_value = mock_descriptions

        result = self.runner.invoke(generate, ["-p", "test_project", "--dry-run"])

        assert result.exit_code == 0
        assert "Processing 1 model(s)" in result.output
        assert "Generated descriptions for test_model" in result.output
        assert "Test model description" in result.output
        assert "Dry run - no files modified" in result.output

    @patch("dbt_projects_cli.commands.descriptions.ProjectDiscovery")
    @patch("dbt_projects_cli.commands.descriptions.LLMDescriptionGenerator")
    @patch("dbt_projects_cli.commands.descriptions.ModelParser")
    @patch("dbt_projects_cli.commands.descriptions.setup_logging")
    def test_generate_command_with_update_success(
        self,
        mock_setup_logging,
        mock_parser_class,
        mock_llm_class,
        mock_discovery_class,
    ):
        """Test successful generate command with file update."""
        # Mock project discovery
        mock_discovery = MagicMock()
        mock_discovery_class.return_value = mock_discovery

        mock_project = MagicMock()
        mock_project.name = "test_project"
        mock_project.path = self.temp_dir
        mock_discovery.get_project_by_name.return_value = mock_project

        # Create a mock model path
        mock_model_path = MagicMock()
        mock_model_path.name = "test_model.sql"
        mock_model_path.stem = "test_model"
        mock_discovery.list_models_in_project.return_value = [mock_model_path]

        # Mock LLM generator
        mock_llm = MagicMock()
        mock_llm_class.return_value = mock_llm
        mock_llm.model = "gpt-4"

        # Mock parser and model info
        mock_parser = MagicMock()
        mock_parser_class.return_value = mock_parser

        mock_model_info = MagicMock()
        mock_model_info.name = "test_model"
        mock_model_info.sql_content = "SELECT * FROM table"
        mock_model_info.existing_description = None
        mock_model_info.columns = []
        mock_parser.parse_model.return_value = mock_model_info

        # Mock LLM descriptions result
        mock_descriptions = MagicMock()
        mock_descriptions.model_description = "Test model description"
        mock_descriptions.column_descriptions = {"col1": "Test column description"}
        mock_llm.generate_descriptions.return_value = mock_descriptions

        # Mock successful update
        mock_parser.update_model_descriptions.return_value = True

        result = self.runner.invoke(generate, ["-p", "test_project"])

        assert result.exit_code == 0
        assert "Processing 1 model(s)" in result.output
        assert "Generated descriptions for test_model" in result.output
        assert "Updated descriptions for test_model" in result.output

    @patch("dbt_projects_cli.commands.descriptions.ProjectDiscovery")
    @patch("dbt_projects_cli.commands.descriptions.LLMDescriptionGenerator")
    @patch("dbt_projects_cli.commands.descriptions.ModelParser")
    @patch("dbt_projects_cli.commands.descriptions.setup_logging")
    def test_generate_command_with_update_failure(
        self,
        mock_setup_logging,
        mock_parser_class,
        mock_llm_class,
        mock_discovery_class,
    ):
        """Test generate command when file update fails."""
        # Mock project discovery
        mock_discovery = MagicMock()
        mock_discovery_class.return_value = mock_discovery

        mock_project = MagicMock()
        mock_project.name = "test_project"
        mock_project.path = self.temp_dir
        mock_discovery.get_project_by_name.return_value = mock_project

        # Create a mock model path
        mock_model_path = MagicMock()
        mock_model_path.name = "test_model.sql"
        mock_model_path.stem = "test_model"
        mock_discovery.list_models_in_project.return_value = [mock_model_path]

        # Mock LLM generator
        mock_llm = MagicMock()
        mock_llm_class.return_value = mock_llm
        mock_llm.model = "gpt-4"

        # Mock parser and model info
        mock_parser = MagicMock()
        mock_parser_class.return_value = mock_parser

        mock_model_info = MagicMock()
        mock_model_info.name = "test_model"
        mock_model_info.sql_content = "SELECT * FROM table"
        mock_parser.parse_model.return_value = mock_model_info

        # Mock LLM descriptions result
        mock_descriptions = MagicMock()
        mock_descriptions.model_description = "Test model description"
        mock_descriptions.column_descriptions = {}
        mock_llm.generate_descriptions.return_value = mock_descriptions

        # Mock failed update
        mock_parser.update_model_descriptions.return_value = False

        result = self.runner.invoke(generate, ["-p", "test_project"])

        assert result.exit_code == 0
        assert "Failed to update descriptions for test_model" in result.output

    @patch("dbt_projects_cli.commands.descriptions.ProjectDiscovery")
    @patch("dbt_projects_cli.commands.descriptions.LLMDescriptionGenerator")
    @patch("dbt_projects_cli.commands.descriptions.ModelParser")
    @patch("dbt_projects_cli.commands.descriptions.setup_logging")
    def test_generate_command_with_exception(
        self,
        mock_setup_logging,
        mock_parser_class,
        mock_llm_class,
        mock_discovery_class,
    ):
        """Test generate command when an exception occurs during processing."""
        # Mock project discovery
        mock_discovery = MagicMock()
        mock_discovery_class.return_value = mock_discovery

        mock_project = MagicMock()
        mock_project.name = "test_project"
        mock_project.path = self.temp_dir
        mock_discovery.get_project_by_name.return_value = mock_project

        # Create a mock model path
        mock_model_path = MagicMock()
        mock_model_path.name = "test_model.sql"
        mock_model_path.stem = "test_model"
        mock_discovery.list_models_in_project.return_value = [mock_model_path]

        # Mock LLM generator
        mock_llm = MagicMock()
        mock_llm_class.return_value = mock_llm
        mock_llm.model = "gpt-4"

        # Mock parser to raise exception during parse_model (this happens
        # outside the try-catch in the loop)
        mock_parser = MagicMock()
        mock_parser_class.return_value = mock_parser
        mock_parser.parse_model.side_effect = Exception("Parse error")

        result = self.runner.invoke(generate, ["-p", "test_project"])

        # The exception occurs during model parsing, before the try-catch in the loop
        # So it will exit with code 1, not 0
        assert result.exit_code == 1

    @patch("dbt_projects_cli.commands.descriptions.ProjectDiscovery")
    @patch("dbt_projects_cli.commands.descriptions.LLMDescriptionGenerator")
    @patch("dbt_projects_cli.commands.descriptions.ModelParser")
    @patch("dbt_projects_cli.commands.descriptions.setup_logging")
    @patch("click.confirm")
    def test_generate_command_interactive_skip(
        self,
        mock_confirm,
        mock_setup_logging,
        mock_parser_class,
        mock_llm_class,
        mock_discovery_class,
    ):
        """Test generate command in interactive mode when user skips update."""
        # Mock project discovery
        mock_discovery = MagicMock()
        mock_discovery_class.return_value = mock_discovery

        mock_project = MagicMock()
        mock_project.name = "test_project"
        mock_project.path = self.temp_dir
        mock_discovery.get_project_by_name.return_value = mock_project

        # Create a mock model path
        mock_model_path = MagicMock()
        mock_model_path.name = "test_model.sql"
        mock_model_path.stem = "test_model"
        mock_discovery.list_models_in_project.return_value = [mock_model_path]

        # Mock LLM generator
        mock_llm = MagicMock()
        mock_llm_class.return_value = mock_llm
        mock_llm.model = "gpt-4"

        # Mock parser and model info
        mock_parser = MagicMock()
        mock_parser_class.return_value = mock_parser

        mock_model_info = MagicMock()
        mock_model_info.name = "test_model"
        mock_model_info.sql_content = "SELECT * FROM table"
        mock_parser.parse_model.return_value = mock_model_info

        # Mock LLM descriptions result
        mock_descriptions = MagicMock()
        mock_descriptions.model_description = "Test model description"
        mock_descriptions.column_descriptions = {}
        mock_llm.generate_descriptions.return_value = mock_descriptions

        # Mock user choosing not to update
        mock_confirm.return_value = False

        result = self.runner.invoke(generate, ["-p", "test_project", "--interactive"])

        assert result.exit_code == 0
        assert "Skipped updating test_model" in result.output
