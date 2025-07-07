"""
Integration tests for fabric CLI commands.

Tests the CLI commands end-to-end with real configuration files.
"""

import yaml
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from dbt_projects_cli.commands.fabric import fabric


class TestFabricCommands:
    """Test fabric CLI commands."""
    
    def get_valid_config_dict(self):
        """Return a valid configuration dictionary for testing."""
        return {
            "fabric": {
                "name": "test-fabric",
                "description": "A test fabric for integration testing",
                "version": "1.0.0"
            },
            "databricks": {
                "host": "test-workspace.cloud.databricks.com",
                "catalog": "test_catalog",
                "schema": "test_schema",
                "auth_type": "oauth"
            },
            "packages": [
                {
                    "git": "https://github.com/dbt-labs/dbt-utils.git",
                    "revision": "1.0.0"
                },
                {
                    "local": "../packages/utils/core"
                },
                {
                    "package": "elementary-data/elementary",
                    "version": ">=0.16.4"
                }
            ],
            "vars": {
                "PROD_CATALOG": "prod_catalog",
                "debug_mode": False
            },
            "models": {
                "+materialized": "table",
                "staging": {
                    "+materialized": "view"
                }
            }
        }
    
    def test_fabric_command_group(self):
        """Test that fabric command group is accessible."""
        runner = CliRunner()
        result = runner.invoke(fabric, ["--help"])
        
        assert result.exit_code == 0
        assert "Lightweight fabric deployment commands" in result.output
        assert "deploy" in result.output
        assert "validate" in result.output
        assert "init" in result.output
        assert "schema" in result.output
        assert "generate" in result.output
    
    def test_validate_command_success(self, tmp_path):
        """Test successful validation of a fabric configuration."""
        config_dict = self.get_valid_config_dict()
        config_file = tmp_path / "fabric.yml"
        
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f)
        
        runner = CliRunner()
        result = runner.invoke(fabric, ["validate", str(config_file)])
        
        assert result.exit_code == 0
        assert "Configuration is valid!" in result.output
        assert "test-fabric" in result.output
        assert "test-workspace.cloud.databricks.com" in result.output
        assert "3" in result.output  # Number of packages
    
    def test_validate_command_missing_file(self, tmp_path):
        """Test validation with missing configuration file."""
        config_file = tmp_path / "nonexistent.yml"
        
        runner = CliRunner()
        result = runner.invoke(fabric, ["validate", str(config_file)])
        
        assert result.exit_code != 0
        assert "does not exist" in result.output
    
    def test_validate_command_invalid_config(self, tmp_path):
        """Test validation with invalid configuration."""
        invalid_config = {"invalid": "configuration"}
        config_file = tmp_path / "invalid.yml"
        
        with open(config_file, 'w') as f:
            yaml.dump(invalid_config, f)
        
        runner = CliRunner()
        result = runner.invoke(fabric, ["validate", str(config_file)])
        
        assert result.exit_code != 0
        assert "Validation failed" in result.output
    
    def test_deploy_command_dry_run(self, tmp_path):
        """Test deploy command in dry-run mode."""
        config_dict = self.get_valid_config_dict()
        config_file = tmp_path / "fabric.yml"
        
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f)
        
        runner = CliRunner()
        result = runner.invoke(fabric, ["deploy", str(config_file), "--dry-run"])
        
        assert result.exit_code == 0
        assert "Configuration is valid!" in result.output
        assert "test-fabric" in result.output
        assert "3 configured" in result.output
    
    def test_deploy_command_with_output_dir(self, tmp_path):
        """Test deploy command with specified output directory."""
        config_dict = self.get_valid_config_dict()
        config_file = tmp_path / "fabric.yml"
        
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f)
        
        output_dir = tmp_path / "deployment"
        
        runner = CliRunner()
        result = runner.invoke(fabric, [
            "deploy", 
            str(config_file), 
            "--output-dir", str(output_dir)
        ])
        
        assert result.exit_code == 0
        assert "Fabric deployed successfully!" in result.output
        assert output_dir.name in result.output
        
        # Check that files were created
        assert (output_dir / "dbt_project.yml").is_file()
        assert (output_dir / "packages.yml").is_file()
        assert (output_dir / "profiles.yml").is_file()
        assert (output_dir / "README.md").is_file()
    
    def test_deploy_command_verbose(self, tmp_path):
        """Test deploy command with verbose output."""
        config_dict = self.get_valid_config_dict()
        config_file = tmp_path / "fabric.yml"
        
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f)
        
        output_dir = tmp_path / "deployment"
        
        runner = CliRunner()
        result = runner.invoke(fabric, [
            "deploy", 
            str(config_file), 
            "--output-dir", str(output_dir),
            "--verbose"
        ])
        
        assert result.exit_code == 0
        assert "Generated files:" in result.output
        assert "dbt_project.yml" in result.output
        assert "packages.yml" in result.output
    
    def test_init_command_interactive(self, tmp_path):
        """Test init command with interactive prompts."""
        output_file = tmp_path / "new_fabric.yml"
        
        runner = CliRunner()
        result = runner.invoke(fabric, [
            "init",
            "--output", str(output_file),
            "--name", "my-test-fabric",
            "--description", "Test fabric description",
            "--databricks-host", "test.cloud.databricks.com",
            "--catalog", "test_cat",
            "--schema", "test_sch"
        ])
        
        assert result.exit_code == 0
        assert "Created fabric configuration" in result.output
        assert output_file.name in result.output
        
        # Check that file was created and contains expected content
        assert output_file.is_file()
        
        with open(output_file, 'r') as f:
            config = yaml.safe_load(f)
        
        assert config["fabric"]["name"] == "my-test-fabric"
        assert config["fabric"]["description"] == "Test fabric description"
        assert config["databricks"]["host"] == "test.cloud.databricks.com"
        assert config["databricks"]["catalog"] == "test_cat"
        assert config["databricks"]["schema"] == "test_sch"
        assert len(config["packages"]) > 0
    
    def test_init_command_overwrite_protection(self, tmp_path):
        """Test init command respects existing files."""
        output_file = tmp_path / "existing.yml"
        
        # Create an existing file
        with open(output_file, 'w') as f:
            f.write("existing content")
        
        runner = CliRunner()
        result = runner.invoke(fabric, [
            "init",
            "--output", str(output_file),
            "--name", "test-fabric",
            "--databricks-host", "test.cloud.databricks.com",
            "--catalog", "test_cat",
            "--schema", "test_sch"
        ], input="n\n")  # Say no to overwrite
        
        assert result.exit_code == 0
        assert "Operation cancelled" in result.output
        
        # Check that original content is preserved
        with open(output_file, 'r') as f:
            content = f.read()
        
        assert content == "existing content"
    
    def test_schema_command(self):
        """Test schema command shows configuration documentation."""
        runner = CliRunner()
        result = runner.invoke(fabric, ["schema"])
        
        assert result.exit_code == 0
        assert "Fabric Configuration Schema" in result.output
        assert "fabric:" in result.output
        assert "databricks:" in result.output
        assert "packages:" in result.output
        assert "Package Types Supported" in result.output
        assert "Advanced Features" in result.output
    
    def test_generate_command(self, tmp_path):
        """Test generate command creates dbt project."""
        config_dict = self.get_valid_config_dict()
        config_file = tmp_path / "fabric.yml"
        
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f)
        
        output_dir = tmp_path / "generated"
        
        runner = CliRunner()
        result = runner.invoke(fabric, [
            "generate",
            str(config_file),
            "--output-dir", str(output_dir)
        ])
        
        assert result.exit_code == 0
        assert "dbt project generated successfully!" in result.output
        assert output_dir.name in result.output
        
        # Check project structure
        assert (output_dir / "dbt_project.yml").is_file()
        assert (output_dir / "packages.yml").is_file()
        assert (output_dir / "profiles.yml").is_file()
        assert (output_dir / "README.md").is_file()
        assert (output_dir / "models").is_dir()
        assert (output_dir / "macros").is_dir()
    
    @patch('subprocess.run')
    def test_generate_command_with_deps_installation(self, mock_subprocess, tmp_path):
        """Test generate command with dependency installation."""
        config_dict = self.get_valid_config_dict()
        config_file = tmp_path / "fabric.yml"
        
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f)
        
        output_dir = tmp_path / "generated"
        
        # Mock successful dbt deps
        mock_subprocess.return_value.returncode = 0
        mock_subprocess.return_value.stderr = ""
        
        runner = CliRunner()
        result = runner.invoke(fabric, [
            "generate",
            str(config_file),
            "--output-dir", str(output_dir),
            "--install-deps"
        ])
        
        assert result.exit_code == 0
        assert "Installing dbt dependencies" in result.output
        assert "Dependencies installed successfully!" in result.output
        
        # Check that dbt deps was called
        mock_subprocess.assert_called_with(
            ["dbt", "deps"], 
            capture_output=True, 
            text=True
        )
    
    @patch('subprocess.run')
    def test_generate_command_with_connection_test(self, mock_subprocess, tmp_path):
        """Test generate command with connection testing."""
        config_dict = self.get_valid_config_dict()
        config_file = tmp_path / "fabric.yml"
        
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f)
        
        output_dir = tmp_path / "generated"
        
        # Mock successful dbt debug
        mock_subprocess.return_value.returncode = 0
        mock_subprocess.return_value.stderr = ""
        
        runner = CliRunner()
        result = runner.invoke(fabric, [
            "generate",
            str(config_file),
            "--output-dir", str(output_dir),
            "--test-connection"
        ])
        
        assert result.exit_code == 0
        assert "Testing Databricks connection" in result.output
        assert "Connection test successful!" in result.output
        
        # Check that dbt debug was called
        mock_subprocess.assert_called_with(
            ["dbt", "debug"], 
            capture_output=True, 
            text=True
        )
    
    @patch('subprocess.run')
    def test_generate_command_failed_deps(self, mock_subprocess, tmp_path):
        """Test generate command handles failed dependency installation."""
        config_dict = self.get_valid_config_dict()
        config_file = tmp_path / "fabric.yml"
        
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f)
        
        output_dir = tmp_path / "generated"
        
        # Mock failed dbt deps
        mock_subprocess.return_value.returncode = 1
        mock_subprocess.return_value.stderr = "Error installing dependencies"
        
        runner = CliRunner()
        result = runner.invoke(fabric, [
            "generate",
            str(config_file),
            "--output-dir", str(output_dir),
            "--install-deps"
        ])
        
        assert result.exit_code == 0  # Command should still succeed
        assert "Failed to install dependencies" in result.output
        assert "Error installing dependencies" in result.output
    
    def test_error_handling_invalid_yaml(self, tmp_path):
        """Test that invalid YAML files are handled gracefully."""
        config_file = tmp_path / "invalid.yml"
        
        with open(config_file, 'w') as f:
            f.write("invalid: yaml: content: [")
        
        runner = CliRunner()
        result = runner.invoke(fabric, ["validate", str(config_file)])
        
        assert result.exit_code != 0
        assert "Invalid YAML" in result.output
