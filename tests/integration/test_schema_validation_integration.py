"""Integration tests for schema validation and error handling."""

import json

from click.testing import CliRunner

from dbt_projects_cli.commands.fabric import fabric


class TestSchemaValidationIntegration:
    """Integration tests for schema validation."""

    def test_fabric_config_validation_with_invalid_schema(self, tmp_path):
        """Test fabric config validation with invalid schema."""
        # Create invalid fabric config
        invalid_config = {
            "fabric": {
                "name": "test-fabric"
                # Missing required version field
            },
            "databricks": {
                "host": "octopus-workspace.cloud.databricks.com",
                "auth_type": "oauth",
            },
            "projects": {
                "test-project": {
                    "name": "test-project",
                    "schema": "test_schema",
                    "packages": [],
                }
            },
        }

        config_file = tmp_path / "fabric.json"
        with open(config_file, "w") as f:
            json.dump(invalid_config, f)

        runner = CliRunner()
        result = runner.invoke(fabric, ["validate", str(config_file)])

        # Should fail validation
        assert result.exit_code != 0
        assert "validation" in result.output.lower() or "error" in result.output.lower()

    def test_fabric_config_validation_with_missing_file(self, tmp_path):
        """Test fabric config validation with missing file."""
        runner = CliRunner()
        nonexistent_file = tmp_path / "nonexistent.json"

        result = runner.invoke(fabric, ["validate", str(nonexistent_file)])

        assert result.exit_code != 0
        assert "not found" in result.output.lower() or "error" in result.output.lower()

    def test_fabric_config_validation_with_invalid_json(self, tmp_path):
        """Test fabric config validation with invalid JSON."""
        config_file = tmp_path / "invalid.json"
        with open(config_file, "w") as f:
            f.write("{invalid json content")

        runner = CliRunner()
        result = runner.invoke(fabric, ["validate", str(config_file)])

        assert result.exit_code != 0
        assert "json" in result.output.lower() or "error" in result.output.lower()

    def test_fabric_config_validation_success(self, tmp_path):
        """Test successful fabric config validation."""
        valid_config = {
            "fabric": {"name": "test-fabric", "version": "1.0.0"},
            "databricks": {
                "host": "octopus-workspace.cloud.databricks.com",
                "auth_type": "oauth",
            },
            "projects": {
                "test-project": {
                    "name": "test-project",
                    "schema": "test_schema",
                    "packages": [{"package": "dbt-utils", "version": "1.0.0"}],
                }
            },
        }

        config_file = tmp_path / "fabric.json"
        with open(config_file, "w") as f:
            json.dump(valid_config, f)

        runner = CliRunner()
        result = runner.invoke(fabric, ["validate", str(config_file)])

        assert result.exit_code == 0
        assert "valid" in result.output.lower()

    def test_multiple_fabrics_config_validation(self, tmp_path):
        """Test multiple fabrics config validation."""
        multiple_config = {
            "fabrics": {
                "fabric1": {
                    "fabric": {"name": "fabric1", "version": "1.0.0"},
                    "databricks": {
                        "host": "octopus-workspace.cloud.databricks.com",
                        "auth_type": "oauth",
                    },
                    "projects": {
                        "project1": {
                            "name": "project1",
                            "schema": "project1_schema",
                            "packages": [{"package": "dbt-utils", "version": "1.0.0"}],
                        }
                    },
                },
                "fabric2": {
                    "fabric": {"name": "fabric2", "version": "1.0.0"},
                    "databricks": {
                        "host": "octopus-workspace.cloud.databricks.com",
                        "auth_type": "oauth",
                    },
                    "projects": {
                        "project2": {
                            "name": "project2",
                            "schema": "project2_schema",
                            "packages": [{"package": "dbt-utils", "version": "1.0.0"}],
                        }
                    },
                },
            }
        }

        config_file = tmp_path / "fabrics.json"
        with open(config_file, "w") as f:
            json.dump(multiple_config, f)

        runner = CliRunner()
        result = runner.invoke(
            fabric, ["validate", "--fabric", "fabric1", str(config_file)]
        )

        assert result.exit_code == 0
        assert "valid" in result.output.lower()

    def test_fabric_config_name_mismatch_error(self, tmp_path):
        """Test fabric config with name mismatch error."""
        config_with_mismatch = {
            "fabric": {"name": "fabric-name", "version": "1.0.0"},
            "databricks": {
                "host": "octopus-workspace.cloud.databricks.com",
                "auth_type": "oauth",
            },
            "projects": {
                "project1": {
                    "name": "project1",
                    "schema": "project1_schema",
                    "packages": [{"package": "dbt-utils", "version": "1.0.0"}],
                }
            },
        }

        config_file = tmp_path / "different-name.json"
        with open(config_file, "w") as f:
            json.dump(config_with_mismatch, f)

        runner = CliRunner()
        result = runner.invoke(fabric, ["validate", str(config_file)])

        # Should still validate since name mismatch is just a warning
        assert result.exit_code == 0
        assert "valid" in result.output.lower()

    def test_fabric_config_with_databricks_connection(self, tmp_path):
        """Test fabric config with Databricks connection validation."""
        config_with_databricks = {
            "fabric": {"name": "test-fabric", "version": "1.0.0"},
            "databricks": {
                "host": "octopus-workspace.cloud.databricks.com",
                "auth_type": "oauth",
            },
            "projects": {
                "test-project": {
                    "name": "test-project",
                    "schema": "test_schema",
                    "packages": [{"package": "dbt-utils", "version": "1.0.0"}],
                }
            },
        }

        config_file = tmp_path / "fabric.json"
        with open(config_file, "w") as f:
            json.dump(config_with_databricks, f)

        runner = CliRunner()
        result = runner.invoke(fabric, ["validate", str(config_file)])

        assert result.exit_code == 0
        assert "valid" in result.output.lower()

    def test_fabric_config_with_invalid_databricks_host(self, tmp_path):
        """Test fabric config with invalid Databricks host."""
        config_with_invalid_host = {
            "fabric": {"name": "test-fabric", "version": "1.0.0"},
            "databricks": {"host": "invalid-host-format", "auth_type": "oauth"},
            "projects": {
                "test-project": {
                    "name": "test-project",
                    "schema": "test_schema",
                    "packages": [{"package": "dbt-utils", "version": "1.0.0"}],
                }
            },
        }

        config_file = tmp_path / "fabric.json"
        with open(config_file, "w") as f:
            json.dump(config_with_invalid_host, f)

        runner = CliRunner()
        result = runner.invoke(fabric, ["validate", str(config_file)])

        assert result.exit_code != 0
        assert "validation" in result.output.lower() or "error" in result.output.lower()

    def test_fabric_config_with_package_validation(self, tmp_path):
        """Test fabric config with package validation."""
        config_with_packages = {
            "fabric": {"name": "test-fabric", "version": "1.0.0"},
            "databricks": {
                "host": "octopus-workspace.cloud.databricks.com",
                "auth_type": "oauth",
            },
            "projects": {
                "test-project": {
                    "name": "test-project",
                    "schema": "test_schema",
                    "packages": [
                        {"package": "dbt-utils", "version": "1.0.0"},
                        {
                            "git": "https://github.com/example/package.git",
                            "revision": "main",
                        },
                        {"local": "/path/to/local/package"},
                    ],
                }
            },
        }

        config_file = tmp_path / "fabric.json"
        with open(config_file, "w") as f:
            json.dump(config_with_packages, f)

        runner = CliRunner()
        result = runner.invoke(fabric, ["validate", str(config_file)])

        assert result.exit_code == 0
        assert "valid" in result.output.lower()

    def test_fabric_config_empty_projects_validation(self, tmp_path):
        """Test fabric config with empty projects validation."""
        config_empty_projects = {
            "fabric": {"name": "test-fabric", "version": "1.0.0"},
            "databricks": {
                "host": "octopus-workspace.cloud.databricks.com",
                "auth_type": "oauth",
            },
            "projects": {},
        }

        config_file = tmp_path / "fabric.json"
        with open(config_file, "w") as f:
            json.dump(config_empty_projects, f)

        runner = CliRunner()
        result = runner.invoke(fabric, ["validate", str(config_file)])

        assert result.exit_code != 0
        assert "validation" in result.output.lower() or "error" in result.output.lower()
