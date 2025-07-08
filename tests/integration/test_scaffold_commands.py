"""
Integration tests for scaffold commands.
These tests run actual CLI commands to verify end-to-end functionality.
"""

import os
import shutil
import tempfile
from pathlib import Path

import pytest
from click.testing import CliRunner

from dbt_projects_cli.main import cli


@pytest.mark.integration
@pytest.mark.cli
class TestScaffoldCommands:
    """Test suite for scaffold commands integration."""

    @pytest.fixture(autouse=True)
    def setup_test_env(self):
        """Set up a temporary directory for each test."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.original_cwd = os.getcwd()
        os.chdir(self.test_dir)
        yield
        os.chdir(self.original_cwd)
        shutil.rmtree(self.test_dir)

    def test_scaffold_domain_source_aligned(self):
        """Test scaffolding a source-aligned domain package."""
        runner = CliRunner()

        result = runner.invoke(
            cli,
            [
                "scaffold",
                "domain",
                "--alignment",
                "source-aligned",
                "--source-system",
                "databricks",
                "--domain-name",
                "test-data",
                "--description",
                "Test source-aligned package",
                "--team-from-catalog",
                "data_platform",
            ],
        )

        assert result.exit_code == 0
        assert "created successfully" in result.output

        # Verify package structure was created
        package_path = Path("packages/domains/source-aligned/databricks/test-data")
        assert package_path.exists()

        # Verify essential files exist
        assert (package_path / "dbt_project.yml").exists()
        assert (package_path / "packages.yml").exists()
        assert (package_path / "groups/_group.yml").exists()
        assert (package_path / "models/staging").exists()

        # Verify dbt_project.yml content
        with open(package_path / "dbt_project.yml", "r") as f:
            content = f.read()
            assert "databricks_test_data" in content
            assert "source-aligned" in content
            assert "databricks" in content

    def test_scaffold_domain_consumer_aligned(self):
        """Test scaffolding a consumer-aligned domain package."""
        runner = CliRunner()

        result = runner.invoke(
            cli,
            [
                "scaffold",
                "domain",
                "--alignment",
                "consumer-aligned",
                "--business-area",
                "marketing",
                "--domain-name",
                "customer-analytics",
                "--description",
                "Test consumer-aligned package",
                "--team-from-catalog",
                "marketing",
            ],
        )

        assert result.exit_code == 0
        assert "created successfully" in result.output

        # Verify package structure was created
        package_path = Path(
            "packages/domains/consumer-aligned/marketing/customer-analytics"
        )
        assert package_path.exists()

        # Verify essential files exist
        assert (package_path / "dbt_project.yml").exists()
        assert (package_path / "packages.yml").exists()
        assert (package_path / "groups/_group.yml").exists()
        assert (package_path / "models/staging").exists()
        assert (package_path / "models/marts").exists()

        # Verify dbt_project.yml content
        with open(package_path / "dbt_project.yml", "r") as f:
            content = f.read()
            assert "marketing_customer_analytics" in content
            assert "consumer-aligned" in content
            assert "marketing" in content

        # Verify packages.yml includes base packages
        with open(package_path / "packages.yml", "r") as f:
            content = f.read()
            assert "dbt-labs/dbt_utils" in content
            assert "dbt-labs/codegen" in content

    def test_scaffold_domain_utils(self):
        """Test scaffolding a utils package."""
        runner = CliRunner()

        result = runner.invoke(
            cli,
            [
                "scaffold",
                "domain",
                "--alignment",
                "utils",
                "--domain-name",
                "test-utils",
                "--description",
                "Test utils package",
                "--team-from-catalog",
                "data_platform",
            ],
        )

        assert result.exit_code == 0
        assert "created successfully" in result.output

        # Verify package structure was created
        package_path = Path("packages/utils/test-utils")
        assert package_path.exists()

        # Verify essential files exist
        assert (package_path / "dbt_project.yml").exists()
        assert (package_path / "packages.yml").exists()
        assert (package_path / "groups/_group.yml").exists()

        # Verify dbt_project.yml content
        with open(package_path / "dbt_project.yml", "r") as f:
            content = f.read()
            assert "utils_test_utils" in content
            assert "data_platform" in content

    def test_scaffold_domain_duplicate_error(self):
        """Test that scaffolding fails when package already exists."""
        runner = CliRunner()

        # Create the package first time
        result = runner.invoke(
            cli,
            [
                "scaffold",
                "domain",
                "--alignment",
                "source-aligned",
                "--source-system",
                "databricks",
                "--domain-name",
                "test-data",
                "--description",
                "Test source-aligned package",
                "--team-from-catalog",
                "data_platform",
            ],
        )
        assert result.exit_code == 0

        # Try to create the same package again
        result = runner.invoke(
            cli,
            [
                "scaffold",
                "domain",
                "--alignment",
                "source-aligned",
                "--source-system",
                "databricks",
                "--domain-name",
                "test-data",
                "--description",
                "Test duplicate package",
                "--group-from-catalog",
                "data_platform",
            ],
        )
        assert result.exit_code == 1
        assert "already exists" in result.output

    def test_scaffold_fabric(self):
        """Test scaffolding a fabric project."""
        runner = CliRunner()

        result = runner.invoke(
            cli,
            [
                "scaffold",
                "fabric",
                "--fabric",
                "test-fabric",
                "--project",
                "analytics",
                "--component",
                "core",
                "--catalog",
                "test_catalog",
                "--workspace-id",
                "123456789",
            ],
        )

        assert result.exit_code == 0
        assert "created successfully" in result.output

        # Verify fabric structure was created
        fabric_path = Path("fabrics/test-fabric/projects/analytics/core")
        assert fabric_path.exists()

        # Verify essential files exist
        assert (fabric_path / "dbt_project.yml").exists()
        assert (fabric_path / "packages.yml").exists()
        assert (fabric_path / "groups/_group.yml").exists()
        assert (fabric_path / "profiles.yml").exists()

        # Verify dbt_project.yml content
        with open(fabric_path / "dbt_project.yml", "r") as f:
            content = f.read()
            assert "test_fabric_analytics_core" in content
            assert "test_catalog" in content
            assert "123456789" in content

    def test_scaffold_info_command(self):
        """Test the scaffold info command."""
        runner = CliRunner()

        result = runner.invoke(cli, ["scaffold", "info"])
        assert result.exit_code == 0
        assert "Data Mesh Domain Patterns" in result.output
        assert "source-aligned" in result.output
        assert "consumer-aligned" in result.output
        assert "utils" in result.output
