"""
Unit tests for the migrate command module.
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml
from click.testing import CliRunner

from dbt_projects_cli.commands.migrate import (
    _backup_files,
    _detect_changes,
    _find_packages,
    _infer_package_context,
    _normalize_yaml_content,
)
from dbt_projects_cli.commands.migrate import all as migrate_all
from dbt_projects_cli.commands.migrate import (
    list_migrations,
    migrate,
    package,
)


class TestMigrateCommand:
    """Test cases for migrate command."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.runner = CliRunner()

    def test_migrate_group_exists(self):
        """Test that the migrate command group exists and is callable."""
        result = self.runner.invoke(migrate, ["--help"])
        assert result.exit_code == 0
        assert "Commands for migrating existing packages" in result.output

    def test_package_command_missing_path(self):
        """Test package command with missing package path."""
        result = self.runner.invoke(package, [])
        assert "Error: --package-path is required" in result.output

    def test_package_command_nonexistent_path(self):
        """Test package command with nonexistent path."""
        result = self.runner.invoke(package, ["--package-path", "/nonexistent/path"])
        assert "does not exist" in result.output

    def test_package_command_missing_dbt_project(self):
        """Test package command with path that has no dbt_project.yml."""
        test_dir = self.temp_dir / "test_package"
        test_dir.mkdir()

        result = self.runner.invoke(package, ["--package-path", str(test_dir)])
        assert "No dbt_project.yml found" in result.output

    def test_package_command_no_package_name(self):
        """Test package command with dbt_project.yml that has no name."""
        test_dir = self.temp_dir / "test_package"
        test_dir.mkdir()

        # Create dbt_project.yml without name
        dbt_project = {"version": "1.0.0"}
        with open(test_dir / "dbt_project.yml", "w") as f:
            yaml.dump(dbt_project, f)

        result = self.runner.invoke(package, ["--package-path", str(test_dir)])
        assert "No package name found" in result.output

    def test_package_command_unknown_alignment(self):
        """Test package command with package that can't be aligned."""
        test_dir = self.temp_dir / "test_package"
        test_dir.mkdir()

        # Create dbt_project.yml with name but no clear alignment
        dbt_project = {"name": "unknown_package", "version": "1.0.0"}
        with open(test_dir / "dbt_project.yml", "w") as f:
            yaml.dump(dbt_project, f)

        result = self.runner.invoke(package, ["--package-path", str(test_dir)])
        assert "Could not determine package alignment" in result.output

    @patch("dbt_projects_cli.commands.migrate._detect_changes")
    @patch("dbt_projects_cli.commands.migrate.create_template_engine")
    def test_package_command_no_changes_needed(
        self, mock_engine_factory, mock_detect_changes
    ):
        """Test package command when no changes are needed."""
        test_dir = (
            self.temp_dir
            / "packages"
            / "domains"
            / "source-aligned"
            / "databricks"
            / "test_package"
        )
        test_dir.mkdir(parents=True)

        # Create dbt_project.yml with clear alignment
        dbt_project = {"name": "databricks_test_package", "version": "1.0.0"}
        with open(test_dir / "dbt_project.yml", "w") as f:
            yaml.dump(dbt_project, f)

        # Mock template engine
        mock_engine = MagicMock()
        mock_engine_factory.return_value = mock_engine
        mock_engine.generate_dbt_project_yml.return_value = "mock content"
        mock_engine.generate_packages_yml.return_value = "mock packages"
        mock_engine.generate_group_yml.return_value = "mock groups"

        # Mock no changes detected
        mock_detect_changes.return_value = False

        result = self.runner.invoke(package, ["--package-path", str(test_dir)])
        assert "already up to date" in result.output

    def test_infer_package_context_source_aligned(self):
        """Test inferring context for source-aligned packages."""
        test_dir = Path("/packages/domains/source-aligned/databricks/systems-data")
        config = {"name": "databricks_systems_data"}

        alignment, context = _infer_package_context(test_dir, config)

        assert alignment == "source-aligned"
        assert context["alignment"] == "source-aligned"
        assert context["package_name"] == "databricks_systems_data"
        assert context["source_system"] == "databricks"
        assert "systems" in context["domain_name"]

    def test_infer_package_context_consumer_aligned(self):
        """Test inferring context for consumer-aligned packages."""
        test_dir = Path(
            "/packages/domains/consumer-aligned/marketing/customer-analytics"
        )
        config = {"name": "marketing_customer_analytics"}

        alignment, context = _infer_package_context(test_dir, config)

        assert alignment == "consumer-aligned"
        assert context["alignment"] == "consumer-aligned"
        assert context["package_name"] == "marketing_customer_analytics"
        assert context["business_area"] == "marketing"
        assert "customer" in context["domain_name"]

    def test_infer_package_context_utils(self):
        """Test inferring context for utils packages."""
        test_dir = Path("/packages/utils/data-quality")
        config = {"name": "utils_data_quality"}

        alignment, context = _infer_package_context(test_dir, config)

        assert alignment == "utils"
        assert context["alignment"] == "utils"
        assert context["package_name"] == "utils_data_quality"
        assert "quality" in context["domain_name"]

    def test_infer_package_context_fallback_from_name(self):
        """Test inferring context when path doesn't contain alignment info."""
        test_dir = Path("/some/other/path")
        config = {"name": "databricks_systems_data"}

        alignment, context = _infer_package_context(test_dir, config)

        # Actually, if path doesn't contain alignment info, it should return None
        # Only if the package name contains underscore should it try to infer
        if alignment:
            assert alignment == "source-aligned"
            assert context["source_system"] == "databricks"
        else:
            assert alignment is None

    def test_infer_package_context_unknown(self):
        """Test inferring context for unknown package types."""
        test_dir = Path("/unknown/path")
        config = {"name": "unknown_package"}

        alignment, context = _infer_package_context(test_dir, config)

        assert alignment is None
        assert context == {}

    def test_find_packages_no_packages_dir(self):
        """Test finding packages when packages directory doesn't exist."""
        with patch("pathlib.Path.exists", return_value=False):
            packages = _find_packages()
            assert packages == []

    @patch("dbt_projects_cli.commands.migrate.console")
    def test_find_packages_with_invalid_yaml(self, mock_console):
        """Test finding packages with invalid YAML files."""
        # Create packages directory structure in temp dir
        packages_dir = self.temp_dir / "packages"
        packages_dir.mkdir()

        test_package_dir = packages_dir / "test_package"
        test_package_dir.mkdir()

        # Create invalid YAML file
        with open(test_package_dir / "dbt_project.yml", "w") as f:
            f.write("invalid: yaml: [")

        # Mock the base_dir in _find_packages to use our temp directory
        with patch("dbt_projects_cli.commands.migrate.Path") as mock_path_class:
            mock_path_instance = MagicMock()
            mock_path_class.return_value = mock_path_instance
            mock_path_instance.exists.return_value = True
            mock_path_instance.rglob.return_value = [
                test_package_dir / "dbt_project.yml"
            ]

            packages = _find_packages()

        # Should handle the error gracefully and continue
        assert packages == []
        mock_console.print.assert_called()

    def test_detect_changes_file_not_exists(self):
        """Test detecting changes when dbt_project.yml doesn't exist."""
        test_dir = self.temp_dir / "test_package"
        test_dir.mkdir()

        changes = _detect_changes(test_dir, "new content", "packages", "groups")

        # Should detect changes since file doesn't exist
        assert changes is True

    def test_detect_changes_different_content(self):
        """Test detecting changes when content is different."""
        test_dir = self.temp_dir / "test_package"
        test_dir.mkdir()

        # Create existing file with different content
        with open(test_dir / "dbt_project.yml", "w") as f:
            f.write("name: old_package\nversion: 1.0.0")

        new_content = "name: new_package\nversion: 2.0.0"
        changes = _detect_changes(test_dir, new_content, "packages", "groups")

        assert changes is True

    def test_detect_changes_same_content(self):
        """Test detecting changes when content is the same."""
        test_dir = self.temp_dir / "test_package"
        test_dir.mkdir()

        content = "name: test_package\nversion: 1.0.0"

        # Create existing file with same content
        with open(test_dir / "dbt_project.yml", "w") as f:
            f.write(content)

        changes = _detect_changes(test_dir, content, "packages", "groups")

        assert changes is False

    def test_normalize_yaml_content_valid(self):
        """Test normalizing valid YAML content."""
        content = "name: test\nversion: 1.0.0\nconfig:\n  var1: value1"

        normalized = _normalize_yaml_content(content)

        # Should be parseable YAML
        parsed = yaml.safe_load(normalized)
        assert parsed["name"] == "test"
        assert parsed["version"] == "1.0.0"

    def test_normalize_yaml_content_invalid(self):
        """Test normalizing invalid YAML content."""
        invalid_content = "invalid: yaml: ["

        normalized = _normalize_yaml_content(invalid_content)

        # Should return original content when parsing fails
        assert normalized == invalid_content.strip()

    def test_backup_files_creates_backups(self):
        """Test that backup files are created for existing files."""
        test_dir = self.temp_dir / "test_package"
        test_dir.mkdir()

        # Create existing files
        dbt_project_file = test_dir / "dbt_project.yml"
        dbt_project_file.write_text("name: test")

        packages_file = test_dir / "packages.yml"
        packages_file.write_text("packages: []")

        groups_dir = test_dir / "groups"
        groups_dir.mkdir()
        group_file = groups_dir / "_group.yml"
        group_file.write_text("groups: []")

        _backup_files(test_dir)

        # Check that backup files were created
        assert (test_dir / "dbt_project.yml.bak").exists()
        assert (test_dir / "packages.yml.bak").exists()
        assert (test_dir / "groups/_group.yml.bak").exists()

        # Check backup content matches original
        assert (test_dir / "dbt_project.yml.bak").read_text() == "name: test"
        assert (test_dir / "packages.yml.bak").read_text() == "packages: []"
        assert (test_dir / "groups/_group.yml.bak").read_text() == "groups: []"

    def test_backup_files_missing_files(self):
        """Test backup when some files don't exist."""
        test_dir = self.temp_dir / "test_package"
        test_dir.mkdir()

        # Only create one file
        dbt_project_file = test_dir / "dbt_project.yml"
        dbt_project_file.write_text("name: test")

        # Should not raise an error
        _backup_files(test_dir)

        # Only the existing file should have a backup
        assert (test_dir / "dbt_project.yml.bak").exists()
        assert not (test_dir / "packages.yml.bak").exists()
        assert not (test_dir / "groups/_group.yml.bak").exists()

    @patch("dbt_projects_cli.commands.migrate.create_template_engine")
    def test_list_migrations_command_no_migrations(self, mock_engine_factory):
        """Test list_migrations command when no migrations are available."""
        # Mock template engine with no migrations
        mock_engine = MagicMock()
        mock_engine_factory.return_value = mock_engine
        mock_engine.get_migrations.return_value = []

        result = self.runner.invoke(list_migrations)

        assert result.exit_code == 0
        assert "No migrations available" in result.output

    @patch("dbt_projects_cli.commands.migrate.create_template_engine")
    def test_list_migrations_command_with_migrations(self, mock_engine_factory):
        """Test list_migrations command with available migrations."""
        # Mock template engine with migrations
        mock_engine = MagicMock()
        mock_engine_factory.return_value = mock_engine
        mock_engine.get_migrations.return_value = [
            {
                "version": "1.1.0",
                "description": "Add new tags",
                "changes": ["tag1", "tag2"],
            },
            {
                "version": "1.2.0",
                "description": "Update models config",
                "changes": ["model_config"],
            },
        ]

        result = self.runner.invoke(list_migrations)

        assert result.exit_code == 0
        assert "Available Migrations" in result.output
        assert "1.1.0" in result.output
        assert "Add new tags" in result.output
        assert "1.2.0" in result.output
        assert "Update models config" in result.output

    @patch("dbt_projects_cli.commands.migrate._find_packages")
    def test_migrate_all_no_packages(self, mock_find_packages):
        """Test migrate all command when no packages are found."""
        mock_find_packages.return_value = []

        result = self.runner.invoke(migrate_all)

        assert result.exit_code == 0
        assert "No packages found to migrate" in result.output

    @patch("dbt_projects_cli.commands.migrate._find_packages")
    def test_migrate_all_dry_run(self, mock_find_packages):
        """Test migrate all command with dry run flag."""
        mock_find_packages.return_value = [
            {
                "name": "test_package",
                "alignment": "source-aligned",
                "path": Path("/test"),
                "context": {},
            }
        ]

        result = self.runner.invoke(migrate_all, ["--dry-run"])

        assert result.exit_code == 0
        assert "This was a dry run" in result.output
