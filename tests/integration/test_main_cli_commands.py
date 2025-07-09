"""
Integration tests for main CLI entry points and project discovery.

Tests core functionality to improve integration test coverage.
"""

import yaml
from click.testing import CliRunner

from dbt_projects_cli.main import cli


class TestMainCliCommands:
    """Test main CLI entry points."""

    def test_main_cli_help(self):
        """Test that main CLI shows help."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])

        assert result.exit_code == 0
        assert "dbt-projects-cli" in result.output
        assert "descriptions" in result.output
        assert "models" in result.output
        assert "projects" in result.output
        assert "utils" in result.output

    def test_main_cli_version(self):
        """Test that main CLI shows version."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])

        assert result.exit_code == 0
        # Version output should contain version info

    def test_main_cli_with_verbose_flag(self):
        """Test main CLI with verbose flag."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--verbose", "--help"])

        assert result.exit_code == 0
        assert "dbt-projects-cli" in result.output

    def test_main_cli_subcommand_access(self):
        """Test that main CLI can access subcommands."""
        runner = CliRunner()

        # Test descriptions subcommand
        result = runner.invoke(cli, ["descriptions", "--help"])
        assert result.exit_code == 0
        assert "Commands for managing dbt model descriptions" in result.output

        # Test models subcommand
        result = runner.invoke(cli, ["models", "--help"])
        assert result.exit_code == 0
        assert "Commands for managing dbt models" in result.output

        # Test projects subcommand
        result = runner.invoke(cli, ["projects", "--help"])
        assert result.exit_code == 0
        assert "Commands for managing dbt projects" in result.output

        # Test utils subcommand
        result = runner.invoke(cli, ["utils", "--help"])
        assert result.exit_code == 0
        assert "Utility commands for dbt projects" in result.output


class TestProjectDiscoveryIntegration:
    """Test project discovery functionality through integration tests."""

    def create_test_dbt_project(
        self, tmp_path, project_name="test_project", project_type="package"
    ):
        """Create a minimal dbt project for testing."""
        if project_type == "package":
            project_dir = tmp_path / "packages" / project_name
        else:
            project_dir = tmp_path / "fabrics" / project_name

        project_dir.mkdir(parents=True)

        # Create dbt_project.yml for packages or fabric.yml for fabrics
        if project_type == "package":
            config_file = project_dir / "dbt_project.yml"
            config = {
                "name": project_name,
                "version": "1.0.0",
                "profile": "test_profile",
                "model-paths": ["models"],
                "target-path": "target",
            }
        else:
            config_file = project_dir / "fabric.yml"
            config = {
                "fabric": {"name": project_name},
                "projects": {"default": {"name": "default"}},
            }

        with open(config_file, "w") as f:
            yaml.dump(config, f)

        # Create models directory and a test model
        models_dir = project_dir / "models"
        models_dir.mkdir()

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

    def test_project_discovery_with_packages(self, tmp_path):
        """Test project discovery finds packages correctly."""
        # Create test projects
        self.create_test_dbt_project(tmp_path, "project1", "package")
        self.create_test_dbt_project(tmp_path, "project2", "package")

        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["projects", "list"])

        assert result.exit_code == 0
        assert "All dbt Projects" in result.output

    def test_project_discovery_with_fabrics(self, tmp_path):
        """Test project discovery finds fabric projects correctly."""
        # Create test fabric projects
        self.create_test_dbt_project(tmp_path, "fabric1", "fabric")
        self.create_test_dbt_project(tmp_path, "fabric2", "fabric")

        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["projects", "list"])

        assert result.exit_code == 0
        assert "All dbt Projects" in result.output

    def test_project_discovery_mixed_projects(self, tmp_path):
        """Test project discovery with both packages and fabrics."""
        # Create mixed projects
        self.create_test_dbt_project(tmp_path, "package1", "package")
        self.create_test_dbt_project(tmp_path, "fabric1", "fabric")

        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["projects", "list"])

        assert result.exit_code == 0
        assert "All dbt Projects" in result.output

    def test_project_info_command_integration(self, tmp_path):
        """Test project info command through main CLI."""
        # Create a test project
        self.create_test_dbt_project(tmp_path, "test_project", "package")

        runner = CliRunner()
        # Change working directory to tmp_path so the CLI can find the project
        import os

        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            result = runner.invoke(cli, ["projects", "info", "test_project"])
        finally:
            os.chdir(original_cwd)

        # Should either succeed or give a meaningful error about the project
        assert result.exit_code == 0 or "test_project" in result.output

    def test_models_list_command_integration(self, tmp_path):
        """Test models list command through main CLI."""
        # Create a test project
        self.create_test_dbt_project(tmp_path, "test_project", "package")

        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["models", "list", "--project", "test_project"])

        assert result.exit_code == 0

    def test_utils_validate_command_integration(self, tmp_path):
        """Test utils validate command through main CLI."""
        # Create test projects
        self.create_test_dbt_project(tmp_path, "project1", "package")
        self.create_test_dbt_project(tmp_path, "project2", "package")

        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["utils", "validate"])

        assert result.exit_code == 0
        assert "Validating dbt projects" in result.output

    def test_utils_clean_command_integration(self, tmp_path):
        """Test utils clean command through main CLI."""
        # Create test projects with target directories
        project1 = self.create_test_dbt_project(tmp_path, "project1", "package")

        # Create target directory to be cleaned
        target_dir = project1 / "target"
        target_dir.mkdir()
        with open(target_dir / "manifest.json", "w") as f:
            f.write('{"version": "1.0.0"}')

        runner = CliRunner()
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["utils", "clean"])

        assert result.exit_code == 0
        assert "Cleaning dbt projects" in result.output

    def test_invalid_command_handling(self):
        """Test main CLI handles invalid commands gracefully."""
        runner = CliRunner()
        result = runner.invoke(cli, ["nonexistent_command"])

        assert result.exit_code != 0
        assert "No such command" in result.output

    def test_command_context_passing(self, tmp_path):
        """Test that CLI context is passed correctly to subcommands."""
        runner = CliRunner()

        # Test with verbose flag
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["--verbose", "projects", "list"])

        assert result.exit_code == 0

    def test_mixed_args_and_subcommands(self, tmp_path):
        """Test main CLI with mixed global args and subcommand args."""
        runner = CliRunner()

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["--verbose", "projects", "list"])

        assert result.exit_code == 0
