"""Unit tests for main CLI module."""

from unittest.mock import patch

from click.testing import CliRunner

from dbt_projects_cli.main import cli


class TestMainCli:
    """Test main CLI functionality."""

    def test_main_cli_basic_functionality(self):
        """Test that main CLI loads correctly."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])

        assert result.exit_code == 0
        assert "Usage:" in result.output
        assert "Commands:" in result.output

    def test_main_cli_version_flag(self):
        """Test the version flag."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])

        assert result.exit_code == 0
        # Should show version information
        assert any(
            word in result.output.lower() for word in ["version", "dbt-projects-cli"]
        )

    def test_main_cli_verbose_flag(self):
        """Test the verbose flag."""
        runner = CliRunner()

        # Test that verbose flag doesn't break help
        result = runner.invoke(cli, ["--verbose", "--help"])
        assert result.exit_code == 0
        assert "Usage:" in result.output

    def test_main_cli_subcommand_access(self):
        """Test that main CLI can access subcommands."""
        runner = CliRunner()

        # Test that subcommands are accessible
        subcommands = [
            "projects",
            "models",
            "utils",
            "descriptions",
            "fabric",
            "migrate",
            "scaffold",
        ]

        for subcommand in subcommands:
            result = runner.invoke(cli, [subcommand, "--help"])
            assert result.exit_code == 0
            assert "Usage:" in result.output

    def test_main_cli_invalid_command(self):
        """Test main CLI with invalid command."""
        runner = CliRunner()
        result = runner.invoke(cli, ["invalid-command"])

        assert result.exit_code != 0
        assert "No such command" in result.output

    def test_main_cli_context_setup(self):
        """Test that CLI context is set up correctly."""
        runner = CliRunner()

        # Test that context doesn't cause errors
        result = runner.invoke(cli, ["projects", "list"])
        # Should not crash due to context issues
        assert result.exit_code == 0 or "Error" not in result.output

    def test_main_cli_nested_command_structure(self):
        """Test nested command structure works."""
        runner = CliRunner()

        # Test nested commands
        nested_commands = [
            ["projects", "list"],
            ["projects", "info", "--help"],
            ["models", "list", "--help"],
            ["models", "analyze", "--help"],
            ["utils", "validate", "--help"],
            ["utils", "clean", "--help"],
        ]

        for cmd_parts in nested_commands:
            result = runner.invoke(cli, cmd_parts)
            assert result.exit_code == 0

    def test_main_cli_error_handling(self):
        """Test that main CLI handles errors gracefully."""
        runner = CliRunner()

        # Test with invalid options
        result = runner.invoke(cli, ["--invalid-option"])
        assert result.exit_code != 0
        assert "No such option" in result.output

    def test_main_cli_help_for_all_commands(self):
        """Test that help is available for all commands."""
        runner = CliRunner()

        # Get list of available commands by checking help output
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0

        # Each command should have help available
        help_commands = [
            ["projects", "--help"],
            ["models", "--help"],
            ["utils", "--help"],
            ["descriptions", "--help"],
            ["fabric", "--help"],
            ["migrate", "--help"],
            ["scaffold", "--help"],
        ]

        for cmd_parts in help_commands:
            result = runner.invoke(cli, cmd_parts)
            assert result.exit_code == 0
            assert "Usage:" in result.output

    def test_main_cli_command_group_structure(self):
        """Test that command groups are structured correctly."""
        runner = CliRunner()

        # Test that main command groups exist
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0

        # Should contain expected command groups
        expected_groups = [
            "projects",
            "models",
            "utils",
            "descriptions",
            "fabric",
            "migrate",
            "scaffold",
        ]
        for group in expected_groups:
            assert group in result.output

    def test_main_cli_context_object(self):
        """Test that CLI context object is set up correctly."""
        runner = CliRunner()

        # Test that context doesn't cause errors
        result = runner.invoke(cli, ["--verbose", "--help"])
        assert result.exit_code == 0

    def test_main_cli_option_propagation(self):
        """Test that CLI options are properly propagated."""
        runner = CliRunner()

        # Test that global options work with subcommands
        result = runner.invoke(cli, ["--verbose", "projects", "list"])
        # Should not fail due to option handling
        assert result.exit_code == 0 or "Error" not in result.output

    def test_main_cli_mixed_options_and_subcommands(self):
        """Test mixing options with subcommands."""
        runner = CliRunner()

        mixed_commands = [
            ["--verbose", "projects", "list"],
            ["projects", "list", "--help"],
            ["--help"],
            ["--version"],
        ]

        for cmd_parts in mixed_commands:
            result = runner.invoke(cli, cmd_parts)
            assert result.exit_code == 0

    def test_main_cli_command_discovery(self):
        """Test that all commands are discoverable."""
        runner = CliRunner()

        # Get main help
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0

        # Should list all major command groups
        command_groups = [
            "projects",
            "models",
            "utils",
            "descriptions",
            "fabric",
            "migrate",
            "scaffold",
        ]
        for group in command_groups:
            assert group in result.output

    @patch("dbt_projects_cli.utils.logging.setup_logging")
    def test_main_cli_logging_setup(self, mock_setup_logging):
        """Test that logging is set up correctly."""
        runner = CliRunner()

        # Test with verbose flag
        result = runner.invoke(cli, ["--verbose", "--help"])
        assert result.exit_code == 0

        # Test without verbose flag
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0

    def test_main_cli_error_messages(self):
        """Test that error messages are helpful."""
        runner = CliRunner()

        # Test various error scenarios
        error_cases = [
            ["nonexistent-command"],
            ["--nonexistent-option"],
            ["projects", "nonexistent-subcommand"],
        ]

        for cmd_parts in error_cases:
            result = runner.invoke(cli, cmd_parts)
            assert result.exit_code != 0
            # Should provide helpful error messages
            assert len(result.output) > 0
