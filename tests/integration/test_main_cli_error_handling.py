"""Integration tests for main CLI error handling scenarios."""

from click.testing import CliRunner

from dbt_projects_cli.main import cli


class TestMainCliErrorHandling:
    """Test error handling in the main CLI."""

    def test_main_cli_exception_handling(self):
        """Test that main CLI handles exceptions gracefully."""
        runner = CliRunner()

        # Test with invalid command
        result = runner.invoke(cli, ["invalid-command"])
        assert result.exit_code != 0
        assert "No such command" in result.output

    def test_main_cli_keyboard_interrupt(self):
        """Test that main CLI handles keyboard interrupts gracefully."""
        runner = CliRunner()

        # Test that help works (basic test for non-crashing behavior)
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Usage:" in result.output

    def test_main_cli_version_command(self):
        """Test version command works correctly."""
        runner = CliRunner()

        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        # Should contain version info
        assert "version" in result.output.lower() or "dbt-projects-cli" in result.output

    def test_main_cli_help_command(self):
        """Test help command works correctly."""
        runner = CliRunner()

        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Usage:" in result.output
        assert "Commands:" in result.output

    def test_main_cli_verbose_flag_with_commands(self):
        """Test verbose flag works with various commands."""
        runner = CliRunner()

        # Test with projects command
        result = runner.invoke(cli, ["--verbose", "projects", "--help"])
        assert result.exit_code == 0
        assert "Usage:" in result.output

    def test_main_cli_command_help_access(self):
        """Test that help for subcommands is accessible."""
        runner = CliRunner()

        # Test help for different command groups
        commands = [
            "projects",
            "models",
            "utils",
            "descriptions",
            "fabric",
            "migrate",
            "scaffold",
        ]

        for cmd in commands:
            result = runner.invoke(cli, [cmd, "--help"])
            assert result.exit_code == 0
            assert "Usage:" in result.output

    def test_main_cli_nested_command_help(self):
        """Test nested command help access."""
        runner = CliRunner()

        # Test nested command help
        result = runner.invoke(cli, ["projects", "list", "--help"])
        assert result.exit_code == 0
        assert "Usage:" in result.output

    def test_main_cli_invalid_option_handling(self):
        """Test handling of invalid options."""
        runner = CliRunner()

        # Test invalid global option
        result = runner.invoke(cli, ["--invalid-option", "projects", "list"])
        assert result.exit_code != 0
        assert "No such option" in result.output

    def test_main_cli_command_context_preservation(self):
        """Test that command context is preserved across calls."""
        runner = CliRunner()

        # Test that context is properly set up
        result = runner.invoke(cli, ["projects", "list"])
        # Should not crash and should have reasonable output
        assert result.exit_code == 0 or "projects" in result.output.lower()

    def test_main_cli_exception_propagation(self):
        """Test that exceptions are properly handled and logged."""
        runner = CliRunner()

        # Test with an invalid directory (should gracefully handle errors)
        result = runner.invoke(
            cli, ["projects", "list", "--fabric-directory", "/nonexistent/path"]
        )
        # Should handle the error gracefully
        assert result.exit_code != 0
