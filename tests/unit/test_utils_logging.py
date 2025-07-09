"""Unit tests for utils and logging modules."""

import logging
from unittest.mock import mock_open, patch

import pytest

from dbt_projects_cli.utils.logging import setup_logging
from dbt_projects_cli.utils.templates import load_template, load_template_safe


class TestLoggingUtils:
    """Test logging utility functions."""

    def test_setup_logging_debug_level(self):
        """Test setting up logging with debug level."""
        setup_logging(verbose=True)

        # Check that root logger level is set to DEBUG
        root_logger = logging.getLogger()
        assert root_logger.level == logging.DEBUG

    def test_setup_logging_info_level(self):
        """Test setting up logging with info level."""
        setup_logging(verbose=False)

        # Check that root logger level is set to INFO
        root_logger = logging.getLogger()
        assert root_logger.level == logging.INFO

    def test_logging_getLogger_functionality(self):
        """Test basic logging.getLogger functionality."""
        logger = logging.getLogger("test_module")

        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_module"

    @patch("dbt_projects_cli.utils.logging.logging.basicConfig")
    def test_setup_logging_calls_basic_config(self, mock_basic_config):
        """Test that setup_logging calls basicConfig."""
        setup_logging(verbose=True)

        mock_basic_config.assert_called_once()

    def test_setup_logging_handler_configuration(self):
        """Test that setup_logging configures handlers correctly."""
        setup_logging(verbose=True)

        root_logger = logging.getLogger()
        # Should have at least one handler
        assert len(root_logger.handlers) >= 1


class TestTemplateUtils:
    """Test template utility functions."""

    def test_load_template_success(self):
        """Test successful template loading."""
        # Mock file content
        mock_content = "Test template content"

        with patch("builtins.open", mock_open(read_data=mock_content)):
            with patch(
                "dbt_projects_cli.utils.templates.Path.exists", return_value=True
            ):
                result = load_template("test_template.txt")

                assert result == mock_content

    def test_load_template_file_not_found(self):
        """Test loading a template that does not exist."""
        with patch("builtins.open", mock_open()):
            with patch(
                "dbt_projects_cli.utils.templates.Path.exists", return_value=False
            ):
                with pytest.raises(FileNotFoundError):
                    load_template("nonexistent_template.txt")

    def test_load_template_safe_with_fallback(self):
        """Test loading a template with a fallback."""
        fallback_content = "Fallback content"

        with patch("builtins.open", mock_open()):
            with patch(
                "dbt_projects_cli.utils.templates.Path.exists", return_value=False
            ):
                result = load_template_safe(
                    "nonexistent_template.txt", fallback=fallback_content
                )

                assert result == fallback_content
