"""
Logging utilities for the dbt-projects-cli.
"""

import logging

from rich.console import Console
from rich.logging import RichHandler

console = Console()


def setup_logging(verbose: bool = False, quiet: bool = False) -> None:
    """Configure logging for the CLI application."""
    # Configure root logger
    if quiet:
        log_level = logging.WARNING
    elif verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    # Create rich handler for console output
    handler = RichHandler(
        console=console, show_time=False, show_path=verbose, markup=True
    )

    # Configure logging format
    if verbose:
        formatter = logging.Formatter("[%(name)s] %(message)s")
    else:
        formatter = logging.Formatter("%(message)s")

    handler.setFormatter(formatter)

    # Configure root logger
    logging.basicConfig(level=log_level, handlers=[handler], force=True)

    # Suppress noisy third-party loggers unless in debug mode
    if not verbose:
        logging.getLogger("databricks").setLevel(logging.WARNING)
        logging.getLogger("openai").setLevel(logging.WARNING)
        logging.getLogger("anthropic").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)
