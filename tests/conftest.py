"""
Centralized fixtures for testing the dbt-projects-cli.
Provides common setup and teardown for different test types.
"""

import pytest
from click.testing import CliRunner
from pathlib import Path
from dbt_projects_cli.main import cli
from dbt_projects_cli.core.template_engine import create_template_engine


@pytest.fixture(scope="session")
def cli_runner():
    """Fixture for invoking CLI commands in tests."""
    return CliRunner()


@pytest.fixture(scope="module")
def template_engine():
    """Fixture for accessing the template engine."""
    return create_template_engine()


@pytest.fixture(scope="function")
def mock_fs(fs):
    """Fixture for creating a mock file system for isolated tests."""
    
    # Set up mock home and project directories
    fs.create_dir("/home/user")
    fs.create_dir("/home/user/dbt-projects")
    
    # Create template configuration file
    template_config_path = Path("src/dbt_projects_cli/templates/template_config.yml")
    fs.create_file(
        template_config_path,
        contents="""template_version: "1.0.0"
variables:
  package_name:
    type: string
    required: true

templates:
  dbt_project_yml:
    base_config:
      name: "{{ package_name }}"
      version: "1.0.0"
    models:
      base:
        +tags: ["{{ package_name }}"]
    vars:
      my_var: "my_value"""
    )
    
    yield fs


@pytest.fixture(scope="function")
def test_project(mock_fs):
    """Fixture for setting up a test project with some dbt files."""
    
    project_dir = Path("/home/user/dbt-projects/packages/domains/consumer-aligned/marketing/customer-analytics")
    mock_fs.create_dir(project_dir)
    
    # Create a dbt_project.yml with some custom config
    dbt_project_content = """name: marketing_customer_analytics
version: 1.0.0
models:
  +tags: [marketing]
  custom_model:
    +materialized: table"""
    mock_fs.create_file(project_dir / "dbt_project.yml", contents=dbt_project_content)
    
    # Create a packages.yml
    packages_content = """packages:
- package: dbt-labs/dbt_utils
  version: '>=1.3.0'"""
    mock_fs.create_file(project_dir / "packages.yml", contents=packages_content)
    
    return {
        "project_dir": project_dir,
        "dbt_project_content": dbt_project_content,
        "packages_content": packages_content
    }


@pytest.fixture
def isolated_cli_runner(cli_runner):
    """Fixture to run CLI commands in an isolated file system."""
    
    def run_cli_with_fs(*args, **kwargs):
        with cli_runner.isolated_filesystem():
            return cli_runner.invoke(cli, *args, **kwargs)
    
    return run_cli_with_fs
