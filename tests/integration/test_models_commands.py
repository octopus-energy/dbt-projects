"""
Integration tests for models CLI commands.

Tests the CLI commands end-to-end with real dbt projects.
"""

import tempfile
from pathlib import Path
from unittest.mock import patch

import yaml
from click.testing import CliRunner

from dbt_projects_cli.commands.models import models


class TestModelsCommands:
    """Test models CLI commands."""

    def create_test_dbt_project(self, tmp_path, project_name="test_project"):
        """Create a minimal dbt project for testing."""
        project_dir = tmp_path / project_name
        project_dir.mkdir()
        
        # Create dbt_project.yml
        dbt_project = {
            "name": project_name,
            "version": "1.0.0",
            "profile": "test_profile",
            "model-paths": ["models"],
            "target-path": "target",
        }
        
        with open(project_dir / "dbt_project.yml", "w") as f:
            yaml.dump(dbt_project, f)
        
        # Create models directory and test models
        models_dir = project_dir / "models"
        models_dir.mkdir()
        
        # Create multiple test models
        test_model_1 = """
select
    id,
    name,
    email,
    created_at
from {{ source('raw', 'users') }}
where status = 'active'
"""
        
        test_model_2 = """
select
    user_id,
    order_date,
    total_amount
from {{ ref('staging_orders') }}
where order_date >= '2023-01-01'
"""
        
        with open(models_dir / "users.sql", "w") as f:
            f.write(test_model_1)
        
        with open(models_dir / "orders.sql", "w") as f:
            f.write(test_model_2)
        
        return project_dir

    def test_models_command_group(self):
        """Test that models command group is accessible."""
        runner = CliRunner()
        result = runner.invoke(models, ["--help"])

        assert result.exit_code == 0
        assert "Commands for managing dbt models" in result.output
        assert "list" in result.output
        assert "analyze" in result.output

    def test_list_models_specific_project(self, tmp_path):
        """Test listing models for a specific project."""
        # Create a test dbt project
        project_dir = self.create_test_dbt_project(tmp_path, "test_project")
        
        runner = CliRunner()
        with patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery") as mock_discovery:
            # Mock project discovery
            mock_instance = mock_discovery.return_value
            mock_instance.root_path = tmp_path
            mock_instance.list_models_in_project.return_value = [
                project_dir / "models" / "users.sql",
                project_dir / "models" / "orders.sql",
            ]
            
            result = runner.invoke(models, ["list", "--project", "test_project"])

        assert result.exit_code == 0
        assert "Models in project 'test_project'" in result.output
        assert "users.sql" in result.output
        assert "orders.sql" in result.output

    def test_list_models_no_models_found(self, tmp_path):
        """Test listing models when no models are found."""
        runner = CliRunner()
        with patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery") as mock_discovery:
            # Mock project discovery
            mock_instance = mock_discovery.return_value
            mock_instance.list_models_in_project.return_value = []
            
            result = runner.invoke(models, ["list", "--project", "empty_project"])

        assert result.exit_code == 0
        assert "No models found for project 'empty_project'" in result.output

    def test_list_all_models(self, tmp_path):
        """Test listing all models across all projects."""
        # Create test dbt projects
        project1_dir = self.create_test_dbt_project(tmp_path, "project1")
        project2_dir = self.create_test_dbt_project(tmp_path, "project2")
        
        runner = CliRunner()
        with patch("dbt_projects_cli.core.project_discovery.ProjectDiscovery") as mock_discovery:
            # Mock project discovery
            mock_instance = mock_discovery.return_value
            mock_instance.root_path = tmp_path
            mock_instance.discover_all_projects.return_value = {
                "packages": [
                    {"name": "project1"},
                    {"name": "project2"}
                ],
                "fabrics": []
            }
            
            def mock_list_models(project_name):
                if project_name == "project1":
                    return [
                        project1_dir / "models" / "users.sql",
                        project1_dir / "models" / "orders.sql"
                    ]
                elif project_name == "project2":
                    return [project2_dir / "models" / "users.sql"]
                return []
            
            mock_instance.list_models_in_project.side_effect = mock_list_models
            
            result = runner.invoke(models, ["list"])

        assert result.exit_code == 0
        assert "All dbt Models" in result.output

    def test_analyze_model_file_exists(self, tmp_path):
        """Test analyzing a model file that exists."""
        # Create a test model file
        test_model_path = tmp_path / "test_model.sql"
        test_model_content = """
-- Test model with various SQL features
select
    id,
    name,
    email,
    created_at
from {{ ref('source_table') }}
left join {{ ref('other_table') }} ot on ot.id = id
where status = 'active'
  and created_at >= '2023-01-01'
group by id, name, email, created_at
having count(*) > 1
order by created_at desc
"""
        
        with open(test_model_path, "w") as f:
            f.write(test_model_content)
        
        runner = CliRunner()
        result = runner.invoke(models, ["analyze", str(test_model_path)])

        assert result.exit_code == 0
        assert f"Analyzing model: {test_model_path.name}" in result.output
        assert "Lines of code:" in result.output
        assert "✓ Contains SELECT statements" in result.output
        assert "✓ Contains FROM clauses" in result.output
        assert "✓ Contains WHERE conditions" in result.output
        assert "✓ Contains GROUP BY clauses" in result.output
        assert "✓ Contains JOIN operations" in result.output
        assert "✓ Contains dbt Jinja templating" in result.output
        assert "✓ Contains dbt ref() functions" in result.output

    def test_analyze_model_file_not_exists(self, tmp_path):
        """Test analyzing a model file that doesn't exist."""
        nonexistent_path = tmp_path / "nonexistent_model.sql"
        
        runner = CliRunner()
        result = runner.invoke(models, ["analyze", str(nonexistent_path)])

        assert result.exit_code == 0
        # The output may wrap, so just check for the key parts
        assert "Model file" in result.output
        assert "not found" in result.output
        assert "nonexistent_model.sql" in result.output

    def test_analyze_model_with_sources(self, tmp_path):
        """Test analyzing a model that uses source() functions."""
        # Create a test model file with source() functions
        test_model_path = tmp_path / "source_model.sql"
        test_model_content = """
select
    u.id,
    u.name,
    p.title
from {{ source('raw', 'users') }} u
left join {{ ref('products') }} p on p.user_id = u.id
where u.created_at > '2023-01-01'
"""
        
        with open(test_model_path, "w") as f:
            f.write(test_model_content)
        
        runner = CliRunner()
        result = runner.invoke(models, ["analyze", str(test_model_path)])

        assert result.exit_code == 0
        assert "✓ Contains SELECT statements" in result.output
        assert "✓ Contains FROM clauses" in result.output
        assert "✓ Contains JOIN operations" in result.output
        assert "✓ Contains WHERE conditions" in result.output
        assert "✓ Contains dbt Jinja templating" in result.output
        assert "✓ Contains dbt ref() functions" in result.output
        assert "✓ Contains dbt source() functions" in result.output

    def test_analyze_simple_model(self, tmp_path):
        """Test analyzing a simple model without complex features."""
        # Create a very simple model file
        test_model_path = tmp_path / "simple_model.sql"
        test_model_content = "SELECT 1 as simple_column, 'test' as name"
        
        with open(test_model_path, "w") as f:
            f.write(test_model_content)
        
        runner = CliRunner()
        result = runner.invoke(models, ["analyze", str(test_model_path)])

        assert result.exit_code == 0
        assert "✓ Contains SELECT statements" in result.output
        # Should not contain complex features
        assert "✓ Contains JOIN operations" not in result.output
        assert "✓ Contains WHERE conditions" not in result.output
        assert "✓ Contains GROUP BY clauses" not in result.output
        assert "✓ Contains dbt Jinja templating" not in result.output

    def test_analyze_model_with_ctes(self, tmp_path):
        """Test analyzing a model with CTEs."""
        # Create a model with CTEs
        test_model_path = tmp_path / "cte_model.sql"
        test_model_content = """
with active_users as (
    select id, name, email
    from {{ source('raw', 'users') }}
    where status = 'active'
),
user_orders as (
    select user_id, count(*) as order_count
    from {{ ref('orders') }}
    group by user_id
)
select 
    au.id,
    au.name,
    au.email,
    coalesce(uo.order_count, 0) as total_orders
from active_users au
left join user_orders uo on uo.user_id = au.id
"""
        
        with open(test_model_path, "w") as f:
            f.write(test_model_content)
        
        runner = CliRunner()
        result = runner.invoke(models, ["analyze", str(test_model_path)])

        assert result.exit_code == 0
        assert "✓ Contains SELECT statements" in result.output
        assert "✓ Contains FROM clauses" in result.output
        assert "✓ Contains WHERE conditions" in result.output
        assert "✓ Contains GROUP BY clauses" in result.output
        assert "✓ Contains JOIN operations" in result.output
        assert "✓ Contains dbt Jinja templating" in result.output
        assert "✓ Contains dbt ref() functions" in result.output
        assert "✓ Contains dbt source() functions" in result.output
