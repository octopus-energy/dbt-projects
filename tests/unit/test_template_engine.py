"""
Unit tests for the template engine functionality.
"""

import pytest
import yaml


@pytest.mark.unit
@pytest.mark.template
class TestConfigTemplateEngine:
    """Test suite for the ConfigTemplateEngine class."""

    def test_template_engine_creation(self, template_engine):
        """Test that template engine can be created successfully."""
        assert template_engine is not None
        assert hasattr(template_engine, "config")
        assert "template_version" in template_engine.config

    def test_context_validation_valid(self, template_engine):
        """Test context validation with valid input."""
        context = {
            "package_name": "test_package",
            "alignment": "source-aligned",
            "source_system": "databricks",
            "domain_name": "test-domain",
            "description": "Test package",
        }

        errors = template_engine.validate_context(context)
        assert errors == []

    def test_context_validation_missing_required(self, template_engine):
        """Test context validation with missing required fields."""
        context = {
            "alignment": "source-aligned"
            # Missing package_name which is required
        }

        errors = template_engine.validate_context(context)
        assert len(errors) > 0
        assert any("package_name" in error for error in errors)

    def test_context_validation_conditional_requirements(self, template_engine):
        """Test conditional requirements validation."""
        context = {
            "package_name": "test_package",
            "alignment": "source-aligned",
            "domain_name": "test-domain",
            # Missing source_system which is required when alignment == 'source-aligned'
        }

        errors = template_engine.validate_context(context)
        assert len(errors) > 0
        assert any("source_system" in error for error in errors)

    def test_render_template_simple_string(self, template_engine):
        """Test rendering simple template strings."""
        template = "Hello {{ name }}"
        context = {"name": "World"}

        result = template_engine.render_template(template, context)
        assert result == "Hello World"

    def test_render_template_preserves_dbt_variables(self, template_engine):
        """Test that dbt-specific variables are preserved."""
        template = "{{ target.name != 'prod' }}"
        context = {"name": "test"}

        result = template_engine.render_template(template, context)
        assert result == "{{ target.name != 'prod' }}"

    def test_render_template_nested_dict(self, template_engine):
        """Test rendering nested dictionary structures."""
        template = {
            "name": "{{ package_name }}",
            "config": {"materialized": "table", "tags": ["{{ package_name }}", "test"]},
        }
        context = {"package_name": "my_package"}

        result = template_engine.render_template(template, context)
        assert result["name"] == "my_package"
        assert result["config"]["tags"] == ["my_package", "test"]

    def test_generate_dbt_project_yml_basic(self, template_engine):
        """Test basic dbt_project.yml generation."""
        context = {
            "package_name": "test_package",
            "alignment": "source-aligned",
            "source_system": "databricks",
            "domain_name": "test-domain",
            "group_name": "databricks",
            "group_description": "Test group",
        }

        result = template_engine.generate_dbt_project_yml(context)

        # Parse the YAML to verify structure
        config = yaml.safe_load(result)
        assert config["name"] == "test_package"
        assert config["version"] == "1.0.0"
        assert "models" in config
        assert "vars" in config

    def test_generate_packages_yml(self, template_engine):
        """Test packages.yml generation."""
        context = {"alignment": "consumer-aligned", "package_name": "test_package"}

        result = template_engine.generate_packages_yml(context)

        # Parse the YAML to verify structure
        config = yaml.safe_load(result)
        assert "packages" in config
        assert len(config["packages"]) > 0

        # Check for base packages (alignment-specific is empty in current config)
        package_names = [pkg["package"] for pkg in config["packages"]]
        assert "dbt-labs/dbt_utils" in package_names  # base package
        assert "dbt-labs/codegen" in package_names  # base package

    def test_merge_configs_preserves_custom(self, template_engine):
        """Test that merge_configs preserves custom configurations."""
        template_config = {
            "name": "test_package",
            "models": {
                "+tags": ["template_tag"],
                "test_package": {"+materialized": "table", "+tags": ["package_tag"]},
            },
            "vars": {"PROD_CATALOG": "new_catalog"},
        }

        existing_config = {
            "name": "test_package",
            "models": {
                "+tags": ["custom_tag"],
                "test_package": {
                    "+materialized": "view",  # Should be updated
                    "+tags": ["custom_package_tag"],
                    "custom_section": {"+meta": {"owner": "team"}},
                },
            },
            "vars": {
                "PROD_CATALOG": "old_catalog",  # Should be updated
                "custom_var": "custom_value",  # Should be preserved
            },
        }

        context = {"package_name": "test_package"}

        result = template_engine._merge_configs(
            template_config, existing_config, context
        )

        # Check that custom configurations are preserved
        assert "custom_tag" in result["models"]["+tags"]
        assert "template_tag" in result["models"]["+tags"]
        assert "custom_package_tag" in result["models"]["test_package"]["+tags"]
        assert "package_tag" in result["models"]["test_package"]["+tags"]
        assert (
            result["models"]["test_package"]["custom_section"]["+meta"]["owner"]
            == "team"
        )
        assert result["vars"]["custom_var"] == "custom_value"

        # Check that template-managed values are updated
        assert result["models"]["test_package"]["+materialized"] == "table"
        assert result["vars"]["PROD_CATALOG"] == "new_catalog"

    def test_merge_package_model_config_preserve_existing(self, template_engine):
        """Test that merging package model configs preserves existing values
        where needed."""
        template_config = {
            "+materialized": "view",
            "+tags": ["template_tag"],
            "custom_subdir": {"+meta": {"source": "template"}},
        }

        existing_config = {
            "+materialized": "table",  # This should be replaced by template value
            "+tags": ["custom_tag"],
            "custom_subdir": {"+meta": {"ownership": "custom"}},
        }

        result = template_engine._merge_package_model_config(
            template_config, existing_config
        )

        assert result["+materialized"] == "view"
        assert "custom_tag" in result["+tags"]
        assert "template_tag" in result["+tags"]
        assert (
            result["custom_subdir"]["+meta"]["ownership"] == "custom"
        )  # Preserve custom

    def test_merge_vars_config_with_existing(self, template_engine):
        """Test that merging vars configuration preserves existing non-template vars."""
        template_vars = {"PROD_CATALOG": "new_catalog", "NEW_VAR": "important_value"}

        existing_vars = {
            "PROD_CATALOG": "old_catalog",  # Should be replaced by template value
            "CUSTOM_VAR": "custom_value",
        }

        result = template_engine._merge_vars_config(template_vars, existing_vars)

        assert result["PROD_CATALOG"] == "new_catalog"
        assert result["NEW_VAR"] == "important_value"
        assert result["CUSTOM_VAR"] == "custom_value"

    def test_get_directory_structure(self, template_engine):
        """Test getting directory structure for different alignments."""
        source_dirs = template_engine.get_directory_structure("source-aligned")
        consumer_dirs = template_engine.get_directory_structure("consumer-aligned")
        utils_dirs = template_engine.get_directory_structure("utils")

        assert isinstance(source_dirs, list)
        assert isinstance(consumer_dirs, list)
        assert isinstance(utils_dirs, list)

        # Consumer-aligned should have marts directory
        assert any("marts" in directory for directory in consumer_dirs)

        # All should have common directories
        for dirs in [source_dirs, consumer_dirs, utils_dirs]:
            assert any("staging" in directory for directory in dirs)
            assert any("models" in directory for directory in dirs)

    def test_context_validation_invalid_choice(self, template_engine):
        """Test context validation with invalid choice values."""
        context = {
            "package_name": "test_package",
            "alignment": "invalid-alignment",  # Not in allowed choices
            "source_system": "databricks",
            "domain_name": "test-domain",
            "description": "Test package",
        }

        errors = template_engine.validate_context(context)
        assert len(errors) > 0
        assert any(
            "alignment" in error and "must be one of" in error for error in errors
        )

    def test_evaluate_condition_with_complex_condition(self, template_engine):
        """Test condition evaluation with conditions that don't match pattern."""
        context = {"alignment": "source-aligned"}

        # Test non-equality condition (should return False)
        result = template_engine._evaluate_condition(
            "alignment != source-aligned", context
        )
        assert result is False

        # Test equality condition
        result = template_engine._evaluate_condition(
            "alignment == 'source-aligned'", context
        )
        assert result is True

    def test_generate_group_yml_content(self, template_engine):
        """Test group.yml generation with template content."""
        context = {
            "package_name": "marketing_customer_analytics",
            "group_name": "marketing_customer_analytics",
            "group_description": "Marketing analytics group",
            "owner_name": "Marketing Team",
            "owner_email": "marketing@example.com",
            "team_name": "marketing",
            "team_description": "Marketing analytics team",
            "team_contact": "Marketing Team Lead",
            "team_domains": ["marketing", "campaigns"]
        }

        result = template_engine.generate_group_yml(context)
        assert "marketing_customer_analytics" in result
        assert "Marketing analytics group" in result
        assert "marketing" in result  # team name
        assert "Marketing Team" in result  # owner name

    def test_get_migrations_returns_list(self, template_engine):
        """Test that get_migrations returns a list."""
        migrations = template_engine.get_migrations()
        assert isinstance(migrations, list)

    def test_merge_models_config_with_template_key(self, template_engine):
        """Test merging models config when template key is not replaced."""
        template_models = {
            "+tags": ["global_tag"],
            "{{ package_name }}": {"+materialized": "table"},
        }

        existing_models = {
            "+tags": ["existing_tag"],
            "test_package": {"+materialized": "view"},
        }

        context = {"package_name": "test_package"}

        result = template_engine._merge_models_config(
            template_models, existing_models, context
        )

        assert "existing_tag" in result["+tags"]
        assert "global_tag" in result["+tags"]
        # Should handle the template key
        assert "test_package" in result

    def test_merge_models_config_with_group_access(self, template_engine):
        """Test that group and access are preserved from existing config."""
        template_models = {"+group": "new_group", "+access": "public"}

        existing_models = {"+group": "existing_group", "+access": "private"}

        result = template_engine._merge_models_config(
            template_models, existing_models, {}
        )

        # Should preserve existing values for group and access
        assert result["+group"] == "existing_group"
        assert result["+access"] == "private"

    def test_merge_vars_config_with_non_managed_var(self, template_engine):
        """Test that non-template-managed vars are preserved when they exist."""
        template_vars = {
            "PROD_CATALOG": "new_catalog",  # Template managed
            "existing_var": "new_value",  # Not template managed
        }

        existing_vars = {
            "PROD_CATALOG": "old_catalog",
            "existing_var": "old_value",  # Should be preserved
            "custom_var": "custom_value",
        }

        result = template_engine._merge_vars_config(template_vars, existing_vars)

        assert result["PROD_CATALOG"] == "new_catalog"  # Updated
        assert result["existing_var"] == "old_value"  # Preserved
        assert result["custom_var"] == "custom_value"  # Preserved
