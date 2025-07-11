"""
Template engine for configuration-driven scaffolding.
Provides templating capabilities using Jinja2 and YAML configuration.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from jinja2 import BaseLoader, Environment


class ConfigTemplateEngine:
    """Template engine that uses YAML configuration for generating dbt packages."""

    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.config = self._load_config()
        self.env = Environment(loader=BaseLoader())

    def _load_config(self) -> Dict[str, Any]:
        """Load template configuration from YAML file."""
        with open(self.config_path, "r") as f:
            result = yaml.safe_load(f)
            return result if result is not None else {}

    def render_template(self, template_content: Any, context: Dict[str, Any]) -> Any:
        """Recursively render Jinja2 templates in nested structures."""
        if isinstance(template_content, str):
            # Skip dbt-specific template strings (those with target.name, etc.)
            if "target." in template_content or "var(" in template_content:
                return template_content
            # Handle Jinja2 template strings for our own templating
            if "{{" in template_content or "{%" in template_content:
                template = self.env.from_string(template_content)
                return template.render(**context)
            return template_content
        elif isinstance(template_content, dict):
            return {
                key: self.render_template(value, context)
                for key, value in template_content.items()
            }
        elif isinstance(template_content, list):
            return [self.render_template(item, context) for item in template_content]
        else:
            return template_content

    def generate_dbt_project_yml(
        self, context: Dict[str, Any], existing_config: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate dbt_project.yml content using templates."""
        templates = self.config["templates"]["dbt_project_yml"]

        # Start with base configuration
        config = templates["base_config"].copy()

        # Add base model configuration
        models_config = templates["models"]["base"].copy()

        # Add alignment-specific configuration
        alignment = context.get("alignment")
        if alignment in templates["models"]:
            alignment_config = templates["models"][alignment].copy()
            models_config.update(alignment_config)

        # Render templates with context
        config = self.render_template(config, context)
        models_config = self.render_template(models_config, context)
        vars_config = self.render_template(templates["vars"], context)

        # Build the final YAML structure
        final_config = config.copy()
        final_config["models"] = models_config
        final_config["vars"] = vars_config

        # If we have existing config, merge it preserving manual
        # additions
        if existing_config:
            final_config = self._merge_configs(final_config, existing_config, context)

        # Custom YAML dumper to handle dbt-specific formatting
        yaml_content = yaml.dump(
            final_config, default_flow_style=False, sort_keys=False
        )

        # Fix dbt variable quotes
        yaml_content = yaml_content.replace(
            "'{{ target.name != ''prod'' }}'", "\"{{ target.name != 'prod' }}\""
        )

        # Fix template keys by replacing the literal template string with
        # the actual package name
        yaml_content = yaml_content.replace(
            "'{{ package_name }}':", f"{context['package_name']}:"
        )

        return str(yaml_content)

    def generate_packages_yml(self, context: Dict[str, Any]) -> str:
        """Generate packages.yml content using templates."""
        templates = self.config["templates"]["packages_yml"]

        # Start with base packages
        packages = templates["base_packages"].copy()

        # Add alignment-specific packages
        alignment = context.get("alignment")
        if alignment in templates["alignment_specific"]:
            alignment_packages = templates["alignment_specific"][alignment]
            packages.extend(alignment_packages)

        # Render templates
        packages = self.render_template(packages, context)

        return str(yaml.dump({"packages": packages}, default_flow_style=False))

    def generate_group_yml(self, context: Dict[str, Any]) -> str:
        """Generate _group.yml content using templates."""
        template_content = self.config["templates"]["group_yml"]["template"]
        result = self.render_template(template_content, context)
        return str(result)

    def get_directory_structure(self, alignment: str) -> List[str]:
        """Get directory structure for the given alignment."""
        result = self.config["directory_structures"].get(alignment, [])
        return result if isinstance(result, list) else []

    def validate_context(self, context: Dict[str, Any]) -> List[str]:
        """Validate that required variables are present in context."""
        errors = []
        variables = self.config["variables"]

        for var_name, var_config in variables.items():
            # Check required fields
            if var_config.get("required", False) and var_name not in context:
                errors.append(f"Required variable '{var_name}' is missing")

            # Check conditional requirements
            if "required_when" in var_config:
                condition = var_config["required_when"]
                # Simple condition parsing (can be enhanced)
                if (
                    self._evaluate_condition(condition, context)
                    and var_name not in context
                ):
                    errors.append(f"Variable '{var_name}' is required when {condition}")

            # Check choices
            if var_name in context and "choices" in var_config:
                if context[var_name] not in var_config["choices"]:
                    errors.append(
                        f"Variable '{var_name}' must be one of: "
                        f"{var_config['choices']}"
                    )

        return errors

    def _evaluate_condition(self, condition: str, context: Dict[str, Any]) -> bool:
        """Simple condition evaluation (can be enhanced for more complex
        conditions).
        """
        # Handle simple equality conditions like "alignment == 'source-aligned'"
        if "==" in condition:
            var_name, value = condition.split("==")
            var_name = var_name.strip()
            value = value.strip().strip("'\"")
            return context.get(var_name) == value
        return False

    def get_migrations(self) -> List[Dict[str, Any]]:
        """Get available migrations for backfilling changes."""
        result = self.config.get("migrations", [])
        return result if isinstance(result, list) else []

    def _merge_configs(
        self,
        template_config: Dict[str, Any],
        existing_config: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Merge template configuration with existing configuration,
        preserving manual additions.
        """

        # Start with the existing configuration as the base
        merged_config = existing_config.copy()

        # Define which keys from template should always be updated
        always_update_keys = {
            "name",
            "version",
            "config-version",
            "profile",
            "model-paths",
            "analysis-paths",
            "test-paths",
            "seed-paths",
            "macro-paths",
            "snapshot-paths",
            "clean-targets",
            "flags",
        }

        # Update basic configuration keys
        for key, value in template_config.items():
            if key in always_update_keys:
                merged_config[key] = value

        # Handle models section with intelligent merging
        if "models" in template_config:
            merged_config["models"] = self._merge_models_config(
                template_config["models"], existing_config.get("models", {}), context
            )

        # Handle vars section with intelligent merging
        if "vars" in template_config:
            merged_config["vars"] = self._merge_vars_config(
                template_config["vars"], existing_config.get("vars", {})
            )

        return merged_config

    def _merge_models_config(
        self,
        template_models: Dict[str, Any],
        existing_models: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Merge models configuration, preserving existing custom model configs."""

        merged_models = existing_models.copy()
        package_name = context.get("package_name")

        # Update base model configuration (persist_docs, global tags)
        for key, value in template_models.items():
            if key.startswith("+"):
                # Handle global model configurations
                if key == "+tags":
                    # Merge tags, ensuring template tags are included
                    existing_tags = merged_models.get(key, [])
                    template_tags = value if isinstance(value, list) else [value]
                    # Add template tags that aren't already present
                    for tag in template_tags:
                        if tag not in existing_tags:
                            existing_tags.append(tag)
                    merged_models[key] = existing_tags
                # Exclude group and access from template updates - preserve
                # existing values
                elif key in {"+group", "+access"}:
                    merged_models[key] = merged_models.get(key, value)
                else:
                    # For other global configs, use template value
                    merged_models[key] = value

        # Handle package-specific model configuration
        # Check both direct package name and template key
        template_package_config = None
        if package_name and package_name in template_models:
            template_package_config = template_models[package_name]
        elif "{{ package_name }}" in template_models:
            # Handle template key that hasn't been replaced yet
            template_package_config = template_models["{{ package_name }}"]

        if template_package_config:
            existing_package_config = (
                merged_models.get(package_name, {}) if package_name else {}
            )

            # Merge package-level configuration
            merged_package_config = self._merge_package_model_config(
                template_package_config, existing_package_config
            )
            if package_name:
                merged_models[package_name] = merged_package_config

        return merged_models

    def _merge_package_model_config(
        self, template_config: Dict[str, Any], existing_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Merge package-level model configuration."""

        merged_config = existing_config.copy()

        # Template keys that should be updated/added
        template_managed_keys = {"+group", "+schema", "+materialized", "+access"}

        for key, value in template_config.items():
            if key in template_managed_keys:
                # Always update template-managed keys
                merged_config[key] = value
            elif key == "+tags":
                # Merge tags intelligently - preserve existing custom tags
                existing_tags = merged_config.get(key, [])
                template_tags = value if isinstance(value, list) else [value]

                # Start with existing tags to preserve custom ones
                merged_tags = existing_tags.copy()

                # Add template tags that aren't already present
                for tag in template_tags:
                    if tag not in merged_tags:
                        merged_tags.append(tag)

                merged_config[key] = merged_tags
            elif key not in merged_config:
                # Add new keys from template (like 'marts', 'staging' subdirs)
                merged_config[key] = value
            elif isinstance(value, dict) and isinstance(merged_config.get(key), dict):
                # Recursively merge nested configurations (like staging, marts)
                merged_config[key] = self._merge_package_model_config(
                    value, merged_config[key]
                )

        return merged_config

    def _merge_vars_config(
        self, template_vars: Dict[str, Any], existing_vars: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Merge vars configuration, preserving existing custom variables."""

        merged_vars = existing_vars.copy()

        # Template-managed variables that should always be updated
        template_managed_vars = {
            "PROD_CATALOG",
            "DATABRICKS_WORKSPACE_ID",
            "disable_dbt_artifacts_autoupload",
            "disable_dbt_columns_autoupload",
            "disable_run_results",
            "disable_tests_results",
            "disable_dbt_invocation_autoupload",
            "distant_future_timestamp",
            "local_timezone",
        }

        for key, value in template_vars.items():
            if key in template_managed_vars:
                # Always update template-managed variables
                merged_vars[key] = value
            elif key not in merged_vars:
                # Add new template variables that don't exist
                merged_vars[key] = value
            # If variable exists and is not template-managed, preserve existing value

        return merged_vars


def create_template_engine() -> ConfigTemplateEngine:
    """Factory function to create a template engine with the default config."""
    config_path = Path(__file__).parent.parent / "templates" / "template_config.yml"
    return ConfigTemplateEngine(config_path)


# Test function
def test_template_engine() -> None:
    """Test the template engine with sample data."""
    engine = create_template_engine()

    # Test context for consumer-aligned domain
    context = {
        "package_name": "marketing_customer_analytics",
        "alignment": "consumer-aligned",
        "business_area": "marketing",
        "domain_name": "customer-analytics",
        "description": "Marketing customer analytics domain package",
        "group_name": "marketing",
        "group_description": "Marketing analytics group",
    }

    print("=== Validation ===")
    errors = engine.validate_context(context)
    if errors:
        print("Validation errors:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("✅ Context validation passed")

    print("\n=== dbt_project.yml ===")
    dbt_project = engine.generate_dbt_project_yml(context)
    print(dbt_project)

    print("\n=== packages.yml ===")
    packages = engine.generate_packages_yml(context)
    print(packages)

    print("\n=== _group.yml ===")
    groups = engine.generate_group_yml(context)
    print(groups)


if __name__ == "__main__":
    test_template_engine()
