"""
Unit tests for the template engine functionality.
"""

import pytest
import yaml
from pathlib import Path
from dbt_projects_cli.core.template_engine import ConfigTemplateEngine


@pytest.mark.unit
@pytest.mark.template
class TestConfigTemplateEngine:
    """Test suite for the ConfigTemplateEngine class."""
    
    def test_template_engine_creation(self, template_engine):
        """Test that template engine can be created successfully."""
        assert template_engine is not None
        assert hasattr(template_engine, 'config')
        assert 'template_version' in template_engine.config
    
    def test_context_validation_valid(self, template_engine):
        """Test context validation with valid input."""
        context = {
            'package_name': 'test_package',
            'alignment': 'source-aligned',
            'source_system': 'databricks',
            'domain_name': 'test-domain',
            'description': 'Test package'
        }
        
        errors = template_engine.validate_context(context)
        assert errors == []
    
    def test_context_validation_missing_required(self, template_engine):
        """Test context validation with missing required fields."""
        context = {
            'alignment': 'source-aligned'
            # Missing package_name which is required
        }
        
        errors = template_engine.validate_context(context)
        assert len(errors) > 0
        assert any('package_name' in error for error in errors)
    
    def test_context_validation_conditional_requirements(self, template_engine):
        """Test conditional requirements validation."""
        context = {
            'package_name': 'test_package',
            'alignment': 'source-aligned',
            'domain_name': 'test-domain'
            # Missing source_system which is required when alignment == 'source-aligned'
        }
        
        errors = template_engine.validate_context(context)
        assert len(errors) > 0
        assert any('source_system' in error for error in errors)
    
    def test_render_template_simple_string(self, template_engine):
        """Test rendering simple template strings."""
        template = "Hello {{ name }}"
        context = {'name': 'World'}
        
        result = template_engine.render_template(template, context)
        assert result == "Hello World"
    
    def test_render_template_preserves_dbt_variables(self, template_engine):
        """Test that dbt-specific variables are preserved."""
        template = "{{ target.name != 'prod' }}"
        context = {'name': 'test'}
        
        result = template_engine.render_template(template, context)
        assert result == "{{ target.name != 'prod' }}"
    
    def test_render_template_nested_dict(self, template_engine):
        """Test rendering nested dictionary structures."""
        template = {
            'name': '{{ package_name }}',
            'config': {
                'materialized': 'table',
                'tags': ['{{ package_name }}', 'test']
            }
        }
        context = {'package_name': 'my_package'}
        
        result = template_engine.render_template(template, context)
        assert result['name'] == 'my_package'
        assert result['config']['tags'] == ['my_package', 'test']
    
    def test_generate_dbt_project_yml_basic(self, template_engine):
        """Test basic dbt_project.yml generation."""
        context = {
            'package_name': 'test_package',
            'alignment': 'source-aligned',
            'source_system': 'databricks',
            'domain_name': 'test-domain',
            'group_name': 'databricks',
            'group_description': 'Test group'
        }
        
        result = template_engine.generate_dbt_project_yml(context)
        
        # Parse the YAML to verify structure
        config = yaml.safe_load(result)
        assert config['name'] == 'test_package'
        assert config['version'] == '1.0.0'
        assert 'models' in config
        assert 'vars' in config
    
    def test_generate_packages_yml(self, template_engine):
        """Test packages.yml generation."""
        context = {
            'alignment': 'consumer-aligned',
            'package_name': 'test_package'
        }
        
        result = template_engine.generate_packages_yml(context)
        
        # Parse the YAML to verify structure
        config = yaml.safe_load(result)
        assert 'packages' in config
        assert len(config['packages']) > 0
        
        # Check for alignment-specific packages
        package_names = [pkg['package'] for pkg in config['packages']]
        assert 'dbt-labs/metrics' in package_names  # consumer-aligned specific
    
    def test_merge_configs_preserves_custom(self, template_engine):
        """Test that merge_configs preserves custom configurations."""
        template_config = {
            'name': 'test_package',
            'models': {
                '+tags': ['template_tag'],
                'test_package': {
                    '+materialized': 'table',
                    '+tags': ['package_tag']
                }
            },
            'vars': {
                'PROD_CATALOG': 'new_catalog'
            }
        }
        
        existing_config = {
            'name': 'test_package',
            'models': {
                '+tags': ['custom_tag'],
                'test_package': {
                    '+materialized': 'view',  # Should be updated
                    '+tags': ['custom_package_tag'],
                    'custom_section': {
                        '+meta': {'owner': 'team'}
                    }
                }
            },
            'vars': {
                'PROD_CATALOG': 'old_catalog',  # Should be updated
                'custom_var': 'custom_value'     # Should be preserved
            }
        }
        
        context = {'package_name': 'test_package'}
        
        result = template_engine._merge_configs(template_config, existing_config, context)
        
        # Check that custom configurations are preserved
        assert 'custom_tag' in result['models']['+tags']
        assert 'template_tag' in result['models']['+tags']
        assert 'custom_package_tag' in result['models']['test_package']['+tags']
        assert 'package_tag' in result['models']['test_package']['+tags']
        assert result['models']['test_package']['custom_section']['+meta']['owner'] == 'team'
        assert result['vars']['custom_var'] == 'custom_value'
        
        # Check that template-managed values are updated
        assert result['models']['test_package']['+materialized'] == 'table'
        assert result['vars']['PROD_CATALOG'] == 'new_catalog'
    
    def test_get_directory_structure(self, template_engine):
        """Test getting directory structure for different alignments."""
        source_dirs = template_engine.get_directory_structure('source-aligned')
        consumer_dirs = template_engine.get_directory_structure('consumer-aligned')
        utils_dirs = template_engine.get_directory_structure('utils')
        
        assert isinstance(source_dirs, list)
        assert isinstance(consumer_dirs, list)
        assert isinstance(utils_dirs, list)
        
        # Consumer-aligned should have marts directory
        assert any('marts' in directory for directory in consumer_dirs)
        
        # All should have common directories
        for dirs in [source_dirs, consumer_dirs, utils_dirs]:
            assert any('staging' in directory for directory in dirs)
            assert any('models' in directory for directory in dirs)
