"""
Integration tests for migration commands.
These tests verify the migration functionality preserves custom configurations.
"""

import pytest
import os
import tempfile
import shutil
import yaml
from pathlib import Path
from click.testing import CliRunner
from dbt_projects_cli.main import cli


@pytest.mark.integration
@pytest.mark.migration
class TestMigrationCommands:
    """Test suite for migration commands integration."""
    
    @pytest.fixture(autouse=True)
    def setup_test_env(self):
        """Set up a temporary directory for each test."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.original_cwd = os.getcwd()
        os.chdir(self.test_dir)
        yield
        os.chdir(self.original_cwd)
        shutil.rmtree(self.test_dir)
    
    def create_test_package(self, package_path: Path, custom_config: dict = None):
        """Helper to create a test package with optional custom config."""
        package_path.mkdir(parents=True, exist_ok=True)
        
        default_config = {
            'name': 'test_package',
            'version': '1.0.0',
            'models': {
                '+tags': ['test_package']
            },
            'vars': {
                'PROD_CATALOG': 'old_catalog'
            }
        }
        
        if custom_config:
            # Deep merge custom config
            for key, value in custom_config.items():
                if key in default_config and isinstance(value, dict):
                    default_config[key].update(value)
                else:
                    default_config[key] = value
        
        with open(package_path / 'dbt_project.yml', 'w') as f:
            yaml.dump(default_config, f)
    
    def test_migrate_package_dry_run(self):
        """Test migration dry run doesn't change files."""
        runner = CliRunner()
        
        # Create a test package
        package_path = Path('packages/domains/consumer-aligned/marketing/test-package')
        self.create_test_package(package_path, {
            'name': 'marketing_test_package',
            'models': {
                '+tags': ['marketing_test_package', 'custom_tag'],
                'custom_section': {
                    '+materialized': 'view'
                }
            },
            'vars': {
                'custom_var': 'custom_value'
            }
        })
        
        # Run migration dry run
        result = runner.invoke(cli, [
            'migrate', 'package',
            '--package-path', str(package_path),
            '--dry-run'
        ])
        
        assert result.exit_code == 0
        assert 'This was a dry run' in result.output
        
        # Verify original file is unchanged
        with open(package_path / 'dbt_project.yml', 'r') as f:
            config = yaml.safe_load(f)
            assert 'custom_tag' in config['models']['+tags']
            assert 'custom_section' in config['models']
            assert config['vars']['custom_var'] == 'custom_value'
    
    def test_migrate_package_preserves_custom_config(self):
        """Test that migration preserves custom configurations."""
        runner = CliRunner()
        
        # Create a test package with custom config
        package_path = Path('packages/domains/consumer-aligned/marketing/test-package')
        self.create_test_package(package_path, {
            'name': 'marketing_test_package',
            'models': {
                '+tags': ['marketing_test_package', 'custom_global_tag'],
                'marketing_test_package': {
                    '+tags': ['custom_package_tag'],
                    'custom_models': {
                        '+materialized': 'view',
                        '+meta': {'owner': 'marketing_team'}
                    }
                }
            },
            'vars': {
                'custom_var': 'custom_value',
                'marketing_threshold': 100
            }
        })
        
        # Run migration
        result = runner.invoke(cli, [
            'migrate', 'package',
            '--package-path', str(package_path),
            '--force'
        ])
        
        assert result.exit_code == 0
        assert 'migrated successfully' in result.output
        
        # Verify backup was created
        assert (package_path / 'dbt_project.yml.bak').exists()
        
        # Verify migrated file preserves custom config
        with open(package_path / 'dbt_project.yml', 'r') as f:
            config = yaml.safe_load(f)
            
            # Custom configurations should be preserved
            assert 'custom_global_tag' in config['models']['+tags']
            assert 'custom_package_tag' in config['models']['marketing_test_package']['+tags']
            assert 'custom_models' in config['models']['marketing_test_package']
            assert config['models']['marketing_test_package']['custom_models']['+meta']['owner'] == 'marketing_team'
            assert config['vars']['custom_var'] == 'custom_value'
            assert config['vars']['marketing_threshold'] == 100
            
            # Template configurations should be added/updated
            assert 'consumer-aligned' in config['models']['marketing_test_package']['+tags']
            assert 'marketing' in config['models']['marketing_test_package']['+tags']
            assert config['models']['marketing_test_package']['+group'] == 'marketing'
            assert config['vars']['PROD_CATALOG'] == 'octoenergy_data_prod_prod'
    
    def test_migrate_all_packages_dry_run(self):
        """Test migrating all packages with dry run."""
        runner = CliRunner()
        
        # Create multiple test packages
        packages = [
            ('packages/domains/source-aligned/databricks/test-data', 'databricks_test_data'),
            ('packages/domains/consumer-aligned/marketing/analytics', 'marketing_analytics'),
            ('packages/utils/test-utils', 'utils_test_utils')
        ]
        
        for package_path, package_name in packages:
            self.create_test_package(Path(package_path), {'name': package_name})
        
        # Run migration on all packages
        result = runner.invoke(cli, [
            'migrate', 'all',
            '--dry-run'
        ])
        
        assert result.exit_code == 0
        assert 'Packages to Migrate' in result.output
        assert 'This was a dry run' in result.output
        
        # Verify all packages are listed
        for _, package_name in packages:
            assert package_name in result.output
    
    def test_migrate_all_packages_by_domain_type(self):
        """Test migrating packages filtered by domain type."""
        runner = CliRunner()
        
        # Create packages of different types
        packages = [
            ('packages/domains/source-aligned/databricks/test-data', 'databricks_test_data'),
            ('packages/domains/consumer-aligned/marketing/analytics', 'marketing_analytics'),
            ('packages/utils/test-utils', 'utils_test_utils')
        ]
        
        for package_path, package_name in packages:
            self.create_test_package(Path(package_path), {'name': package_name})
        
        # Migrate only consumer-aligned packages
        result = runner.invoke(cli, [
            'migrate', 'all',
            '--domain-type', 'consumer-aligned',
            '--force'
        ])
        
        assert result.exit_code == 0
        assert 'marketing_analytics' in result.output
        assert 'migrated successfully' in result.output
        
        # Verify only consumer-aligned package was migrated
        consumer_path = Path('packages/domains/consumer-aligned/marketing/analytics')
        assert (consumer_path / 'dbt_project.yml.bak').exists()
        
        # Other packages should not have backups
        source_path = Path('packages/domains/source-aligned/databricks/test-data')
        assert not (source_path / 'dbt_project.yml.bak').exists()
    
    def test_migrate_package_missing_path_error(self):
        """Test migration fails gracefully with missing package path."""
        runner = CliRunner()
        
        result = runner.invoke(cli, [
            'migrate', 'package',
            '--package-path', 'nonexistent/path',
            '--force'
        ])
        
        assert result.exit_code == 0
        assert 'does not exist' in result.output
    
    def test_migrate_package_no_dbt_project_error(self):
        """Test migration fails gracefully when no dbt_project.yml exists."""
        runner = CliRunner()
        
        # Create directory without dbt_project.yml
        package_path = Path('packages/test-package')
        package_path.mkdir(parents=True)
        
        result = runner.invoke(cli, [
            'migrate', 'package',
            '--package-path', str(package_path),
            '--force'
        ])
        
        assert result.exit_code == 0
        assert 'No dbt_project.yml found' in result.output
    
    def test_migrate_creates_groups_yml(self):
        """Test that migration creates groups.yml if it doesn't exist."""
        runner = CliRunner()
        
        # Create a test package without groups.yml
        package_path = Path('packages/domains/consumer-aligned/marketing/test-package')
        self.create_test_package(package_path, {'name': 'marketing_test_package'})
        
        # Run migration
        result = runner.invoke(cli, [
            'migrate', 'package',
            '--package-path', str(package_path),
            '--force'
        ])
        
        assert result.exit_code == 0
        
        # Verify _group.yml was created
        groups_path = package_path / 'groups/_group.yml'
        assert groups_path.exists()
        
        with open(groups_path, 'r') as f:
            content = f.read()
            assert 'marketing' in content
            assert 'Data Platform Team' in content
