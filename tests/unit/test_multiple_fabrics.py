"""
Unit tests for multiple fabrics configuration functionality.

Tests the MultipleFabricsConfig schema and related functionality.
"""

import json
import tempfile
import yaml
from pathlib import Path

import pytest
from pydantic import ValidationError

from dbt_projects_cli.core.fabric import FabricDeployer
from dbt_projects_cli.schemas.fabric import MultipleFabricsConfig, FabricConfig


class TestMultipleFabricsConfig:
    """Test the MultipleFabricsConfig schema."""
    
    def get_valid_multiple_fabrics_config(self):
        """Return a valid multiple fabrics configuration dictionary."""
        return {
            "fabrics": {
                "test-fabric-1": {
                    "fabric": {
                        "name": "test-fabric-1",
                        "description": "First test fabric",
                        "version": "1.0.0"
                    },
                    "databricks": {
                        "host": "test1.cloud.databricks.com",
                        "catalog": "test1_catalog",
                        "schema": "test1_schema"
                    },
                    "projects": {
                        "analytics": {
                            "name": "analytics",
                            "description": "Analytics project",
                            "schema": "analytics",
                            "packages": [
                                {
                                    "git": "https://github.com/dbt-labs/dbt-utils.git",
                                    "revision": "1.0.0"
                                }
                            ]
                        }
                    }
                },
                "test-fabric-2": {
                    "fabric": {
                        "name": "test-fabric-2",
                        "description": "Second test fabric",
                        "version": "1.0.0"
                    },
                    "databricks": {
                        "host": "test2.cloud.databricks.com",
                        "catalog": "test2_catalog",
                        "schema": "test2_schema"
                    },
                    "projects": {
                        "quality": {
                            "name": "quality",
                            "description": "Quality project",
                            "schema": "quality",
                            "packages": [
                                {
                                    "package": "elementary-data/elementary",
                                    "version": ">=0.16.4"
                                }
                            ]
                        }
                    }
                }
            }
        }
    
    def test_valid_multiple_fabrics_config(self):
        """Test valid multiple fabrics configuration creation."""
        config_dict = self.get_valid_multiple_fabrics_config()
        config = MultipleFabricsConfig(**config_dict)
        
        assert len(config.fabrics) == 2
        assert "test-fabric-1" in config.fabrics
        assert "test-fabric-2" in config.fabrics
        
        # Check that individual fabrics are valid
        fabric1 = config.fabrics["test-fabric-1"]
        assert fabric1.fabric.name == "test-fabric-1"
        assert fabric1.databricks.host == "test1.cloud.databricks.com"
        
        fabric2 = config.fabrics["test-fabric-2"]
        assert fabric2.fabric.name == "test-fabric-2"
        assert fabric2.databricks.host == "test2.cloud.databricks.com"
    
    def test_empty_fabrics_validation(self):
        """Test that empty fabrics dictionary is rejected."""
        config_dict = {"fabrics": {}}
        
        with pytest.raises(ValidationError) as exc_info:
            MultipleFabricsConfig(**config_dict)
        
        assert "At least one fabric must be specified" in str(exc_info.value)
    
    def test_fabric_name_mismatch_validation(self):
        """Test that fabric names must match their dictionary keys."""
        config_dict = self.get_valid_multiple_fabrics_config()
        # Change the fabric name to not match the key
        config_dict["fabrics"]["test-fabric-1"]["fabric"]["name"] = "different-name"
        
        with pytest.raises(ValidationError) as exc_info:
            MultipleFabricsConfig(**config_dict)
        
        assert "does not match key" in str(exc_info.value)
    
    def test_no_extra_fields(self):
        """Test that extra fields are not allowed."""
        config_dict = self.get_valid_multiple_fabrics_config()
        config_dict["invalid_field"] = "should not be allowed"
        
        with pytest.raises(ValidationError) as exc_info:
            MultipleFabricsConfig(**config_dict)
        
        assert "Extra inputs are not permitted" in str(exc_info.value)


class TestFabricDeployerMultipleFabrics:
    """Test FabricDeployer with multiple fabrics functionality."""
    
    def get_valid_multiple_fabrics_config(self):
        """Return a valid multiple fabrics configuration dictionary."""
        return {
            "fabrics": {
                "data-quality": {
                    "fabric": {
                        "name": "data-quality",
                        "description": "Data quality fabric",
                        "version": "1.0.0"
                    },
                    "databricks": {
                        "host": "test.cloud.databricks.com",
                        "catalog": "data_quality",
                        "schema": "monitoring"
                    },
                    "projects": {
                        "monitoring": {
                            "name": "monitoring",
                            "description": "Data quality monitoring project",
                            "schema": "monitoring",
                            "packages": [
                                {
                                    "package": "elementary-data/elementary",
                                    "version": ">=0.16.4"
                                }
                            ]
                        }
                    }
                },
                "analytics": {
                    "fabric": {
                        "name": "analytics",
                        "description": "Analytics fabric",
                        "version": "1.0.0"
                    },
                    "databricks": {
                        "host": "test.cloud.databricks.com",
                        "catalog": "analytics",
                        "schema": "core"
                    },
                    "projects": {
                        "core": {
                            "name": "core",
                            "description": "Core analytics project",
                            "schema": "core",
                            "packages": [
                                {
                                    "git": "https://github.com/dbt-labs/dbt-utils.git",
                                    "revision": "1.0.0"
                                }
                            ]
                        }
                    }
                }
            }
        }
    
    def test_load_multiple_fabrics_config_json(self, tmp_path):
        """Test loading multiple fabrics from JSON file."""
        config_dict = self.get_valid_multiple_fabrics_config()
        config_file = tmp_path / "fabrics.json"
        
        with open(config_file, 'w') as f:
            json.dump(config_dict, f)
        
        deployer = FabricDeployer()
        config = deployer.load_multiple_fabrics_config(config_file)
        
        assert isinstance(config, MultipleFabricsConfig)
        assert len(config.fabrics) == 2
    
    def test_load_multiple_fabrics_config_yaml(self, tmp_path):
        """Test loading multiple fabrics from YAML file."""
        config_dict = self.get_valid_multiple_fabrics_config()
        config_file = tmp_path / "fabrics.yml"
        
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f)
        
        deployer = FabricDeployer()
        config = deployer.load_multiple_fabrics_config(config_file)
        
        assert isinstance(config, MultipleFabricsConfig)
        assert len(config.fabrics) == 2
    
    def test_load_fabric_from_multiple(self, tmp_path):
        """Test loading a specific fabric from multiple fabrics config."""
        config_dict = self.get_valid_multiple_fabrics_config()
        config_file = tmp_path / "fabrics.json"
        
        with open(config_file, 'w') as f:
            json.dump(config_dict, f)
        
        deployer = FabricDeployer()
        fabric_config = deployer.load_fabric_from_multiple(config_file, "data-quality")
        
        assert isinstance(fabric_config, FabricConfig)
        assert fabric_config.fabric.name == "data-quality"
        assert fabric_config.databricks.host == "test.cloud.databricks.com"
    
    def test_load_fabric_from_multiple_not_found(self, tmp_path):
        """Test error when requested fabric not found."""
        config_dict = self.get_valid_multiple_fabrics_config()
        config_file = tmp_path / "fabrics.json"
        
        with open(config_file, 'w') as f:
            json.dump(config_dict, f)
        
        deployer = FabricDeployer()
        
        with pytest.raises(ValueError) as exc_info:
            deployer.load_fabric_from_multiple(config_file, "nonexistent-fabric")
        
        assert "Fabric 'nonexistent-fabric' not found" in str(exc_info.value)
        assert "data-quality, analytics" in str(exc_info.value)
    
    def test_load_fabric_from_single_config_error(self, tmp_path):
        """Test error when trying to load from single fabric config."""
        single_config = {
            "fabric": {
                "name": "single-fabric",
                "description": "Single fabric"
            },
            "databricks": {
                "host": "test.cloud.databricks.com",
                "catalog": "test",
                "schema": "test"
            },
            "projects": {
                "default": {
                    "name": "default",
                    "description": "Default project",
                    "schema": "default",
                    "packages": [
                        {
                            "git": "https://github.com/dbt-labs/dbt-utils.git",
                            "revision": "1.0.0"
                        }
                    ]
                }
            }
        }
        config_file = tmp_path / "single.yml"
        
        with open(config_file, 'w') as f:
            yaml.dump(single_config, f)
        
        deployer = FabricDeployer()
        
        with pytest.raises(ValueError) as exc_info:
            deployer.load_fabric_from_multiple(config_file, "any-fabric")
        
        assert "does not contain multiple fabrics" in str(exc_info.value)
    
    def test_list_fabrics(self, tmp_path):
        """Test listing fabrics from multiple fabrics config."""
        config_dict = self.get_valid_multiple_fabrics_config()
        config_file = tmp_path / "fabrics.json"
        
        with open(config_file, 'w') as f:
            json.dump(config_dict, f)
        
        deployer = FabricDeployer()
        fabrics_info = deployer.list_fabrics(config_file)
        
        assert len(fabrics_info) == 2
        
        # Check first fabric info
        fabric1 = next(f for f in fabrics_info if f['name'] == 'data-quality')
        assert fabric1['description'] == 'Data quality fabric'
        assert fabric1['host'] == 'test.cloud.databricks.com'
        assert fabric1['projects'] == '1'
        assert fabric1['packages'] == '1'
        
        # Check second fabric info
        fabric2 = next(f for f in fabrics_info if f['name'] == 'analytics')
        assert fabric2['description'] == 'Analytics fabric'
        assert fabric2['host'] == 'test.cloud.databricks.com'
        assert fabric2['projects'] == '1'
        assert fabric2['packages'] == '1'
    
    def test_list_fabrics_single_config_error(self, tmp_path):
        """Test error when trying to list fabrics from single config."""
        single_config = {
            "fabric": {"name": "single-fabric"},
            "databricks": {"host": "test.cloud.databricks.com", "catalog": "test", "schema": "test"},
            "projects": {
                "default": {
                    "name": "default",
                    "description": "Default project",
                    "schema": "default",
                    "packages": [{"git": "https://github.com/dbt-labs/dbt-utils.git", "revision": "1.0.0"}]
                }
            }
        }
        config_file = tmp_path / "single.yml"
        
        with open(config_file, 'w') as f:
            yaml.dump(single_config, f)
        
        deployer = FabricDeployer()
        
        with pytest.raises(ValueError) as exc_info:
            deployer.list_fabrics(config_file)
        
        assert "does not contain multiple fabrics" in str(exc_info.value)
    
    def test_load_config_multiple_fabrics_error(self, tmp_path):
        """Test error when trying to load single config from multiple fabrics file."""
        config_dict = self.get_valid_multiple_fabrics_config()
        config_file = tmp_path / "fabrics.json"
        
        with open(config_file, 'w') as f:
            json.dump(config_dict, f)
        
        deployer = FabricDeployer()
        
        with pytest.raises(ValueError) as exc_info:
            deployer.load_config(config_file)
        
        assert "contains multiple fabrics" in str(exc_info.value)
        assert "list-fabrics" in str(exc_info.value)
        assert "--fabric" in str(exc_info.value)
    
    def test_json_file_detection(self, tmp_path):
        """Test that JSON files are correctly detected."""
        deployer = FabricDeployer()
        
        json_file = tmp_path / "test.json"
        yaml_file = tmp_path / "test.yml"
        
        assert deployer._is_json_file(json_file) is True
        assert deployer._is_json_file(yaml_file) is False
    
    def test_invalid_json_error(self, tmp_path):
        """Test error handling for invalid JSON."""
        config_file = tmp_path / "invalid.json"
        
        with open(config_file, 'w') as f:
            f.write('{"invalid": json content}')  # Invalid JSON
        
        deployer = FabricDeployer()
        
        with pytest.raises(ValueError) as exc_info:
            deployer._load_file_data(config_file)
        
        assert "Invalid JSON" in str(exc_info.value)
