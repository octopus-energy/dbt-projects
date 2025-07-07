"""
Unit tests for fabric configuration schemas.

Tests the Pydantic models used for validating fabric configurations
with the new multi-project schema structure.
"""

import pytest
from pydantic import ValidationError

from dbt_projects_cli.schemas.fabric import (
    FabricConfig,
    FabricMetadata,
    DatabricksConnection,
    ProjectConfig,
    GitPackage,
    LocalPackage,
    HubPackage,
    MultipleFabricsConfig,
    LegacyFabricConfig,
)


class TestFabricMetadata:
    """Test the FabricMetadata schema."""
    
    def test_valid_fabric_metadata(self):
        """Test valid fabric metadata creation."""
        metadata = FabricMetadata(
            name="test-fabric",
            description="A test fabric",
            version="1.0.0",
            dbt_version="1.7.0"
        )
        
        assert metadata.name == "test-fabric"
        assert metadata.description == "A test fabric"
        assert metadata.version == "1.0.0"
        assert metadata.dbt_version == "1.7.0"
    
    def test_fabric_metadata_defaults(self):
        """Test that default values are applied correctly."""
        metadata = FabricMetadata(name="test-fabric")
        
        assert metadata.name == "test-fabric"
        assert metadata.description is None
        assert metadata.version == "1.0.0"
        assert metadata.dbt_version == "1.7.0"
    
    def test_fabric_metadata_requires_name(self):
        """Test that name is required."""
        with pytest.raises(ValidationError) as exc_info:
            FabricMetadata()
        
        assert "name" in str(exc_info.value)


class TestDatabricksConnection:
    """Test the DatabricksConnection schema."""
    
    def test_valid_databricks_connection(self):
        """Test valid Databricks connection creation."""
        connection = DatabricksConnection(
            host="my-workspace.cloud.databricks.com",
            auth_type="oauth"
        )
        
        assert connection.host == "my-workspace.cloud.databricks.com"
        assert connection.auth_type == "oauth"
    
    def test_databricks_connection_defaults(self):
        """Test default values for optional fields."""
        connection = DatabricksConnection(
            host="my-workspace.cloud.databricks.com"
        )
        
        assert connection.auth_type == "oauth"
        assert connection.http_path is None
    
    def test_invalid_databricks_host(self):
        """Test that invalid hostnames are rejected."""
        with pytest.raises(ValidationError) as exc_info:
            DatabricksConnection(host="invalid-host.com")
        
        assert "Host must be a valid Databricks hostname" in str(exc_info.value)
    
    def test_valid_azure_databricks_host(self):
        """Test that Azure Databricks hostnames are accepted."""
        connection = DatabricksConnection(
            host="my-workspace.azuredatabricks.net"
        )
        
        assert connection.host == "my-workspace.azuredatabricks.net"


class TestPackageSchemas:
    """Test package schema classes."""
    
    def test_git_package(self):
        """Test GitPackage schema."""
        package = GitPackage(
            git="https://github.com/dbt-labs/dbt-utils.git",
            subdirectory="some/path",
            revision="v1.0.0",
            warn_unpinned=False
        )
        
        assert package.git == "https://github.com/dbt-labs/dbt-utils.git"
        assert package.subdirectory == "some/path"
        assert package.revision == "v1.0.0"
        assert package.warn_unpinned is False
    
    def test_git_package_minimal(self):
        """Test GitPackage with only required fields."""
        package = GitPackage(git="https://github.com/dbt-labs/dbt-utils.git")
        
        assert package.git == "https://github.com/dbt-labs/dbt-utils.git"
        assert package.subdirectory is None
        assert package.revision is None
        assert package.warn_unpinned is True  # Default value
    
    def test_local_package(self):
        """Test LocalPackage schema."""
        package = LocalPackage(local="../packages/my-package")
        
        assert package.local == "../packages/my-package"
    
    def test_hub_package(self):
        """Test HubPackage schema."""
        package = HubPackage(
            package="elementary-data/elementary",
            version=">=0.16.4"
        )
        
        assert package.package == "elementary-data/elementary"
        assert package.version == ">=0.16.4"


class TestProjectConfig:
    """Test the ProjectConfig schema."""
    
    def get_valid_project_dict(self):
        """Return a valid project configuration dictionary."""
        return {
            "name": "test-project",
            "description": "A test project",
            "schema": "test_schema",
            "packages": [
                {
                    "git": "https://github.com/dbt-labs/dbt-utils.git",
                    "revision": "1.0.0"
                },
                {
                    "package": "elementary-data/elementary",
                    "version": ">=0.16.4"
                }
            ],
            "vars": {
                "timezone": "UTC"
            },
            "tags": ["analytics", "core"]
        }
    
    def test_valid_project_config(self):
        """Test valid project configuration creation."""
        project_dict = self.get_valid_project_dict()
        project = ProjectConfig(**project_dict)
        
        assert project.name == "test-project"
        assert project.description == "A test project"
        assert project.schema_name == "test_schema"
        assert len(project.packages) == 2
        assert project.vars["timezone"] == "UTC"
        assert "analytics" in project.tags
    
    def test_project_config_empty_packages_validation(self):
        """Test that empty packages list is rejected."""
        project_dict = self.get_valid_project_dict()
        project_dict["packages"] = []
        
        with pytest.raises(ValidationError) as exc_info:
            ProjectConfig(**project_dict)
        
        assert "At least one package must be specified" in str(exc_info.value)
    
    def test_project_config_defaults(self):
        """Test that default values are applied correctly."""
        project_dict = {
            "name": "test-project",
            "schema": "test_schema",
            "packages": [
                {
                    "package": "elementary-data/elementary",
                    "version": ">=0.16.4"
                }
            ]
        }
        project = ProjectConfig(**project_dict)
        
        assert project.vars == {}
        assert project.models == {}
        assert project.tags == []
        assert project.catalog is None


class TestFabricConfig:
    """Test the main FabricConfig schema."""
    
    def get_valid_config_dict(self):
        """Return a valid configuration dictionary for testing."""
        return {
            "fabric": {
                "name": "octoenergy_data_prod",
                "description": "Octopus Energy data platform fabric"
            },
            "databricks": {
                "host": "octopus-workspace.cloud.databricks.com",
                "auth_type": "oauth"
            },
            "projects": {
                "core": {
                    "name": "core",
                    "description": "Core analytics transformations",
                    "schema": "core",
                    "packages": [
                        {
                            "package": "elementary-data/elementary",
                            "version": ">=0.16.4"
                        },
                        {
                            "git": "https://github.com/octopus/local-dbx-info.git",
                            "revision": "main"
                        }
                    ],
                    "vars": {
                        "timezone": "UTC"
                    },
                    "tags": ["core", "transformations"]
                }
            }
        }
    
    def test_valid_fabric_config(self):
        """Test valid fabric configuration creation."""
        config_dict = self.get_valid_config_dict()
        config = FabricConfig(**config_dict)
        
        assert config.fabric.name == "octoenergy_data_prod"
        assert config.databricks.host == "octopus-workspace.cloud.databricks.com"
        assert len(config.projects) == 1
        assert "core" in config.projects
        
        core_project = config.projects["core"]
        assert core_project.name == "core"
        assert len(core_project.packages) == 2
        assert core_project.vars["timezone"] == "UTC"
    
    def test_fabric_config_with_multiple_projects(self):
        """Test fabric configuration with multiple projects."""
        config_dict = self.get_valid_config_dict()
        config_dict["projects"]["data-quality"] = {
            "name": "data-quality",
            "description": "Data quality monitoring",
            "schema": "data_quality",
            "packages": [
                {
                    "git": "https://github.com/octopus/dq-package.git",
                    "revision": "v1.0.0"
                }
            ]
        }
        
        config = FabricConfig(**config_dict)
        
        assert len(config.projects) == 2
        assert "core" in config.projects
        assert "data-quality" in config.projects
        
        dq_project = config.projects["data-quality"]
        assert dq_project.name == "data-quality"
        assert dq_project.schema_name == "data_quality"
    
    def test_fabric_config_empty_projects_validation(self):
        """Test that empty projects dict is rejected."""
        config_dict = self.get_valid_config_dict()
        config_dict["projects"] = {}
        
        with pytest.raises(ValidationError) as exc_info:
            FabricConfig(**config_dict)
        
        assert "At least one project must be specified" in str(exc_info.value)
    
    def test_fabric_config_project_name_mismatch(self):
        """Test that project names must match their dictionary keys."""
        config_dict = self.get_valid_config_dict()
        config_dict["projects"]["core"]["name"] = "different-name"
        
        with pytest.raises(ValidationError) as exc_info:
            FabricConfig(**config_dict)
        
        assert "Project name 'different-name' does not match key 'core'" in str(exc_info.value)
    
    def test_fabric_config_no_extra_fields(self):
        """Test that extra fields are not allowed."""
        config_dict = self.get_valid_config_dict()
        config_dict["invalid_field"] = "should not be allowed"
        
        with pytest.raises(ValidationError) as exc_info:
            FabricConfig(**config_dict)
        
        assert "Extra inputs are not permitted" in str(exc_info.value)
    
    def test_fabric_config_missing_required_fields(self):
        """Test that missing required fields are caught."""
        with pytest.raises(ValidationError) as exc_info:
            FabricConfig(fabric={"name": "test"})  # Missing databricks and projects
        
        error_str = str(exc_info.value)
        assert "databricks" in error_str
        assert "projects" in error_str
    
    def test_fabric_config_defaults(self):
        """Test that default values are applied correctly."""
        config_dict = self.get_valid_config_dict()
        config = FabricConfig(**config_dict)
        
        assert config.tags == []  # Default empty list
    
    def test_get_catalog_name(self):
        """Test automatic catalog name generation."""
        config_dict = self.get_valid_config_dict()
        config = FabricConfig(**config_dict)
        
        assert config.get_catalog_name("dev") == "octoenergy_data_prod_data_prod_test"
        assert config.get_catalog_name("prod") == "octoenergy_data_prod_data_prod_prod"
        assert config.get_catalog_name("source") == "octoenergy_data_prod_data_prod_source"


class TestMultipleFabricsConfig:
    """Test the MultipleFabricsConfig schema."""
    
    def get_valid_multiple_fabrics_dict(self):
        """Return a valid multiple fabrics configuration dictionary."""
        return {
            "fabrics": {
                "octoenergy_data_prod": {
                    "fabric": {
                        "name": "octoenergy_data_prod",
                        "description": "Octopus Energy production data fabric"
                    },
                    "databricks": {
                        "host": "octopus-workspace.cloud.databricks.com",
                        "auth_type": "oauth"
                    },
                    "projects": {
                        "core": {
                            "name": "core",
                            "description": "Core analytics transformations",
                            "schema": "core",
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
        config_dict = self.get_valid_multiple_fabrics_dict()
        config = MultipleFabricsConfig(**config_dict)
        
        assert len(config.fabrics) == 1
        assert "octoenergy_data_prod" in config.fabrics
        
        fabric = config.fabrics["octoenergy_data_prod"]
        assert fabric.fabric.name == "octoenergy_data_prod"
        assert len(fabric.projects) == 1
    
    def test_multiple_fabrics_empty_validation(self):
        """Test that empty fabrics dict is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            MultipleFabricsConfig(fabrics={})
        
        assert "At least one fabric must be specified" in str(exc_info.value)
    
    def test_multiple_fabrics_name_mismatch(self):
        """Test that fabric names must match their dictionary keys."""
        config_dict = self.get_valid_multiple_fabrics_dict()
        config_dict["fabrics"]["octoenergy_data_prod"]["fabric"]["name"] = "different-name"
        
        with pytest.raises(ValidationError) as exc_info:
            MultipleFabricsConfig(**config_dict)
        
        assert "Fabric name 'different-name' does not match key 'octoenergy_data_prod'" in str(exc_info.value)


class TestLegacyFabricConfig:
    """Test the LegacyFabricConfig schema and conversion."""
    
    def get_valid_legacy_config_dict(self):
        """Return a valid legacy configuration dictionary."""
        return {
            "fabric": {
                "name": "test-fabric",
                "description": "A test fabric"
            },
            "databricks": {
                "host": "test-workspace.cloud.databricks.com",
                "auth_type": "oauth"
            },
            "packages": [
                {
                    "git": "https://github.com/dbt-labs/dbt-utils.git",
                    "revision": "1.0.0"
                },
                {
                    "package": "elementary-data/elementary",
                    "version": ">=0.16.4"
                }
            ],
            "catalog": "test_catalog",
            "schema": "test_schema",
            "vars": {
                "debug_mode": False
            },
            "models": {
                "+materialized": "table"
            }
        }
    
    def test_valid_legacy_fabric_config(self):
        """Test valid legacy fabric configuration creation."""
        config_dict = self.get_valid_legacy_config_dict()
        config = LegacyFabricConfig(**config_dict)
        
        assert config.fabric.name == "test-fabric"
        assert config.databricks.host == "test-workspace.cloud.databricks.com"
        assert len(config.packages) == 2
        assert config.catalog == "test_catalog"
        assert config.schema_name == "test_schema"
    
    def test_legacy_to_new_format_conversion(self):
        """Test conversion from legacy to new format."""
        config_dict = self.get_valid_legacy_config_dict()
        legacy_config = LegacyFabricConfig(**config_dict)
        
        new_config = legacy_config.to_new_format("core")
        
        assert new_config.fabric.name == "test-fabric"
        assert new_config.databricks.host == "test-workspace.cloud.databricks.com"
        assert len(new_config.projects) == 1
        assert "core" in new_config.projects
        
        project = new_config.projects["core"]
        assert project.name == "core"
        assert project.schema_name == "test_schema"
        assert project.catalog == "test_catalog"
        assert len(project.packages) == 2
        assert project.vars["debug_mode"] is False
        assert project.models["+materialized"] == "table"
