"""
Schema definitions for lightweight fabric deployment configuration.

This module defines the Pydantic models used to validate and parse
fabric deployment configuration files with support for multiple projects
per fabric and automatic catalog naming conventions.
"""

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator


class GitPackage(BaseModel):
    """Configuration for a git-based dbt package."""

    git: str = Field(..., description="Git repository URL")
    subdirectory: Optional[str] = Field(
        None, description="Subdirectory within the git repository"
    )
    revision: Optional[str] = Field(
        None, description="Git revision (branch, tag, or commit)"
    )
    warn_unpinned: Optional[bool] = Field(
        True, description="Whether to warn about unpinned packages"
    )


class LocalPackage(BaseModel):
    """Configuration for a local dbt package."""

    local: str = Field(..., description="Local path to the package")


class HubPackage(BaseModel):
    """Configuration for a dbt Hub package."""

    package: str = Field(..., description="Package name from dbt Hub")
    version: str = Field(..., description="Package version")


# Union type for all package types
Package = Union[GitPackage, LocalPackage, HubPackage]


class DatabricksConnection(BaseModel):
    """Databricks connection configuration at fabric level."""

    host: str = Field(..., description="Databricks workspace hostname")
    http_path: Optional[str] = Field(None, description="HTTP path for SQL warehouse")
    auth_type: Optional[str] = Field("oauth", description="Authentication type")

    @field_validator("host")
    @classmethod
    def validate_host(cls, v: str) -> str:
        """Validate the Databricks host format."""
        if not v.endswith(".cloud.databricks.com") and not v.endswith(
            ".azuredatabricks.net"
        ):
            raise ValueError("Host must be a valid Databricks hostname")
        return v


class FabricMetadata(BaseModel):
    """Basic fabric metadata."""

    name: str = Field(..., description="Fabric name")
    description: Optional[str] = Field(None, description="Fabric description")
    version: str = Field("1.0.0", description="Fabric version")
    dbt_version: str = Field("1.7.0", description="Required dbt version")


class ProjectConfig(BaseModel):
    """Configuration for a single project within a fabric."""

    name: str = Field(..., description="Project name")
    description: Optional[str] = Field(None, description="Project description")
    schema_name: str = Field(
        ..., alias="schema", description="Databricks schema name for this project"
    )
    packages: List[Package] = Field(..., description="List of dbt packages to install")
    vars: Optional[Dict[str, Any]] = Field(
        default_factory=dict, description="dbt variables"
    )
    models: Optional[Dict[str, Any]] = Field(
        default_factory=dict, description="Model configurations"
    )
    tags: Optional[List[str]] = Field(default_factory=list, description="Project tags")
    catalog: Optional[str] = Field(
        None, description="Optional project-specific catalog override"
    )

    @field_validator("packages")
    @classmethod
    def validate_packages_not_empty(cls, v: List[Package]) -> List[Package]:
        """Ensure at least one package is specified."""
        if not v:
            raise ValueError("At least one package must be specified")
        return v

    class Config:
        """Pydantic configuration."""

        extra = "forbid"  # Don't allow extra fields


class FabricConfig(BaseModel):
    """Root configuration model for lightweight fabric deployment.

    Supports multiple projects per fabric.
    """

    fabric: FabricMetadata = Field(..., description="Fabric metadata")
    databricks: DatabricksConnection = Field(
        ..., description="Databricks connection details"
    )
    tags: Optional[List[str]] = Field(
        default_factory=list, description="Fabric-level tags"
    )
    projects: Dict[str, ProjectConfig] = Field(
        ..., description="Dictionary of project configurations"
    )

    @field_validator("projects")
    @classmethod
    def validate_projects_not_empty(
        cls, v: Dict[str, ProjectConfig]
    ) -> Dict[str, ProjectConfig]:
        """Ensure at least one project is specified."""
        if not v:
            raise ValueError("At least one project must be specified")
        return v

    @model_validator(mode="after")
    def validate_project_names_match_keys(self) -> "FabricConfig":
        """Ensure project names match their dictionary keys."""
        for key, project_config in self.projects.items():
            if project_config.name != key:
                raise ValueError(
                    f"Project name '{project_config.name}' does not match key '{key}'"
                )
        return self

    def get_catalog_name(self, environment: str = "dev") -> str:
        """Generate catalog name based on fabric name and environment.

        Args:
            environment: Either 'dev', 'prod', or 'source'

        Returns:
            Formatted catalog name following the convention:
            - prod: {fabric_name}_data_prod_prod
            - source: {fabric_name}_data_prod_source
            - dev/test: {fabric_name}_data_prod_test
        """
        if environment == "prod":
            return f"{self.fabric.name}_data_prod_prod"
        elif environment == "source":
            return f"{self.fabric.name}_data_prod_source"
        else:  # dev/test
            return f"{self.fabric.name}_data_prod_test"

    class Config:
        """Pydantic configuration."""

        extra = "forbid"  # Don't allow extra fields
        json_schema_extra = {
            "example": {
                "fabric": {
                    "name": "octoenergy_data_prod",
                    "description": "Octopus Energy data platform fabric",
                    "version": "1.0.0",
                },
                "databricks": {
                    "host": "octopus-workspace.cloud.databricks.com",
                    "auth_type": "oauth",
                },
                "tags": ["data-platform", "production"],
                "projects": {
                    "core": {
                        "name": "core",
                        "description": "Core data transformations",
                        "schema": "core",
                        "packages": [
                            {
                                "package": "elementary-data/elementary",
                                "version": ">=0.16.4",
                            },
                            {
                                "git": "https://github.com/octopus/local-dbx-info.git",
                                "revision": "main",
                            },
                        ],
                        "vars": {"timezone": "UTC"},
                        "tags": ["core", "transformations"],
                    }
                },
            }
        }


class MultipleFabricsConfig(BaseModel):
    """Configuration model for multiple fabric deployments in a single file."""

    fabrics: Dict[str, FabricConfig] = Field(
        ..., description="Dictionary of fabric configurations"
    )

    @field_validator("fabrics")
    @classmethod
    def validate_fabrics_not_empty(
        cls, v: Dict[str, FabricConfig]
    ) -> Dict[str, FabricConfig]:
        """Ensure at least one fabric is specified."""
        if not v:
            raise ValueError("At least one fabric must be specified")
        return v

    @model_validator(mode="after")
    def validate_fabric_names_match_keys(self) -> "MultipleFabricsConfig":
        """Ensure fabric names match their dictionary keys."""
        for key, fabric_config in self.fabrics.items():
            if fabric_config.fabric.name != key:
                raise ValueError(
                    f"Fabric name '{fabric_config.fabric.name}' does not "
                    f"match key '{key}'"
                )
        return self

    class Config:
        """Pydantic configuration."""

        extra = "forbid"  # Don't allow extra fields
        json_schema_extra = {
            "example": {
                "fabrics": {
                    "octoenergy_data_prod": {
                        "fabric": {
                            "name": "octoenergy_data_prod",
                            "description": "Octopus Energy production data fabric",
                            "version": "1.0.0",
                        },
                        "databricks": {
                            "host": "octopus-workspace.cloud.databricks.com",
                            "auth_type": "oauth",
                        },
                        "tags": ["production", "data-platform"],
                        "projects": {
                            "core": {
                                "name": "core",
                                "description": "Core analytics transformations",
                                "schema": "core",
                                "packages": [
                                    {
                                        "package": "elementary-data/elementary",
                                        "version": ">=0.16.4",
                                    }
                                ],
                                "vars": {"timezone": "UTC"},
                            },
                            "data-quality": {
                                "name": "data-quality",
                                "description": "Data quality monitoring",
                                "schema": "data_quality",
                                "packages": [
                                    {
                                        "git": (
                                            "https://github.com/octopus/dq-package.git"
                                        ),
                                        "revision": "v1.0.0",
                                    }
                                ],
                            },
                        },
                    }
                }
            }
        }


# Legacy support for backward compatibility
class LegacyFabricConfig(BaseModel):
    """Legacy single-project fabric configuration for backward compatibility."""

    fabric: FabricMetadata = Field(..., description="Fabric metadata")
    databricks: DatabricksConnection = Field(
        ..., description="Databricks connection details"
    )
    packages: List[Package] = Field(..., description="List of dbt packages to install")
    vars: Optional[Dict[str, Any]] = Field(
        default_factory=dict, description="dbt variables"
    )
    models: Optional[Dict[str, Any]] = Field(
        default_factory=dict, description="Model configurations"
    )

    # Legacy fields that used to be in databricks config
    catalog: Optional[str] = Field(None, description="Legacy catalog field")
    schema_name: Optional[str] = Field(
        None, alias="schema", description="Legacy schema field"
    )

    @field_validator("packages")
    @classmethod
    def validate_packages_not_empty(cls, v: List[Package]) -> List[Package]:
        """Ensure at least one package is specified."""
        if not v:
            raise ValueError("At least one package must be specified")
        return v

    def to_new_format(self, project_name: str = "default") -> FabricConfig:
        """Convert legacy config to new multi-project format."""
        # Create project configuration
        project_config = ProjectConfig(
            name=project_name,
            description=f"Migrated from legacy {self.fabric.name} configuration",
            schema=self.schema_name or "default",
            packages=self.packages,
            vars=self.vars or {},
            models=self.models or {},
            catalog=self.catalog,
        )

        return FabricConfig(
            fabric=self.fabric,
            databricks=self.databricks,
            projects={project_name: project_config},
        )

    class Config:
        """Pydantic configuration."""

        extra = "forbid"  # Don't allow extra fields
