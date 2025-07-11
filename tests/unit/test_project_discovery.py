"""
Unit tests for the project discovery module.
"""

import tempfile
from pathlib import Path
from unittest.mock import patch

import yaml

from dbt_projects_cli.core.project_discovery import DbtProject, ProjectDiscovery


class TestDbtProject:
    """Test cases for DbtProject dataclass."""

    def test_dbt_project_creation(self):
        """Test creating DbtProject dataclass."""
        config = {"name": "test_project", "version": "1.0.0", "profile": "test_profile"}

        project = DbtProject(
            name="test_project",
            path=Path("/tmp/test"),
            config=config,
            project_type="package",
            model_count=5,
            macro_count=3,
            test_count=2,
        )

        assert project.name == "test_project"
        assert project.path == Path("/tmp/test")
        assert project.config == config
        assert project.project_type == "package"
        assert project.model_count == 5
        assert project.macro_count == 3
        assert project.test_count == 2


class TestProjectDiscovery:
    """Test cases for ProjectDiscovery class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.discovery = ProjectDiscovery(self.temp_dir)

    def test_project_discovery_initialization(self):
        """Test ProjectDiscovery initialization."""
        # Test with explicit path
        discovery = ProjectDiscovery(self.temp_dir)
        assert discovery.root_path == self.temp_dir
        assert discovery.projects == []

        # Test with default path (current working directory)
        discovery_default = ProjectDiscovery()
        assert discovery_default.root_path == Path.cwd()

    def test_discover_all_projects_empty(self):
        """Test discovering projects when no projects exist."""
        result = self.discovery.discover_all_projects()

        assert "packages" in result
        assert "fabrics" in result
        assert result["packages"] == []
        assert result["fabrics"] == []

    def test_discover_packages_projects(self):
        """Test discovering projects in packages directory."""
        # Create packages structure
        packages_dir = self.temp_dir / "packages"
        packages_dir.mkdir()

        project_dir = packages_dir / "test_package"
        project_dir.mkdir()

        dbt_project = {
            "name": "test_package",
            "version": "1.0.0",
            "profile": "test_profile",
            "model-paths": ["models"],
        }

        with open(project_dir / "dbt_project.yml", "w") as f:
            yaml.dump(dbt_project, f)

        # Create models directory and file
        models_dir = project_dir / "models"
        models_dir.mkdir()
        (models_dir / "test_model.sql").write_text("SELECT 1")

        result = self.discovery.discover_all_projects()

        assert len(result["packages"]) == 1
        assert result["packages"][0]["name"] == "test_package"
        assert result["packages"][0]["project_type"] == "package"
        assert result["packages"][0]["model_count"] == 1

    def test_discover_fabrics_projects(self):
        """Test discovering projects in fabrics directory."""
        # Create fabrics structure
        fabrics_dir = self.temp_dir / "fabrics"
        fabrics_dir.mkdir()

        project_dir = fabrics_dir / "test_fabric"
        project_dir.mkdir()

        dbt_project = {
            "name": "test_fabric",
            "version": "2.0.0",
            "profile": "fabric_profile",
        }

        with open(project_dir / "dbt_project.yml", "w") as f:
            yaml.dump(dbt_project, f)

        result = self.discovery.discover_all_projects()

        assert len(result["fabrics"]) == 1
        assert result["fabrics"][0]["name"] == "test_fabric"
        assert result["fabrics"][0]["project_type"] == "fabric"
        assert result["fabrics"][0]["version"] == "2.0.0"

    def test_discover_projects_with_invalid_yaml(self):
        """Test discovering projects with invalid YAML files."""
        packages_dir = self.temp_dir / "packages"
        packages_dir.mkdir()

        project_dir = packages_dir / "invalid_project"
        project_dir.mkdir()

        # Create invalid YAML
        with open(project_dir / "dbt_project.yml", "w") as f:
            f.write("invalid: yaml: content: [")

        result = self.discovery.discover_all_projects()

        # Should not include the invalid project
        assert len(result["packages"]) == 0

    def test_discover_projects_missing_name(self):
        """Test discovering projects missing required name field."""
        packages_dir = self.temp_dir / "packages"
        packages_dir.mkdir()

        project_dir = packages_dir / "unnamed_project"
        project_dir.mkdir()

        # Create config without name
        dbt_project = {"version": "1.0.0", "profile": "test_profile"}

        with open(project_dir / "dbt_project.yml", "w") as f:
            yaml.dump(dbt_project, f)

        result = self.discovery.discover_all_projects()

        # Should not include project without name
        assert len(result["packages"]) == 0

    def test_discover_projects_excludes_target_directories(self):
        """Test that target and other excluded directories are skipped."""
        packages_dir = self.temp_dir / "packages"
        packages_dir.mkdir()

        # Create project in excluded directory
        target_dir = packages_dir / "target"
        target_dir.mkdir()

        with open(target_dir / "dbt_project.yml", "w") as f:
            yaml.dump({"name": "target_project"}, f)

        # Create valid project
        valid_dir = packages_dir / "valid_project"
        valid_dir.mkdir()

        with open(valid_dir / "dbt_project.yml", "w") as f:
            yaml.dump({"name": "valid_project"}, f)

        result = self.discovery.discover_all_projects()

        # Should only include the valid project
        assert len(result["packages"]) == 1
        assert result["packages"][0]["name"] == "valid_project"

    def test_count_files_in_paths(self):
        """Test counting files in specified paths."""
        project_dir = self.temp_dir / "test_project"
        project_dir.mkdir()

        # Create models directory with SQL files
        models_dir = project_dir / "models"
        models_dir.mkdir()
        (models_dir / "model1.sql").write_text("SELECT 1")
        (models_dir / "model2.sql").write_text("SELECT 2")
        (models_dir / "not_sql.txt").write_text("Not SQL")

        # Create subdirectory with more SQL files
        subdir = models_dir / "subdir"
        subdir.mkdir()
        (subdir / "model3.sql").write_text("SELECT 3")

        count = self.discovery._count_files_in_paths(project_dir, ["models"], [".sql"])

        assert count == 3  # Should count 3 SQL files

    def test_count_files_nonexistent_paths(self):
        """Test counting files in nonexistent paths."""
        project_dir = self.temp_dir / "test_project"
        project_dir.mkdir()

        count = self.discovery._count_files_in_paths(
            project_dir, ["nonexistent"], [".sql"]
        )

        assert count == 0

    def test_get_project_by_name_found(self):
        """Test getting a project by name when it exists."""
        # Create a project
        packages_dir = self.temp_dir / "packages"
        packages_dir.mkdir()

        project_dir = packages_dir / "find_me"
        project_dir.mkdir()

        dbt_project = {"name": "find_me", "version": "1.0.0", "profile": "test_profile"}

        with open(project_dir / "dbt_project.yml", "w") as f:
            yaml.dump(dbt_project, f)

        project = self.discovery.get_project_by_name("find_me")

        assert project is not None
        assert project.name == "find_me"
        assert project.path == project_dir
        assert project.config == dbt_project
        assert project.project_type == "package"

    def test_get_project_by_name_not_found(self):
        """Test getting a project by name when it doesn't exist."""
        project = self.discovery.get_project_by_name("nonexistent")

        assert project is None

    def test_list_models_in_project_found(self):
        """Test listing models in an existing project."""
        # Create a project with models
        packages_dir = self.temp_dir / "packages"
        packages_dir.mkdir()

        project_dir = packages_dir / "with_models"
        project_dir.mkdir()

        dbt_project = {
            "name": "with_models",
            "version": "1.0.0",
            "model-paths": ["models", "custom_models"],
        }

        with open(project_dir / "dbt_project.yml", "w") as f:
            yaml.dump(dbt_project, f)

        # Create model files
        models_dir = project_dir / "models"
        models_dir.mkdir()
        model1 = models_dir / "model1.sql"
        model1.write_text("SELECT 1")

        custom_dir = project_dir / "custom_models"
        custom_dir.mkdir()
        model2 = custom_dir / "model2.sql"
        model2.write_text("SELECT 2")

        models = self.discovery.list_models_in_project("with_models")

        assert len(models) == 2
        assert model1 in models
        assert model2 in models

    def test_list_models_in_project_not_found(self):
        """Test listing models in a nonexistent project."""
        models = self.discovery.list_models_in_project("nonexistent")

        assert models == []

    def test_list_models_in_project_no_models(self):
        """Test listing models in a project with no model files."""
        # Create a project without models
        packages_dir = self.temp_dir / "packages"
        packages_dir.mkdir()

        project_dir = packages_dir / "no_models"
        project_dir.mkdir()

        dbt_project = {"name": "no_models", "version": "1.0.0"}

        with open(project_dir / "dbt_project.yml", "w") as f:
            yaml.dump(dbt_project, f)

        models = self.discovery.list_models_in_project("no_models")

        assert models == []

    def test_analyze_project_with_macros_and_tests(self):
        """Test analyzing a project with macros and tests."""
        packages_dir = self.temp_dir / "packages"
        packages_dir.mkdir()

        project_dir = packages_dir / "full_project"
        project_dir.mkdir()

        dbt_project = {
            "name": "full_project",
            "version": "1.0.0",
            "profile": "test_profile",
            "model-paths": ["models"],
            "macro-paths": ["macros"],
            "test-paths": ["tests"],
        }

        with open(project_dir / "dbt_project.yml", "w") as f:
            yaml.dump(dbt_project, f)

        # Create files
        for dir_name in ["models", "macros", "tests"]:
            dir_path = project_dir / dir_name
            dir_path.mkdir()
            (dir_path / f"{dir_name}_file.sql").write_text("SELECT 1")
            (dir_path / f"{dir_name}_file2.sql").write_text("SELECT 2")

        result = self.discovery.discover_all_projects()

        assert len(result["packages"]) == 1
        project_info = result["packages"][0]
        assert project_info["model_count"] == 2
        assert project_info["macro_count"] == 2
        assert project_info["test_count"] == 2

    @patch("dbt_projects_cli.core.project_discovery.console")
    def test_analyze_project_with_exception(self, mock_console):
        """Test error handling when analyzing a project fails."""
        packages_dir = self.temp_dir / "packages"
        packages_dir.mkdir()

        project_dir = packages_dir / "error_project"
        project_dir.mkdir()

        # Create invalid YAML file that will cause an error during analysis
        with open(project_dir / "dbt_project.yml", "w") as f:
            f.write("name: test_project\nversion: 1.0.0")

        # Mock yaml.safe_load to raise an exception
        with patch("yaml.safe_load", side_effect=Exception("YAML parsing error")):
            result = self.discovery.discover_all_projects()

        # Should handle error gracefully
        assert len(result["packages"]) == 0
        mock_console.print.assert_called()

    def test_discover_projects_nested_structure(self):
        """Test discovering projects in deeply nested structure."""
        # Create nested structure in packages
        deep_dir = self.temp_dir / "packages" / "domain" / "subdomain"
        deep_dir.mkdir(parents=True)

        project_dir = deep_dir / "nested_project"
        project_dir.mkdir()

        dbt_project = {"name": "nested_project", "version": "1.0.0"}

        with open(project_dir / "dbt_project.yml", "w") as f:
            yaml.dump(dbt_project, f)

        result = self.discovery.discover_all_projects()

        assert len(result["packages"]) == 1
        assert result["packages"][0]["name"] == "nested_project"
