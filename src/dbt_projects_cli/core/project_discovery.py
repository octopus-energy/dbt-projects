"""
Project discovery utilities for finding and analyzing dbt projects.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from rich.console import Console

console = Console()


@dataclass
class DbtProject:
    """Represents a dbt project with its metadata."""
    name: str
    path: Path
    config: Dict[str, Any]
    project_type: str  # 'package' or 'fabric'
    model_count: int = 0
    macro_count: int = 0
    test_count: int = 0


class ProjectDiscovery:
    """Discovers and analyzes dbt projects in the repository."""
    
    def __init__(self, root_path: Optional[Path] = None):
        self.root_path = root_path or Path.cwd()
        self.projects: List[DbtProject] = []
    
    def discover_all_projects(self) -> Dict[str, List[Dict[str, Any]]]:
        """Discover all dbt projects in packages and fabrics directories."""
        projects_info = {
            "packages": [],
            "fabrics": []
        }
        
        # Find packages
        packages_dir = self.root_path / "packages"
        if packages_dir.exists():
            projects_info["packages"] = self._discover_projects_in_directory(
                packages_dir, "package"
            )
        
        # Find fabrics
        fabrics_dir = self.root_path / "fabrics"
        if fabrics_dir.exists():
            projects_info["fabrics"] = self._discover_projects_in_directory(
                fabrics_dir, "fabric"
            )
        
        return projects_info
    
    def _discover_projects_in_directory(
        self, directory: Path, project_type: str
    ) -> List[Dict[str, Any]]:
        """Discover dbt projects in a specific directory."""
        projects = []
        
        for dbt_project_file in directory.rglob("dbt_project.yml"):
            try:
                project_info = self._analyze_project(dbt_project_file, project_type)
                if project_info:
                    projects.append(project_info)
            except Exception as e:
                console.print(
                    f"[yellow]Warning: Could not analyze project at {dbt_project_file}: {e}[/yellow]"
                )
        
        return projects
    
    def _analyze_project(
        self, dbt_project_file: Path, project_type: str
    ) -> Optional[Dict[str, Any]]:
        """Analyze a single dbt project."""
        project_dir = dbt_project_file.parent
        
        try:
            with open(dbt_project_file, 'r') as f:
                config = yaml.safe_load(f)
            
            if not config or 'name' not in config:
                return None
            
            # Count models, macros, tests
            model_count = self._count_files_in_paths(project_dir, config.get('model-paths', ['models']), ['.sql'])
            macro_count = self._count_files_in_paths(project_dir, config.get('macro-paths', ['macros']), ['.sql'])
            test_count = self._count_files_in_paths(project_dir, config.get('test-paths', ['tests']), ['.sql'])
            
            return {
                "name": config['name'],
                "path": str(project_dir.relative_to(self.root_path)),
                "config": config,
                "project_type": project_type,
                "model_count": model_count,
                "macro_count": macro_count,
                "test_count": test_count,
                "profile": config.get('profile'),
                "version": config.get('version'),
            }
        
        except Exception as e:
            console.print(f"[red]Error analyzing project {dbt_project_file}: {e}[/red]")
            return None
    
    def _count_files_in_paths(
        self, project_dir: Path, paths: List[str], extensions: List[str]
    ) -> int:
        """Count files with specific extensions in given paths."""
        count = 0
        for path_str in paths:
            path = project_dir / path_str
            if path.exists():
                for ext in extensions:
                    count += len(list(path.rglob(f"*{ext}")))
        return count
    
    def get_project_by_name(self, name: str) -> Optional[DbtProject]:
        """Get a specific project by name."""
        all_projects = self.discover_all_projects()
        
        for project_type, projects_list in all_projects.items():
            for project_info in projects_list:
                if project_info["name"] == name:
                    return DbtProject(
                        name=project_info["name"],
                        path=self.root_path / project_info["path"],
                        config=project_info["config"],
                        project_type=project_info["project_type"],
                        model_count=project_info["model_count"],
                        macro_count=project_info["macro_count"],
                        test_count=project_info["test_count"],
                    )
        
        return None
    
    def list_models_in_project(self, project_name: str) -> List[Path]:
        """List all model files in a specific project."""
        project = self.get_project_by_name(project_name)
        if not project:
            return []
        
        models = []
        model_paths = project.config.get('model-paths', ['models'])
        
        for path_str in model_paths:
            path = project.path / path_str
            if path.exists():
                models.extend(path.rglob("*.sql"))
        
        return models
