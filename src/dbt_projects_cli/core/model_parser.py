"""
Parser for dbt model files and YAML schema files.
"""

import yaml
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from rich.console import Console

console = Console()


@dataclass
class ModelInfo:
    """Information about a dbt model."""
    name: str
    path: Path
    sql_content: str
    schema_file: Optional[Path] = None
    existing_description: Optional[str] = None
    columns: List[Dict[str, str]] = None


class ModelParser:
    """Parser for dbt model files and their schema definitions."""
    
    def __init__(self, project_path: Path):
        self.project_path = project_path
    
    def parse_model(self, model_path: Path) -> ModelInfo:
        """Parse a dbt model file and associated schema."""
        
        # Read SQL content
        with open(model_path, 'r') as f:
            sql_content = f.read()
        
        # Find associated schema file
        schema_file = self._find_schema_file(model_path)
        existing_description = None
        columns = []
        
        if schema_file:
            existing_description, columns = self._parse_schema_file(
                schema_file, model_path.stem
            )
        
        return ModelInfo(
            name=model_path.stem,
            path=model_path,
            sql_content=sql_content,
            schema_file=schema_file,
            existing_description=existing_description,
            columns=columns
        )
    
    def _find_schema_file(self, model_path: Path) -> Optional[Path]:
        """Find the schema YAML file for a model."""
        
        # Look for schema files in the same directory
        model_dir = model_path.parent
        
        # Common schema file patterns
        schema_patterns = [
            "_models.yml",
            "_schema.yml", 
            "schema.yml",
            f"{model_path.stem}.yml"
        ]
        
        for pattern in schema_patterns:
            schema_path = model_dir / pattern
            if schema_path.exists():
                return schema_path
        
        # Look in parent directories (up to 2 levels)
        for i in range(1, 3):
            parent_dir = model_path.parents[i] if len(model_path.parents) > i else None
            if parent_dir and parent_dir.is_relative_to(self.project_path):
                for pattern in schema_patterns:
                    schema_path = parent_dir / pattern
                    if schema_path.exists():
                        return schema_path
        
        return None
    
    def _parse_schema_file(
        self, schema_file: Path, model_name: str
    ) -> Tuple[Optional[str], List[Dict[str, str]]]:
        """Parse a schema YAML file for model description and columns."""
        
        try:
            with open(schema_file, 'r') as f:
                schema_data = yaml.safe_load(f)
            
            if not schema_data or 'models' not in schema_data:
                return None, []
            
            # Find the specific model in the schema
            for model in schema_data['models']:
                if model.get('name') == model_name:
                    description = model.get('description')
                    columns = model.get('columns', [])
                    return description, columns
            
            return None, []
        
        except Exception as e:
            console.print(f"[yellow]Warning: Could not parse schema file {schema_file}: {e}[/yellow]")
            return None, []
    
    def update_model_descriptions(
        self, 
        model_info: ModelInfo, 
        new_description: str, 
        column_descriptions: Dict[str, str],
        create_if_missing: bool = True
    ) -> bool:
        """Update model descriptions in the schema file."""
        
        if not model_info.schema_file and create_if_missing:
            # Create a new schema file
            schema_file = model_info.path.parent / "_models.yml"
            self._create_schema_file(schema_file, model_info.name, new_description, column_descriptions)
            return True
        
        elif model_info.schema_file:
            # Update existing schema file
            return self._update_existing_schema_file(
                model_info.schema_file, 
                model_info.name, 
                new_description, 
                column_descriptions
            )
        
        return False
    
    def _create_schema_file(
        self, 
        schema_file: Path, 
        model_name: str, 
        description: str, 
        column_descriptions: Dict[str, str]
    ) -> None:
        """Create a new schema YAML file."""
        
        schema_data = {
            'version': 2,
            'models': [{
                'name': model_name,
                'description': description,
                'columns': [
                    {
                        'name': col_name,
                        'description': col_desc
                    }
                    for col_name, col_desc in column_descriptions.items()
                ]
            }]
        }
        
        with open(schema_file, 'w') as f:
            yaml.dump(schema_data, f, default_flow_style=False, sort_keys=False)
        
        console.print(f"[green]Created schema file: {schema_file}[/green]")
    
    def _update_existing_schema_file(
        self, 
        schema_file: Path, 
        model_name: str, 
        new_description: str, 
        column_descriptions: Dict[str, str]
    ) -> bool:
        """Update an existing schema YAML file."""
        
        try:
            with open(schema_file, 'r') as f:
                schema_data = yaml.safe_load(f)
            
            if not schema_data:
                schema_data = {'version': 2, 'models': []}
            
            if 'models' not in schema_data:
                schema_data['models'] = []
            
            # Find and update the model
            model_found = False
            for model in schema_data['models']:
                if model.get('name') == model_name:
                    model['description'] = new_description
                    
                    # Update columns
                    existing_columns = {col.get('name'): col for col in model.get('columns', [])}
                    updated_columns = []
                    
                    # Update existing columns and add new ones
                    for col_name, col_desc in column_descriptions.items():
                        if col_name in existing_columns:
                            existing_columns[col_name]['description'] = col_desc
                            updated_columns.append(existing_columns[col_name])
                        else:
                            updated_columns.append({
                                'name': col_name,
                                'description': col_desc
                            })
                    
                    # Add columns that weren't in the new descriptions but existed before
                    for col_name, col_data in existing_columns.items():
                        if col_name not in column_descriptions:
                            updated_columns.append(col_data)
                    
                    model['columns'] = updated_columns
                    model_found = True
                    break
            
            # If model not found, add it
            if not model_found:
                new_model = {
                    'name': model_name,
                    'description': new_description,
                    'columns': [
                        {
                            'name': col_name,
                            'description': col_desc
                        }
                        for col_name, col_desc in column_descriptions.items()
                    ]
                }
                schema_data['models'].append(new_model)
            
            # Write back to file
            with open(schema_file, 'w') as f:
                yaml.dump(schema_data, f, default_flow_style=False, sort_keys=False)
            
            console.print(f"[green]Updated schema file: {schema_file}[/green]")
            return True
        
        except Exception as e:
            console.print(f"[red]Error updating schema file {schema_file}: {e}[/red]")
            return False
