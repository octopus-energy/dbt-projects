"""
Parser for dbt model files and YAML schema files.
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
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
    columns: Optional[List[Dict[str, str]]] = None


class ModelParser:
    """Parser for dbt model files and their schema definitions."""

    def __init__(self, project_path: Path):
        self.project_path = project_path

    def parse_model(self, model_path: Path) -> ModelInfo:
        """Parse a dbt model file and associated schema."""

        # Read SQL content with null byte protection
        try:
            with open(model_path, "r", encoding="utf-8") as f:
                sql_content = f.read()

            # Remove null bytes if present (they can cause compilation errors)
            if "\x00" in sql_content:
                console.print(
                    f"[yellow]⚠️  Removing null bytes from {model_path.name}[/yellow]"
                )
                sql_content = sql_content.replace("\x00", "")

        except UnicodeDecodeError:
            # Try with different encoding if UTF-8 fails
            with open(model_path, "r", encoding="latin-1") as f:
                sql_content = f.read()
            # Remove non-ASCII characters that might cause issues
            sql_content = sql_content.encode("ascii", "ignore").decode("ascii")

        # Find associated schema file
        schema_file = self._find_schema_file(model_path)
        existing_description = None
        columns: List[Dict[str, str]] = []

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
            columns=columns,
        )

    def _find_schema_file(self, model_path: Path) -> Optional[Path]:
        """Find the schema YAML file for a model."""

        # Look for schema files in the same directory
        model_dir = model_path.parent

        # Common schema file patterns (check .yaml before .yml)
        schema_patterns = [
            "_models.yaml",
            "_models.yml",
            "_schema.yaml",
            "_schema.yml",
            "schema.yaml",
            "schema.yml",
            f"{model_path.stem}.yaml",
            f"{model_path.stem}.yml",
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
            with open(schema_file, "r") as f:
                schema_data = yaml.safe_load(f)

            if not schema_data or "models" not in schema_data:
                return None, []

            # Find the specific model in the schema
            for model in schema_data["models"]:
                if model.get("name") == model_name:
                    description = model.get("description")
                    columns = model.get("columns", [])
                    return description, columns

            return None, []

        except Exception as e:
            console.print(
                f"[yellow]Warning: Could not parse schema file "
                f"{schema_file}: {e}[/yellow]"
            )
            return None, []

    def update_model_descriptions(
        self,
        model_info: ModelInfo,
        new_description: str,
        column_descriptions: Dict[str, str],
        create_if_missing: bool = True,
    ) -> bool:
        """Update model descriptions in the schema file."""

        if not model_info.schema_file and create_if_missing:
            # Create a new schema file - check if there's an existing pattern to follow
            existing_extension = self._get_preferred_schema_extension(
                model_info.path.parent
            )
            schema_file = model_info.path.parent / f"_models.{existing_extension}"
            self._create_schema_file(
                schema_file, model_info.name, new_description, column_descriptions
            )
            return True

        elif model_info.schema_file:
            # Update existing schema file
            return self._update_existing_schema_file(
                model_info.schema_file,
                model_info.name,
                new_description,
                column_descriptions,
            )

        return False

    def _create_schema_file(
        self,
        schema_file: Path,
        model_name: str,
        description: str,
        column_descriptions: Dict[str, str],
    ) -> None:
        """Create a new schema YAML file."""

        schema_data = {
            "version": 2,
            "models": [
                {
                    "name": model_name,
                    "description": description,
                    "columns": [
                        {"name": col_name, "description": col_desc}
                        for col_name, col_desc in column_descriptions.items()
                    ],
                }
            ],
        }

        with open(schema_file, "w") as f:
            self._write_yaml_with_proper_indentation(f, schema_data)

        console.print(f"[green]Created schema file: {schema_file}[/green]")

    def _update_existing_schema_file(
        self,
        schema_file: Path,
        model_name: str,
        new_description: str,
        column_descriptions: Dict[str, str],
    ) -> bool:
        """Update an existing schema YAML file."""

        try:
            with open(schema_file, "r") as f:
                schema_data = yaml.safe_load(f)

            if not schema_data:
                schema_data = {"version": 2, "models": []}

            if "models" not in schema_data:
                schema_data["models"] = []

            # Find and update the model
            model_found = False
            for model in schema_data["models"]:
                if model.get("name") == model_name:
                    # Clean the model description to remove column descriptions
                    clean_description = self._clean_model_description(new_description)
                    model["description"] = clean_description

                    # Get existing valid columns (filter out invalid column names)
                    existing_columns = {}
                    for col in model.get("columns", []):
                        col_name = col.get("name", "")
                        # Filter out invalid column names (those with bullet
                        # points, backticks, etc.)
                        if self._is_valid_column_name(col_name):
                            existing_columns[col_name] = col

                    updated_columns = []

                    # Update existing columns and add new ones
                    for col_name, col_desc in column_descriptions.items():
                        if col_name in existing_columns:
                            # Keep existing column properties but update description
                            existing_col = existing_columns[col_name].copy()
                            existing_col["description"] = col_desc
                            updated_columns.append(existing_col)
                        else:
                            # Add new column
                            updated_columns.append(
                                {"name": col_name, "description": col_desc}
                            )

                    # Add columns that weren't in the new descriptions but
                    # existed before and are valid
                    for col_name, col_data in existing_columns.items():
                        if col_name not in column_descriptions:
                            updated_columns.append(col_data)

                    model["columns"] = updated_columns
                    model_found = True
                    break

            # If model not found, add it
            if not model_found:
                clean_description = self._clean_model_description(new_description)
                new_model = {
                    "name": model_name,
                    "description": clean_description,
                    "columns": [
                        {"name": col_name, "description": col_desc}
                        for col_name, col_desc in column_descriptions.items()
                    ],
                }
                schema_data["models"].append(new_model)

            # Write back to file with proper formatting
            with open(schema_file, "w") as f:
                self._write_yaml_with_proper_indentation(f, schema_data)

            console.print(f"[green]Updated schema file: {schema_file}[/green]")
            return True

        except Exception as e:
            console.print(f"[red]Error updating schema file {schema_file}: {e}[/red]")
            return False

    def _clean_model_description(self, description: str) -> str:
        """Clean model description to remove column descriptions and
        formatting issues.
        """
        if not description:
            return description

        # Remove column descriptions section
        # Look for patterns like "**COLUMN_DESCRIPTIONS:**" and remove everything after
        patterns = [
            r"\*\*COLUMN_DESCRIPTIONS:\*\*.*$",
            r"COLUMN_DESCRIPTIONS:.*$",
            r"\n\n- [a-zA-Z_].*$",  # Remove bullet points with column names
            r"\n\n\*\*[A-Z_]+:\*\*.*$",  # Remove any other section headers
        ]

        cleaned = description
        for pattern in patterns:
            cleaned = re.sub(pattern, "", cleaned, flags=re.DOTALL | re.IGNORECASE)

        # Clean up extra whitespace and newlines
        cleaned = re.sub(
            r"\n\s*\n\s*\n", "\n\n", cleaned
        )  # Multiple newlines to double
        cleaned = cleaned.strip()

        return cleaned

    def _is_valid_column_name(self, column_name: str) -> bool:
        """Check if a column name is valid (not containing markdown artifacts)."""
        if not column_name:
            return False

        # Invalid patterns
        invalid_patterns = [
            r"^-\s",  # Starts with dash and space (bullet point)
            r"`",  # Contains backticks
            r"^\*\*",  # Starts with markdown bold
            r"\*\*$",  # Ends with markdown bold
            r":\s*$",  # Ends with colon
            r"^[^a-zA-Z_]",  # Doesn't start with letter or underscore
        ]

        for pattern in invalid_patterns:
            if re.search(pattern, column_name):
                return False

        return True

    def _get_preferred_schema_extension(self, directory: Path) -> str:
        """Determine the preferred file extension based on existing schema files."""

        # Check for existing schema files in the directory and parent directories
        for dir_path in [directory] + list(directory.parents[:2]):
            for file_path in dir_path.glob("*models.*"):
                if file_path.suffix in [".yaml", ".yml"]:
                    return file_path.suffix[1:]  # Remove the dot

            for file_path in dir_path.glob("*schema.*"):
                if file_path.suffix in [".yaml", ".yml"]:
                    return file_path.suffix[1:]  # Remove the dot

        # Default to yaml if no existing pattern found
        return "yaml"

    def _escape_yaml_string(self, text: str) -> str:
        """Properly escape a string for YAML output."""
        if not text:
            return "''"

        # Check if the string contains characters that require quoting
        needs_quoting = any(
            [
                "'" in text,
                '"' in text,
                "\n" in text,
                "\t" in text,
                text.startswith(" ") or text.endswith(" "),
                text.startswith("-"),
                text.startswith("["),
                text.startswith("{"),
                ":" in text,
                "#" in text,
                "|" in text,
                ">" in text,
                "&" in text,
                "*" in text,
                "!" in text,
                "%" in text,
                "@" in text,
                "`" in text,
            ]
        )

        if needs_quoting:
            # Use double quotes and escape any double quotes in the content
            escaped_text = text.replace("\\", "\\\\").replace('"', '\\"')
            return f'"{escaped_text}"'
        else:
            # Simple string that doesn't need quoting
            return f"'{text}'"

    def _write_yaml_with_proper_indentation(self, file_handle: Any, data: Any) -> None:
        """Write YAML with proper dbt-style indentation using template approach."""

        # Use a simple template-based approach for reliable YAML generation
        if "models" in data and data["models"]:
            file_handle.write("version: 2\n")
            file_handle.write("models:\n")

            for model in data["models"]:
                file_handle.write(f"  - name: {model['name']}\n")

                if "description" in model:
                    # Handle multi-line descriptions properly
                    desc = model["description"]
                    if "\n" in desc or len(desc) > 80:
                        file_handle.write("    description: |\n")
                        for line in desc.split("\n"):
                            file_handle.write(f"      {line}\n")
                    else:
                        # Properly escape the description for YAML
                        escaped_desc = self._escape_yaml_string(desc)
                        file_handle.write(f"    description: {escaped_desc}\n")

                if "columns" in model and model["columns"]:
                    file_handle.write("    columns:\n")
                    for column in model["columns"]:
                        file_handle.write(f"      - name: {column['name']}\n")
                        if "description" in column:
                            # Properly escape the column description for YAML
                            escaped_desc = self._escape_yaml_string(
                                column["description"]
                            )
                            file_handle.write(f"        description: {escaped_desc}\n")
                        if "tests" in column:
                            file_handle.write("        tests:\n")
                            for test in column["tests"]:
                                file_handle.write(f"          - {test}\n")
                        if "constraints" in column:
                            file_handle.write("        constraints:\n")
                            for constraint in column["constraints"]:
                                if isinstance(constraint, dict):
                                    file_handle.write(
                                        f"          - type: {constraint['type']}\n"
                                    )
                                else:
                                    file_handle.write(f"          - {constraint}\n")

                if "config" in model:
                    file_handle.write("    config:\n")
                    config = model["config"]
                    for key, value in config.items():
                        if key == "meta" and isinstance(value, dict):
                            file_handle.write("      meta:\n")
                            for meta_key, meta_value in value.items():
                                file_handle.write(f"        {meta_key}: {meta_value}\n")
                        elif key == "tags" and isinstance(value, list):
                            file_handle.write("      tags:\n")
                            for tag in value:
                                file_handle.write(f"        - {tag}\n")
                        else:
                            file_handle.write(f"      {key}: {value}\n")

                # Add blank line between models
                file_handle.write("\n")
        else:
            # Fallback to standard YAML dump
            yaml.dump(
                data, file_handle, default_flow_style=False, sort_keys=False, indent=2
            )
