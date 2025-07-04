"""
Unit tests for the model parser module.
"""

import tempfile
from pathlib import Path
from unittest.mock import mock_open, patch

import yaml

from dbt_projects_cli.core.model_parser import ModelParser


class TestModelParser:
    """Test cases for ModelParser class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.parser = ModelParser(self.temp_dir)

    def test_model_parser_initialization(self):
        """Test ModelParser initialization."""
        assert self.parser.project_path == self.temp_dir

    def test_parse_model_basic(self):
        """Test basic model parsing with SQL content."""
        # Create a temporary SQL file
        sql_file = self.temp_dir / "test_model.sql"
        sql_content = "SELECT * FROM source_table"
        sql_file.write_text(sql_content)

        model_info = self.parser.parse_model(sql_file)

        assert model_info.name == "test_model"
        assert model_info.path == sql_file
        assert model_info.sql_content == sql_content
        assert model_info.schema_file is None
        assert model_info.existing_description is None

    def test_parse_model_with_null_bytes(self):
        """Test model parsing with null bytes in SQL content."""
        sql_file = self.temp_dir / "test_model.sql"
        sql_content_with_nulls = "SELECT * FROM\x00 source_table\x00"
        sql_file.write_text(sql_content_with_nulls)

        with patch("builtins.print"):  # Mock console.print
            model_info = self.parser.parse_model(sql_file)

        # Null bytes should be removed
        assert "\x00" not in model_info.sql_content
        assert model_info.sql_content == "SELECT * FROM source_table"

    def test_parse_model_with_unicode_decode_error(self):
        """Test model parsing with Unicode decode error."""
        sql_file = self.temp_dir / "test_model.sql"

        # Mock open to raise UnicodeDecodeError on first call, succeed on second
        def mock_open_side_effect(*args, **kwargs):
            if "utf-8" in kwargs.get("encoding", ""):
                raise UnicodeDecodeError("utf-8", b"", 0, 1, "mock error")
            else:
                return mock_open(read_data="SELECT * FROM source_table")()

        with patch("builtins.open", side_effect=mock_open_side_effect):
            model_info = self.parser.parse_model(sql_file)

        assert model_info.sql_content == "SELECT * FROM source_table"

    def test_find_schema_file_same_directory(self):
        """Test finding schema file in the same directory."""
        # Create model file
        sql_file = self.temp_dir / "test_model.sql"
        sql_file.write_text("SELECT 1")

        # Create schema file
        schema_file = self.temp_dir / "_models.yaml"
        schema_file.write_text("version: 2\nmodels: []")

        found_schema = self.parser._find_schema_file(sql_file)
        assert found_schema == schema_file

    def test_find_schema_file_parent_directory(self):
        """Test finding schema file in parent directory."""
        # Create subdirectory structure
        subdir = self.temp_dir / "models"
        subdir.mkdir()

        # Create model file in subdirectory
        sql_file = subdir / "test_model.sql"
        sql_file.write_text("SELECT 1")

        # Create schema file in parent directory
        schema_file = self.temp_dir / "_models.yaml"
        schema_file.write_text("version: 2\nmodels: []")

        found_schema = self.parser._find_schema_file(sql_file)
        assert found_schema == schema_file

    def test_parse_schema_file_valid(self):
        """Test parsing a valid schema file."""
        schema_content = {
            "version": 2,
            "models": [
                {
                    "name": "test_model",
                    "description": "Test description",
                    "columns": [
                        {"name": "col1", "description": "Column 1"},
                        {"name": "col2", "description": "Column 2"},
                    ],
                }
            ],
        }

        schema_file = self.temp_dir / "schema.yml"
        with open(schema_file, "w") as f:
            yaml.dump(schema_content, f)

        description, columns = self.parser._parse_schema_file(schema_file, "test_model")

        assert description == "Test description"
        assert len(columns) == 2
        assert columns[0]["name"] == "col1"
        assert columns[1]["description"] == "Column 2"

    def test_parse_schema_file_model_not_found(self):
        """Test parsing schema file when model is not found."""
        schema_content = {
            "version": 2,
            "models": [{"name": "other_model", "description": "Other description"}],
        }

        schema_file = self.temp_dir / "schema.yml"
        with open(schema_file, "w") as f:
            yaml.dump(schema_content, f)

        description, columns = self.parser._parse_schema_file(schema_file, "test_model")

        assert description is None
        assert columns == []

    def test_parse_schema_file_invalid(self):
        """Test parsing invalid schema file."""
        schema_file = self.temp_dir / "schema.yml"
        schema_file.write_text("invalid yaml content: [")

        with patch("builtins.print"):  # Mock console.print
            description, columns = self.parser._parse_schema_file(
                schema_file, "test_model"
            )

        assert description is None
        assert columns == []

    def test_escape_yaml_string_simple(self):
        """Test YAML string escaping for simple strings."""
        result = self.parser._escape_yaml_string("simple text")
        assert result == "'simple text'"

    def test_escape_yaml_string_with_quotes(self):
        """Test YAML string escaping with quotes."""
        result = self.parser._escape_yaml_string('text with "quotes"')
        assert result == '"text with \\"quotes\\""'

    def test_escape_yaml_string_with_special_chars(self):
        """Test YAML string escaping with special characters."""
        result = self.parser._escape_yaml_string("text: with #special @chars")
        assert result == '"text: with #special @chars"'

    def test_escape_yaml_string_empty(self):
        """Test YAML string escaping with empty string."""
        result = self.parser._escape_yaml_string("")
        assert result == "''"

    def test_is_valid_column_name_valid(self):
        """Test valid column name validation."""
        assert self.parser._is_valid_column_name("valid_column")
        assert self.parser._is_valid_column_name("_starts_with_underscore")
        assert self.parser._is_valid_column_name("column123")
        assert self.parser._is_valid_column_name("A_Column_Name")

    def test_is_valid_column_name_invalid(self):
        """Test invalid column name validation."""
        assert not self.parser._is_valid_column_name("- bullet_point")
        assert not self.parser._is_valid_column_name("`backtick_column`")
        assert not self.parser._is_valid_column_name("**bold_column**")
        assert not self.parser._is_valid_column_name("column:")
        assert not self.parser._is_valid_column_name("123starts_with_number")
        assert not self.parser._is_valid_column_name("")

    def test_clean_model_description(self):
        """Test cleaning model description to remove column descriptions."""
        description_with_columns = """
        This is a model description.

        **COLUMN_DESCRIPTIONS:**
        - column1: Description 1
        - column2: Description 2
        """

        cleaned = self.parser._clean_model_description(description_with_columns)
        assert "COLUMN_DESCRIPTIONS" not in cleaned
        assert "column1" not in cleaned
        assert "This is a model description." in cleaned

    def test_clean_model_description_with_markdown(self):
        """Test cleaning model description with markdown formatting."""
        description = "This is **bold** text with `code` and _italic_."

        # Note: The model parser's clean method only removes column descriptions
        # It doesn't actually clean markdown formatting in the base implementation
        cleaned = self.parser._clean_model_description(description)

        # The method should preserve the original text since there are no column
        # descriptions to remove
        assert "bold" in cleaned
        assert "code" in cleaned

    def test_get_preferred_schema_extension_yaml(self):
        """Test getting preferred schema extension when YAML files exist."""
        # Create a YAML schema file
        schema_file = self.temp_dir / "_models.yaml"
        schema_file.write_text("version: 2")

        extension = self.parser._get_preferred_schema_extension(self.temp_dir)
        assert extension == "yaml"

    def test_get_preferred_schema_extension_yml(self):
        """Test getting preferred schema extension when YML files exist."""
        # Create a YML schema file
        schema_file = self.temp_dir / "_models.yml"
        schema_file.write_text("version: 2")

        extension = self.parser._get_preferred_schema_extension(self.temp_dir)
        assert extension == "yml"

    def test_get_preferred_schema_extension_default(self):
        """Test getting preferred schema extension when no files exist."""
        extension = self.parser._get_preferred_schema_extension(self.temp_dir)
        assert extension == "yaml"

    def test_create_schema_file(self):
        """Test creating a new schema file."""
        schema_file = self.temp_dir / "_models.yaml"
        model_name = "test_model"
        description = "Test model description"
        column_descriptions = {
            "col1": "Column 1 description",
            "col2": "Column 2 description",
        }

        with patch("builtins.print"):  # Mock console.print
            self.parser._create_schema_file(
                schema_file, model_name, description, column_descriptions
            )

        assert schema_file.exists()

        # Verify content
        with open(schema_file, "r") as f:
            content = f.read()
            assert "test_model" in content
            assert "Test model description" in content
            assert "col1" in content
            assert "Column 1 description" in content
