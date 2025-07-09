"""
Unit tests for the Databricks integration module.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from dbt_projects_cli.integrations.databricks import (
    DatabricksConfig,
    DatabricksConnector,
    DatabricksQueryError,
    create_databricks_connector,
)


class TestDatabricksConfig:
    """Test cases for DatabricksConfig dataclass."""

    def test_databricks_config_creation(self):
        """Test creating DatabricksConfig with basic parameters."""
        config = DatabricksConfig(
            server_hostname="test.databricks.com", http_path="/sql/1.0/warehouses/test"
        )

        assert config.server_hostname == "test.databricks.com"
        assert config.http_path == "/sql/1.0/warehouses/test"
        assert config.catalog is None
        assert config.schema is None
        assert config.access_token is None
        assert config.auth_type == "token"
        assert config.client_id is None
        assert config.client_secret is None

    def test_databricks_config_with_all_parameters(self):
        """Test creating DatabricksConfig with all parameters."""
        config = DatabricksConfig(
            server_hostname="test.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            catalog="test_catalog",
            schema="test_schema",
            access_token="test_token",
            auth_type="oauth",
            client_id="test_client_id",
            client_secret="test_client_secret",
        )

        assert config.catalog == "test_catalog"
        assert config.schema == "test_schema"
        assert config.access_token == "test_token"
        assert config.auth_type == "oauth"
        assert config.client_id == "test_client_id"
        assert config.client_secret == "test_client_secret"


class TestDatabricksConnector:
    """Test cases for DatabricksConnector class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())

        self.config = DatabricksConfig(
            server_hostname="test.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            catalog="test_catalog",
            schema="test_schema",
            access_token="test_token",
        )

    def test_databricks_connector_initialization_without_sql_module(self):
        """Test DatabricksConnector initialization when databricks.sql is not
        available.
        """
        with patch("dbt_projects_cli.integrations.databricks.sql", None):
            with pytest.raises(
                ImportError, match="databricks-sql-connector is required"
            ):
                DatabricksConnector(self.config)

    @patch("dbt_projects_cli.integrations.databricks.sql")
    def test_databricks_connector_initialization_success(self, mock_sql):
        """Test successful DatabricksConnector initialization."""
        connector = DatabricksConnector(self.config)

        assert connector.config == self.config
        assert connector._connection is None
        assert connector._connection_key is not None

    @patch("dbt_projects_cli.integrations.databricks.sql")
    def test_create_connection_key(self, mock_sql):
        """Test creating unique connection key."""
        connector = DatabricksConnector(self.config)

        key = connector._create_connection_key()

        assert isinstance(key, str)
        assert "test.databricks.com" in key
        assert "/sql/1.0/warehouses/test" in key
        assert "token" in key

    @patch("dbt_projects_cli.integrations.databricks.sql")
    def test_create_connection_key_oauth(self, mock_sql):
        """Test creating connection key for OAuth authentication."""
        oauth_config = DatabricksConfig(
            server_hostname="test.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            auth_type="oauth",
            client_id="test_client",
            client_secret="test_secret",
        )

        connector = DatabricksConnector(oauth_config)
        key = connector._create_connection_key()

        assert "oauth" in key
        assert "test_client" in key

    @patch("dbt_projects_cli.integrations.databricks.sql")
    @patch("dbt_projects_cli.integrations.databricks.atexit.register")
    def test_cleanup_registration(self, mock_atexit, mock_sql):
        """Test that cleanup handler is registered."""
        # Reset the class-level flag for testing
        DatabricksConnector._cleanup_registered = False

        DatabricksConnector(self.config)

        mock_atexit.assert_called_once()

    @patch("dbt_projects_cli.integrations.databricks.sql")
    def test_cleanup_all_connections(self, mock_sql):
        """Test cleaning up all cached connections."""
        # Mock connection
        mock_connection = MagicMock()
        DatabricksConnector._connection_cache["test_key"] = mock_connection

        DatabricksConnector._cleanup_all_connections()

        mock_connection.close.assert_called_once()
        assert DatabricksConnector._connection_cache == {}

    def test_from_environment_success(self):
        """Test creating connector from environment variables."""
        env_vars = {
            "DATABRICKS_SERVER_HOSTNAME": "test.databricks.com",
            "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/test",
            "DATABRICKS_ACCESS_TOKEN": "test_token",
            "DATABRICKS_CATALOG": "test_catalog",
            "DATABRICKS_SCHEMA": "test_schema",
        }

        with patch.dict(os.environ, env_vars):
            with patch("dbt_projects_cli.integrations.databricks.sql"):
                connector = DatabricksConnector.from_environment()

        assert connector.config.server_hostname == "test.databricks.com"
        assert connector.config.catalog == "test_catalog"
        assert connector.config.schema == "test_schema"

    def test_from_environment_missing_variables(self):
        """Test creating connector with missing environment variables."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(
                ValueError, match="Missing required environment variables"
            ):
                DatabricksConnector.from_environment()

    def test_from_dbt_profiles_no_profiles_file(self):
        """Test creating connector when profiles.yml doesn't exist."""
        project_path = self.temp_dir

        result = DatabricksConnector.from_dbt_profiles(project_path)

        assert result is None

    def test_from_dbt_profiles_invalid_yaml(self):
        """Test creating connector with invalid YAML in profiles.yml."""
        project_path = self.temp_dir
        profiles_file = project_path / "profiles.yml"
        profiles_file.write_text("invalid: yaml: content: [")

        result = DatabricksConnector.from_dbt_profiles(project_path)

        assert result is None

    def test_from_dbt_profiles_null_bytes_in_file(self):
        """Test creating connector with null bytes in profiles.yml."""
        project_path = self.temp_dir
        profiles_file = project_path / "profiles.yml"

        profiles_content = """
        test_profile:
          target: dev
          outputs:
            dev:
              type: databricks
              host: test.databricks.com\x00
              http_path: /sql/1.0/warehouses/test\x00
              token: test_token
              catalog: test_catalog
              schema: test_schema
        """

        profiles_file.write_text(profiles_content)

        with patch("dbt_projects_cli.integrations.databricks.sql"):
            with patch(
                "dbt_projects_cli.integrations.databricks.logger"
            ) as mock_logger:
                result = DatabricksConnector.from_dbt_profiles(project_path)

        # Should warn about null bytes but still create connector
        mock_logger.warning.assert_called()
        assert result is not None
        assert "\x00" not in result.config.server_hostname

    def test_from_dbt_profiles_success(self):
        """Test successful creation from dbt profiles.yml."""
        project_path = self.temp_dir
        profiles_file = project_path / "profiles.yml"

        profiles_content = {
            "test_profile": {
                "target": "dev",
                "outputs": {
                    "dev": {
                        "type": "databricks",
                        "host": "test.databricks.com",
                        "http_path": "/sql/1.0/warehouses/test",
                        "token": "test_token",
                        "catalog": "test_catalog",
                        "schema": "test_schema",
                    }
                },
            }
        }

        with open(profiles_file, "w") as f:
            yaml.dump(profiles_content, f)

        with patch("dbt_projects_cli.integrations.databricks.sql"):
            result = DatabricksConnector.from_dbt_profiles(project_path)

        assert result is not None
        assert result.config.server_hostname == "test.databricks.com"
        assert result.config.catalog == "test_catalog"

    def test_from_dbt_profiles_oauth_with_credentials(self):
        """Test creation from profiles.yml with OAuth and client credentials."""
        project_path = self.temp_dir
        profiles_file = project_path / "profiles.yml"

        profiles_content = {
            "test_profile": {
                "target": "dev",
                "outputs": {
                    "dev": {
                        "type": "databricks",
                        "host": "test.databricks.com",
                        "http_path": "/sql/1.0/warehouses/test",
                        "auth_type": "oauth",
                        "client_id": "test_client",
                        "client_secret": "test_secret",
                        "catalog": "test_catalog",
                        "schema": "test_schema",
                    }
                },
            }
        }

        with open(profiles_file, "w") as f:
            yaml.dump(profiles_content, f)

        with patch("dbt_projects_cli.integrations.databricks.sql"):
            result = DatabricksConnector.from_dbt_profiles(project_path)

        assert result is not None
        assert result.config.auth_type == "oauth"
        assert result.config.client_id == "test_client"
        assert result.config.client_secret == "test_secret"

    def test_from_dbt_profiles_oauth_browser_only(self):
        """Test creation from profiles.yml with OAuth browser authentication."""
        project_path = self.temp_dir
        profiles_file = project_path / "profiles.yml"

        profiles_content = {
            "test_profile": {
                "target": "dev",
                "outputs": {
                    "dev": {
                        "type": "databricks",
                        "host": "test.databricks.com",
                        "http_path": "/sql/1.0/warehouses/test",
                        "auth_type": "oauth",
                        "catalog": "test_catalog",
                        "schema": "test_schema",
                    }
                },
            }
        }

        with open(profiles_file, "w") as f:
            yaml.dump(profiles_content, f)

        with patch("dbt_projects_cli.integrations.databricks.sql"):
            result = DatabricksConnector.from_dbt_profiles(project_path)

        assert result is not None
        assert result.config.auth_type == "oauth"
        assert result.config.client_id is None
        assert result.config.client_secret is None

    def test_from_dbt_profiles_non_databricks_profile(self):
        """Test creation from profiles.yml with non-Databricks profile."""
        project_path = self.temp_dir
        profiles_file = project_path / "profiles.yml"

        profiles_content = {
            "test_profile": {
                "target": "dev",
                "outputs": {
                    "dev": {"type": "postgres", "host": "localhost", "port": 5432}
                },
            }
        }

        with open(profiles_file, "w") as f:
            yaml.dump(profiles_content, f)

        result = DatabricksConnector.from_dbt_profiles(project_path)

        assert result is None

    def test_from_dbt_profiles_missing_required_fields(self):
        """Test creation from profiles.yml with missing required fields."""
        project_path = self.temp_dir
        profiles_file = project_path / "profiles.yml"

        profiles_content = {
            "test_profile": {
                "target": "dev",
                "outputs": {
                    "dev": {
                        "type": "databricks",
                        # Missing host and http_path
                        "token": "test_token",
                    }
                },
            }
        }

        with open(profiles_file, "w") as f:
            yaml.dump(profiles_content, f)

        result = DatabricksConnector.from_dbt_profiles(project_path)

        assert result is None

    @patch("dbt_projects_cli.integrations.databricks.sql")
    def test_execute_query_success(self, mock_sql):
        """Test successful query execution."""
        # Mock the SQL connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock cursor description and fetchall
        mock_cursor.description = [("column1", "string"), ("column2", "int")]
        mock_cursor.fetchall.return_value = [("value1", 123), ("value2", 456)]

        connector = DatabricksConnector(self.config)

        with patch.object(connector, "connection") as mock_conn_context:
            mock_conn_context.return_value.__enter__.return_value = mock_connection
            result = connector.execute_query("SELECT * FROM test_table", max_rows=5)

        assert result["columns"] == [
            {"name": "column1", "type": "string"},
            {"name": "column2", "type": "int"},
        ]
        assert len(result["rows"]) == 2
        assert result["rows"][0] == {"column1": "value1", "column2": 123}

    @patch("dbt_projects_cli.integrations.databricks.sql")
    def test_execute_query_failure(self, mock_sql):
        """Test query execution failure."""
        connector = DatabricksConnector(self.config)

        with patch.object(connector, "connection") as mock_conn_context:
            mock_conn_context.side_effect = Exception("Connection failed")

            with pytest.raises(DatabricksQueryError, match="Query failed"):
                connector.execute_query("SELECT * FROM test_table")

    @patch("dbt_projects_cli.integrations.databricks.sql")
    def test_test_connection_success(self, mock_sql):
        """Test successful connection test."""
        connector = DatabricksConnector(self.config)

        with patch.object(connector, "execute_query") as mock_execute:
            mock_execute.return_value = {"rows": [{"test_connection": 1}]}

            result = connector.test_connection()

            assert result is True
            mock_execute.assert_called_once_with(
                "SELECT 1 as test_connection", max_rows=1
            )

    @patch("dbt_projects_cli.integrations.databricks.sql")
    def test_test_connection_failure(self, mock_sql):
        """Test connection test failure."""
        connector = DatabricksConnector(self.config)

        with patch.object(connector, "execute_query") as mock_execute:
            mock_execute.side_effect = Exception("Connection failed")

            result = connector.test_connection()

            assert result is False

    @patch("dbt_projects_cli.integrations.databricks.sql")
    def test_get_table_info_success(self, mock_sql):
        """Test successful table info retrieval."""
        connector = DatabricksConnector(self.config)

        mock_schema_result = {"columns": ["column_info"], "rows": []}
        mock_sample_result = {"columns": ["col1"], "rows": [{"col1": "value1"}]}

        with patch.object(connector, "execute_query") as mock_execute:
            mock_execute.side_effect = [mock_schema_result, mock_sample_result]

            result = connector.get_table_info("test_catalog.test_schema.test_table")

            assert result["table_name"] == "test_catalog.test_schema.test_table"
            assert result["schema_info"] == mock_schema_result
            assert result["sample_data"] == mock_sample_result

    @patch("dbt_projects_cli.integrations.databricks.sql")
    def test_list_tables_success(self, mock_sql):
        """Test successful table listing."""
        connector = DatabricksConnector(self.config)

        mock_result = {
            "rows": [
                {"tableName": "table1"},
                {"table_name": "table2"},
                {"some_field": "table3"},  # Test fallback to first value
            ]
        }

        with patch.object(connector, "execute_query") as mock_execute:
            mock_execute.return_value = mock_result

            result = connector.list_tables()

            expected_tables = [
                "test_catalog.test_schema.table1",
                "test_catalog.test_schema.table2",
                "test_catalog.test_schema.table3",
            ]
            assert result == expected_tables


class TestCreateDatabricksConnector:
    """Test cases for create_databricks_connector function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())

    def test_create_databricks_connector_success(self):
        """Test successful creation of Databricks connector."""
        profiles_file = self.temp_dir / "profiles.yml"

        profiles_content = {
            "test_profile": {
                "target": "dev",
                "outputs": {
                    "dev": {
                        "type": "databricks",
                        "host": "test.databricks.com",
                        "http_path": "/sql/1.0/warehouses/test",
                        "token": "test_token",
                        "catalog": "test_catalog",
                        "schema": "test_schema",
                    }
                },
            }
        }

        with open(profiles_file, "w") as f:
            yaml.dump(profiles_content, f)

        with patch("dbt_projects_cli.integrations.databricks.sql"):
            result = create_databricks_connector(self.temp_dir)

        assert result is not None
        assert isinstance(result, DatabricksConnector)

    def test_create_databricks_connector_no_profiles(self):
        """Test creating connector when no profiles.yml exists."""
        with patch("builtins.print"):  # Mock console.print
            result = create_databricks_connector(self.temp_dir)

        assert result is None

    def test_create_databricks_connector_import_error(self):
        """Test creating connector when databricks.sql is not available."""
        with patch("dbt_projects_cli.integrations.databricks.sql", None):
            with patch("builtins.print"):  # Mock console.print
                result = create_databricks_connector(self.temp_dir)

        assert result is None


class TestDatabricksQueryError:
    """Test cases for DatabricksQueryError exception."""

    def test_databricks_query_error_creation(self):
        """Test creating DatabricksQueryError."""
        error = DatabricksQueryError("Test error message")

        assert str(error) == "Test error message"
        assert isinstance(error, Exception)
