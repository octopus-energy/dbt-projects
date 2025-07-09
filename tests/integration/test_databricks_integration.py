import os
from unittest.mock import MagicMock, patch

import pytest

from dbt_projects_cli.integrations.databricks import (
    DatabricksConnector,
    DatabricksQueryError,
)


class TestDatabricksIntegration:
    """Integration tests for Databricks connector operations."""

    @patch("dbt_projects_cli.integrations.databricks.sql.connect")
    def test_connection_creation_from_environment_success(self, mock_sql_connect):
        """Test creating connection from environment variables successfully."""
        env_vars = {
            "DATABRICKS_SERVER_HOSTNAME": "test.databricks.com",
            "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/test",
            "DATABRICKS_ACCESS_TOKEN": "fake-token",
        }

        with patch.dict(os.environ, env_vars):
            connector = DatabricksConnector.from_environment()
            assert connector is not None
            assert connector.config.server_hostname == "test.databricks.com"

    def test_connection_creation_missing_env_vars(self):
        """Test connection creation with missing environment variables."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(
                ValueError, match="Missing required environment variables"
            ):
                DatabricksConnector.from_environment()

    @patch("dbt_projects_cli.integrations.databricks.sql.connect")
    def test_query_execution_success(self, mock_sql_connect):
        """Test successful execution of a SQL query."""
        mock_conn = MagicMock()
        mock_sql_connect.return_value = mock_conn
        mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
        mock_cursor.fetchall.return_value = [("value1", 123)]
        mock_cursor.description = [("column1", "string"), ("column2", "int")]

        env_vars = {
            "DATABRICKS_SERVER_HOSTNAME": "test.databricks.com",
            "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/test",
            "DATABRICKS_ACCESS_TOKEN": "fake-token",
        }

        with patch.dict(os.environ, env_vars):
            connector = DatabricksConnector.from_environment()
        result = connector.execute_query("SELECT * FROM test_table")

        assert result["columns"] == [
            {"name": "column1", "type": "string"},
            {"name": "column2", "type": "int"},
        ]
        assert result["rows"] == [{"column1": "value1", "column2": 123}]

    @patch("dbt_projects_cli.integrations.databricks.sql.connect")
    def test_query_execution_failure(self, mock_sql_connect):
        """Test handling of query execution failure."""
        # Clear any cached connections to ensure fresh state
        DatabricksConnector._connection_cache.clear()

        # Set up the mock to fail when connecting
        mock_sql_connect.side_effect = Exception("Connection failed")

        env_vars = {
            "DATABRICKS_SERVER_HOSTNAME": "test.databricks.com",
            "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/test",
            "DATABRICKS_ACCESS_TOKEN": "fake-token",
        }

        with patch.dict(os.environ, env_vars):
            connector = DatabricksConnector.from_environment()
            with pytest.raises(DatabricksQueryError, match="Query failed"):
                connector.execute_query("SELECT * FROM test_table")

    @patch("dbt_projects_cli.integrations.databricks.sql.connect")
    def test_list_tables_success(self, mock_sql_connect):
        """Test listing tables successfully."""
        mock_conn = MagicMock()
        mock_sql_connect.return_value = mock_conn
        mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
        mock_cursor.fetchall.return_value = [("table1",), ("table2",)]
        mock_cursor.description = [("tableName", "string")]

        env_vars = {
            "DATABRICKS_SERVER_HOSTNAME": "test.databricks.com",
            "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/test",
            "DATABRICKS_ACCESS_TOKEN": "fake-token",
        }

        with patch.dict(os.environ, env_vars):
            connector = DatabricksConnector.from_environment()
        tables = connector.list_tables(catalog="test_catalog", schema="test_schema")

        assert tables == [
            "test_catalog.test_schema.table1",
            "test_catalog.test_schema.table2",
        ]
