"""
Databricks SQL warehouse integration for sample data querying.
"""

import atexit
import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

try:
    from databricks import sql
except ImportError:
    sql = None  # type: ignore

from rich.console import Console

console = Console()
logger = logging.getLogger(__name__)


@dataclass
class DatabricksConfig:
    """Configuration for Databricks SQL warehouse connection."""

    server_hostname: str
    http_path: str
    catalog: Optional[str] = None
    schema: Optional[str] = None
    # Authentication - either token or OAuth
    access_token: Optional[str] = None
    auth_type: str = "token"  # "token" or "oauth"
    client_id: Optional[str] = None
    client_secret: Optional[str] = None


class DatabricksQueryError(Exception):
    """Exception raised when Databricks query fails."""

    pass


class DatabricksConnector:
    """Handles connections and queries to Databricks SQL warehouse."""

    # Class-level connection cache to reuse connections
    _connection_cache: Dict[str, Any] = {}
    _cleanup_registered = False

    def __init__(self, config: DatabricksConfig):
        if sql is None:
            raise ImportError(
                "databricks-sql-connector is required for Databricks integration. "
                "Install with: pip install databricks-sql-connector"
            )

        self.config = config
        self._connection = None
        self._connection_key = self._create_connection_key()

        # Register cleanup handler once for the class
        if not DatabricksConnector._cleanup_registered:
            atexit.register(DatabricksConnector._cleanup_all_connections)
            DatabricksConnector._cleanup_registered = True

    def _create_connection_key(self) -> str:
        """Create a unique key for this connection configuration."""
        key_parts = [
            self.config.server_hostname,
            self.config.http_path,
            self.config.auth_type,
            self.config.catalog or "",
            self.config.schema or "",
        ]

        # Add auth-specific parts
        if self.config.auth_type == "oauth":
            key_parts.extend(
                [self.config.client_id or "", self.config.client_secret or ""]
            )
        else:
            key_parts.append(self.config.access_token or "")

        return "|".join(key_parts)

    @classmethod
    def _cleanup_all_connections(cls) -> None:
        """Clean up all cached connections to prevent ThriftBackend shutdown errors."""
        if cls._connection_cache:
            logger.debug("🧹 Cleaning up Databricks connections...")
            for connection_key, connection in cls._connection_cache.items():
                try:
                    connection.close()
                except Exception:
                    # Ignore errors during cleanup - connection might already be closed
                    pass
            cls._connection_cache.clear()

    @classmethod
    def from_environment(cls) -> "DatabricksConnector":
        """Create connector from environment variables."""
        required_vars = {
            "DATABRICKS_SERVER_HOSTNAME": "server_hostname",
            "DATABRICKS_HTTP_PATH": "http_path",
            "DATABRICKS_ACCESS_TOKEN": "access_token",
        }

        config_values = {}
        missing_vars = []

        for env_var, config_key in required_vars.items():
            value = os.getenv(env_var)
            if value:
                config_values[config_key] = value
            else:
                missing_vars.append(env_var)

        if missing_vars:
            raise ValueError(
                f"Missing required environment variables for Databricks "
                f"connection: {', '.join(missing_vars)}\\n"
                "Set these environment variables:\\n"
                "  export DATABRICKS_SERVER_HOSTNAME="
                "your-workspace.cloud.databricks.com\\n"
                "  export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id\\n"
                "  export DATABRICKS_ACCESS_TOKEN=your-access-token"
            )

        # Optional variables
        catalog = os.getenv("DATABRICKS_CATALOG")
        schema = os.getenv("DATABRICKS_SCHEMA")
        if catalog:
            config_values["catalog"] = catalog
        if schema:
            config_values["schema"] = schema

        return cls(DatabricksConfig(**config_values))

    @classmethod
    def from_dbt_profiles(
        cls,
        project_path: Path,
        profile_name: Optional[str] = None,
        target: Optional[str] = None,
        verbose: bool = False,
    ) -> Optional["DatabricksConnector"]:
        """Create connector from dbt profiles.yml file."""

        def expand_env_vars(value: Any) -> Any:
            """Expand environment variables in dbt profile values."""
            if isinstance(value, str):
                # Handle dbt's {{ env_var("VAR_NAME") }} syntax
                import re

                env_var_pattern = r"{{\s*env_var\(['\"]([^'\"]+)['\"]\)\s*}}"
                matches = re.findall(env_var_pattern, value)
                for var_name in matches:
                    env_value = os.getenv(var_name)
                    if env_value:
                        value = value.replace(
                            f"{{{{ env_var('{var_name}') }}}}", env_value
                        )
                        value = value.replace(
                            f'{{{{ env_var("{var_name}") }}}}', env_value
                        )
            return value

        # Look for profiles.yml specifically in the dbt project root
        project_profiles_path = project_path / "profiles.yml"

        if not project_profiles_path.exists():
            logger.error(f"No profiles.yml found in dbt project root: {project_path}")
            logger.warning(
                "This dbt project is not configured for sample data generation."
            )
            logger.warning(
                f"Create a profiles.yml file in {project_path} with "
                f"Databricks connection details."
            )
            return None

        # Load the project-specific profiles.yml with null byte protection
        try:
            with open(project_profiles_path, "r", encoding="utf-8") as f:
                content = f.read()
                # Remove null bytes that could cause compilation errors
                if "\x00" in content:
                    logger.warning(
                        f"Removing null bytes from profiles.yml: "
                        f"{project_profiles_path}"
                    )
                    content = content.replace("\x00", "")
                profiles_data = yaml.safe_load(content)
                logger.debug(f"Loaded profiles.yml from {project_profiles_path}")
        except Exception as e:
            logger.error(
                f"Error loading profiles.yml from {project_profiles_path}: {e}"
            )
            return None

        if not profiles_data:
            logger.error(f"Empty or invalid profiles.yml in {project_profiles_path}")
            return None

        # Determine which profile to use
        if not profile_name:
            # Try to auto-detect profile name from available profiles
            available_profiles = list(profiles_data.keys())
            if len(available_profiles) == 1:
                profile_name = available_profiles[0]
                logger.info(f"Auto-selected profile: {profile_name}")
            else:
                logger.warning(f"Multiple profiles found: {available_profiles}")
                logger.warning("Specify profile with --profile option")
                return None

        if profile_name not in profiles_data:
            logger.error(f"Profile '{profile_name}' not found in profiles.yml")
            logger.warning(f"Available profiles: {list(profiles_data.keys())}")
            return None

        profile_config = profiles_data[profile_name]

        # Determine target
        if not target:
            target = profile_config.get("target", "dev")

        if "outputs" not in profile_config or target not in profile_config["outputs"]:
            logger.error(f"Target '{target}' not found in profile '{profile_name}'")
            available_targets = list(profile_config.get("outputs", {}).keys())
            logger.warning(f"Available targets: {available_targets}")
            return None

        target_config = profile_config["outputs"][target]

        # Check if this is a Databricks profile
        if target_config.get("type") != "databricks":
            logger.warning(
                f"Profile '{profile_name}' target '{target}' is not a "
                f"Databricks connection"
            )
            logger.warning(f"Connection type: {target_config.get('type', 'unknown')}")
            return None

        # Extract configuration
        try:
            # Function to sanitize configuration values
            def sanitize_config_value(value: Any) -> Any:
                if value and isinstance(value, str) and "\x00" in value:
                    logger.warning(
                        f"Removing null bytes from config value: {value[:50]}..."
                    )
                    return value.replace("\x00", "")
                return value

            config_values = {
                "server_hostname": sanitize_config_value(
                    expand_env_vars(target_config.get("host"))
                ),
                "http_path": sanitize_config_value(
                    expand_env_vars(target_config.get("http_path"))
                ),
                "catalog": sanitize_config_value(
                    expand_env_vars(target_config.get("catalog"))
                ),
                "schema": sanitize_config_value(
                    expand_env_vars(target_config.get("schema"))
                ),
            }

            # Handle authentication
            auth_type = target_config.get("auth_type", "token")
            config_values["auth_type"] = auth_type

            if auth_type == "oauth":
                # Check if client credentials are provided (M2M OAuth) or use
                # browser OAuth
                client_id = sanitize_config_value(
                    expand_env_vars(target_config.get("client_id"))
                )
                client_secret = sanitize_config_value(
                    expand_env_vars(target_config.get("client_secret"))
                )

                if client_id and client_secret:
                    # Machine-to-machine OAuth with client credentials
                    config_values["client_id"] = client_id
                    config_values["client_secret"] = client_secret
                    logger.debug("Using OAuth with client credentials")
                else:
                    # Browser-based OAuth (no client credentials needed)
                    config_values["client_id"] = None
                    config_values["client_secret"] = None
                    logger.debug("Using OAuth with browser authentication")

            elif auth_type == "token":
                config_values["access_token"] = sanitize_config_value(
                    expand_env_vars(target_config.get("token"))
                )

                if not config_values["access_token"]:
                    logger.error(f"Access token missing for profile '{profile_name}'")
                    logger.warning("Check token in profiles.yml")
                    return None

            else:
                logger.error(
                    f"Unsupported auth_type '{auth_type}' in profile '{profile_name}'"
                )
                logger.warning("Supported auth types: 'oauth', 'token'")
                return None

            # Validate required fields
            if not config_values["server_hostname"] or not config_values["http_path"]:
                logger.error(
                    f"Missing required connection info in profile '{profile_name}'"
                )
                logger.warning("Check host and http_path in profiles.yml")
                return None

            logger.info(
                f"Loaded Databricks profile '{profile_name}' (target: {target})"
            )
            logger.debug(f"Host: {config_values['server_hostname']}")
            logger.debug(f"Auth: {auth_type}")
            if config_values.get("catalog"):
                logger.debug(f"Catalog: {config_values['catalog']}")
            if config_values.get("schema"):
                logger.debug(f"Schema: {config_values['schema']}")

            return cls(DatabricksConfig(**config_values))

        except Exception as e:
            console.print(f"[red]❌ Error parsing profile '{profile_name}': {e}[/red]")
            return None

    @contextmanager
    def connection(self) -> Any:
        """Context manager for Databricks SQL connection with caching."""
        connection_used = None
        try:
            # Check if we have a cached connection for this configuration
            if self._connection_key in self._connection_cache:
                connection_used = self._connection_cache[self._connection_key]
                logger.debug("Reusing cached Databricks connection")
            else:
                logger.debug("Creating new Databricks SQL connection...")

                # Build connection parameters based on auth type
                connection_params = {
                    "server_hostname": self.config.server_hostname,
                    "http_path": self.config.http_path,
                }

                if self.config.auth_type == "oauth":
                    connection_params["auth_type"] = "oauth"

                    if self.config.client_id and self.config.client_secret:
                        # Machine-to-machine OAuth
                        connection_params.update(
                            {
                                "client_id": self.config.client_id,
                                "client_secret": self.config.client_secret,
                            }
                        )
                        logger.debug("Using OAuth with client credentials")
                    else:
                        # Browser-based OAuth (no client credentials needed)
                        logger.debug("Using OAuth with browser authentication")
                        logger.info(
                            "OAuth authentication will open your browser for login"
                        )
                else:
                    if self.config.access_token:
                        connection_params["access_token"] = self.config.access_token
                    logger.debug("Using token authentication")

                connection_used = sql.connect(**connection_params)

                # Cache the connection for reuse
                self._connection_cache[self._connection_key] = connection_used
                logger.debug("Connection cached for reuse")

            yield connection_used

        except Exception as e:
            # If connection fails, remove it from cache
            if self._connection_key in self._connection_cache:
                try:
                    self._connection_cache[self._connection_key].close()
                except Exception:
                    pass
                del self._connection_cache[self._connection_key]
                logger.debug("Removed failed connection from cache")

            logger.error(f"Databricks connection failed: {e}")
            raise DatabricksQueryError(f"Failed to connect to Databricks: {e}")

        # Note: We don't close the connection here since it's cached for reuse
        # Connections will be closed when the process terminates or cache is cleared

    def execute_query(self, query: str, max_rows: int = 5) -> Dict[str, Any]:
        """
        Execute a query and return results with column metadata.

        Args:
            query: SQL query to execute
            max_rows: Maximum number of rows to return

        Returns:
            Dictionary with 'columns' and 'rows' keys
        """
        try:
            with self.connection() as conn:
                with conn.cursor() as cursor:
                    logger.debug(
                        f"Executing query: {query[:100]}"
                        f"{'...' if len(query) > 100 else ''}"
                    )

                    cursor.execute(query)

                    # Get column metadata
                    columns = []
                    if cursor.description:
                        for desc in cursor.description:
                            columns.append(
                                {
                                    "name": desc[0],
                                    "type": str(desc[1]) if desc[1] else "unknown",
                                }
                            )

                    # Fetch rows (limited)
                    rows = []
                    for i, row in enumerate(cursor.fetchall()):
                        if i >= max_rows:
                            break

                        # Convert row to dictionary
                        row_dict = {}
                        for j, value in enumerate(row):
                            if j < len(columns):
                                row_dict[columns[j]["name"]] = value

                        rows.append(row_dict)

                    logger.info(
                        f"Query executed successfully, {len(rows)} rows returned"
                    )

                    return {"columns": columns, "rows": rows, "query": query}

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise DatabricksQueryError(f"Query failed: {e}")

    def test_connection(self) -> bool:
        """Test the Databricks connection."""
        try:
            test_query = "SELECT 1 as test_connection"
            result = self.execute_query(test_query, max_rows=1)
            return len(result.get("rows", [])) > 0
        except Exception as e:
            logger.warning(f"Connection test failed: {e}")
            return False

    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a table including columns and sample data.

        Args:
            table_name: Fully qualified table name (e.g., 'catalog.schema.table')

        Returns:
            Dictionary with table metadata and sample data
        """
        try:
            # First, try to get table schema
            describe_query = f"DESCRIBE TABLE {table_name}"

            try:
                schema_result = self.execute_query(describe_query, max_rows=100)
                logger.info(f"Got schema for table {table_name}")
            except Exception as e:
                logger.warning(f"Could not describe table {table_name}: {e}")
                schema_result = None

            # Get sample data
            sample_query = f"SELECT * FROM {table_name} LIMIT 5"
            sample_result = self.execute_query(sample_query, max_rows=5)

            # Combine results
            result = {
                "table_name": table_name,
                "sample_data": sample_result,
                "schema_info": schema_result,
            }

            return result

        except Exception as e:
            logger.error(f"Failed to get table info for {table_name}: {e}")
            raise DatabricksQueryError(f"Failed to get table info: {e}")

    def list_tables(
        self, catalog: Optional[str] = None, schema: Optional[str] = None
    ) -> List[str]:
        """
        List available tables in a catalog/schema.

        Args:
            catalog: Catalog name (uses config default if not provided)
            schema: Schema name (uses config default if not provided)

        Returns:
            List of table names
        """
        catalog = catalog or self.config.catalog or "hive_metastore"
        schema = schema or self.config.schema or "default"

        try:
            query = f"SHOW TABLES IN {catalog}.{schema}"
            result = self.execute_query(query, max_rows=100)

            tables = []
            for row in result.get("rows", []):
                # SHOW TABLES returns different formats depending on Databricks version
                if isinstance(row, dict):
                    table_name = (
                        row.get("tableName")
                        or row.get("table_name")
                        or list(row.values())[0]
                    )
                else:
                    table_name = str(row)

                if table_name:
                    tables.append(f"{catalog}.{schema}.{table_name}")

            return tables

        except Exception as e:
            logger.warning(f"Could not list tables in {catalog}.{schema}: {e}")
            return []


def create_databricks_connector(
    project_path: Path,
    profile_name: Optional[str] = None,
    target: Optional[str] = None,
    verbose: bool = False,
) -> Optional[DatabricksConnector]:
    """
    Create a Databricks connector for a specific dbt project.

    Args:
        project_path: Path to the dbt project root
        profile_name: Specific dbt profile to use
        target: Specific dbt target to use

    Returns:
        DatabricksConnector instance or None if configuration is not available
    """
    try:
        # Try to load from project's profiles.yml
        connector = DatabricksConnector.from_dbt_profiles(
            project_path, profile_name, target
        )
        if connector:
            return connector

        # If no project profiles.yml found, gracefully fail
        console.print(
            "[yellow]💡 No Databricks configuration found for this project[/yellow]"
        )
        console.print(
            f"[yellow]   To enable sample data generation, create a "
            f"profiles.yml in {project_path}[/yellow]"
        )
        return None

    except ImportError as e:
        console.print(
            f"[yellow]⚠️  Databricks SQL connector not available: {e}[/yellow]"
        )
        return None


def test_databricks_connection() -> bool:
    """Test Databricks connection and return success status."""
    try:
        connector = create_databricks_connector(Path("."))
        if connector:
            return connector.test_connection()
        return False
    except Exception as e:
        console.print(f"[red]❌ Databricks connection test failed: {e}[/red]")
        return False
