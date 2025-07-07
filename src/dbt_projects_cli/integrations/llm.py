"""
LLM integration for generating dbt model and column descriptions.
"""

import logging
import os
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import openai
import yaml
from anthropic import Anthropic
from rich.console import Console

console = Console()
logger = logging.getLogger(__name__)


class LLMProvider(Enum):
    """Supported LLM providers."""

    OPENAI = "openai"
    ANTHROPIC = "anthropic"


@dataclass
class ModelContext:
    """Context information about a dbt model for LLM processing."""

    name: str
    sql_content: str
    existing_description: Optional[str] = None
    columns: Optional[List[Dict[str, str]]] = None
    dependencies: Optional[List[str]] = None
    project_name: str = ""
    schema_name: str = ""


@dataclass
class DescriptionResult:
    """Result of LLM description generation."""

    model_description: str
    column_descriptions: Dict[str, str]
    confidence_score: float = 0.0


class LLMDescriptionGenerator:
    """Generates descriptions for dbt models and columns using LLMs."""

    # Default models - can be overridden via environment variables
    DEFAULT_MODELS = {
        LLMProvider.OPENAI: "gpt-4o",  # Latest GPT-4 Omni model
        LLMProvider.ANTHROPIC: "claude-3-5-sonnet-20241022",  # Latest Claude 3.5 Sonnet
    }

    def __init__(
        self, provider: LLMProvider = LLMProvider.OPENAI, model: Optional[str] = None
    ):
        self.provider = provider
        self.model = model  # Allow custom model override
        self.config = self._load_model_config()
        self._setup_client()

    def _setup_client(self) -> None:
        """Setup the LLM client based on the provider."""
        if self.provider == LLMProvider.OPENAI:
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                raise ValueError("OPENAI_API_KEY environment variable not set")
            self.client: Union[openai.OpenAI, Anthropic] = openai.OpenAI(
                api_key=api_key
            )

            # Use custom model, environment variable, config file, or default
            self.model = (
                self.model
                or os.getenv("OPENAI_MODEL")
                or self._get_config_model(self.provider)
                or self.DEFAULT_MODELS[LLMProvider.OPENAI]
            )

        elif self.provider == LLMProvider.ANTHROPIC:
            api_key = os.getenv("ANTHROPIC_API_KEY")
            if not api_key:
                raise ValueError("ANTHROPIC_API_KEY environment variable not set")
            self.client = Anthropic(api_key=api_key)

            # Use custom model, environment variable, config file, or default
            self.model = (
                self.model
                or os.getenv("ANTHROPIC_MODEL")
                or self._get_config_model(self.provider)
                or self.DEFAULT_MODELS[LLMProvider.ANTHROPIC]
            )

    def _load_model_config(self) -> Dict[Any, Any]:
        """Load model configuration from config file."""
        try:
            config_path = Path(__file__).parent.parent / "config" / "models.yaml"
            if config_path.exists():
                with open(config_path, "r") as f:
                    result = yaml.safe_load(f)
                    return result or {}
        except Exception as e:
            console.print(f"[yellow]Warning: Could not load model config: {e}[/yellow]")

        return {}  # type: ignore[no-any-return]

    def _get_config_model(self, provider: LLMProvider) -> Optional[str]:
        """Get the default model for a provider from config file."""
        try:
            providers_config = self.config.get("providers", {})
            provider_config = providers_config.get(provider.value, {})
            return provider_config.get("default_model")  # type: ignore[no-any-return]
        except Exception:
            return None

    def generate_descriptions(
        self,
        model_context: ModelContext,
        expand_existing: bool = True,
        pii_protection_level: str = "high",
        dbt_profile: Optional[str] = None,
        dbt_target: Optional[str] = None,
        project_path: Optional[Path] = None,
        verbose: bool = False,
    ) -> DescriptionResult:
        """Generate descriptions for a dbt model and its columns."""

        prompt = self._build_description_prompt(
            model_context, expand_existing, pii_protection_level, project_path
        )

        # Log what's being sent to the LLM (for debugging and transparency)
        if verbose:
            self._log_llm_prompt(prompt, model_context.name)

        try:
            if self.provider == LLMProvider.OPENAI:
                response = self._call_openai(prompt)
            elif self.provider == LLMProvider.ANTHROPIC:
                response = self._call_anthropic(prompt)
            else:
                raise ValueError(f"Unsupported provider: {self.provider}")

            return self._parse_response(response)

        except Exception as e:
            console.print(f"[red]Error generating descriptions: {e}[/red]")
            raise

    def _build_description_prompt(
        self,
        context: ModelContext,
        expand_existing: bool,
        pii_protection_level: str = "high",
        project_path: Optional[Path] = None,
    ) -> str:
        """Build the prompt for the LLM to generate descriptions."""

        prompt_parts = [
            "You are a data analyst helping to document dbt models. ",
            "Generate clear, concise descriptions for the model and its "
            "columns based on the SQL logic.\n\n",
        ]

        # Model information
        prompt_parts.append(f"**Model Name:** {context.name}\n")
        if context.project_name:
            prompt_parts.append(f"**Project:** {context.project_name}\n")
        if context.schema_name:
            prompt_parts.append(f"**Schema:** {context.schema_name}\n")

        # Try to get sample data for better context
        sample_data = self._get_databricks_sample_data(
            context, pii_protection_level, project_path
        )
        if sample_data:
            prompt_parts.append(f"**Source Table:** {sample_data['source_table']}\n")
            if sample_data.get("sample_rows"):
                prompt_parts.append("**Sample Data (first 3 rows):**\n")

                # Add important context about PII protection
                protection_info = sample_data.get("pii_protection", {})
                if protection_info.get("protection_applied"):
                    method = protection_info.get("method", "unknown")
                    if method == "hash":
                        prompt_parts.append(
                            "**IMPORTANT:** Values in format "
                            "[PII_PLACEHOLDER:xxxxxxxx] are temporary placeholders "
                            "created for this analysis only. They are NOT actual "
                            "values stored in the database. Do not describe these "
                            "as 'hashed identifiers' or refer to privacy protection "
                            "- instead describe the column's actual business "
                            "purpose.\n\n"
                        )
                    elif method == "exclude":
                        prompt_parts.append(
                            "**IMPORTANT:** Values showing '[REDACTED]' are "
                            "privacy-protected placeholders created for this analysis "
                            "only. They are NOT actual values stored in the database. "
                            "Do not describe these as 'redacted' or refer to privacy "
                            "protection - instead describe the column's actual "
                            "business purpose.\n\n"
                        )
                    elif method == "mask":
                        prompt_parts.append(
                            "**IMPORTANT:** Some values appear masked (e.g., "
                            "'t***@e******.com', '**********67') for privacy "
                            "protection during this analysis. These are NOT the "
                            "actual values stored in the database. Do not describe "
                            "these as 'masked' or refer to privacy protection - "
                            "instead describe the column's actual business "
                            "purpose.\n\n"
                        )
                    else:
                        prompt_parts.append(
                            "**IMPORTANT:** Some values may be privacy-protected "
                            "placeholders created for this analysis only. They are "
                            "NOT actual values stored in the database. Focus on "
                            "describing the column's actual business purpose.\n\n"
                        )

                for i, row in enumerate(sample_data["sample_rows"]):
                    prompt_parts.append(f"Row {i+1}: {row}\n")
                prompt_parts.append("\n")

        # Existing description context
        if context.existing_description and expand_existing:
            prompt_parts.append(
                f"**Existing Description:** {context.existing_description}\n"
            )
            prompt_parts.append(
                "Please expand and improve the existing description.\n\n"
            )
        elif context.existing_description:
            prompt_parts.append(
                f"**Current Description:** {context.existing_description}\n"
            )
            prompt_parts.append(
                "Use this as context but provide a fresh, comprehensive "
                "description.\n\n"
            )

        # SQL content
        prompt_parts.append("**SQL Content:**\n```sql\n")
        prompt_parts.append(context.sql_content)
        prompt_parts.append("\n```\n\n")

        # Dependencies
        if context.dependencies:
            prompt_parts.append(
                "**Dependencies:** " + ", ".join(context.dependencies) + "\n\n"
            )

        # Column information
        if context.columns:
            prompt_parts.append("**Known Columns:**\n")
            for col in context.columns:
                col_name = col.get("name", "")
                col_type = col.get("type", "")
                col_desc = col.get("description", "")
                prompt_parts.append(f"- {col_name} ({col_type})")
                if col_desc:
                    prompt_parts.append(f": {col_desc}")
                prompt_parts.append("\n")
            prompt_parts.append("\n")

        # Instructions
        prompt_parts.extend(
            [
                "**Instructions:**\n",
                "1. Provide a clear, business-focused description of what this "
                "model represents\n",
                "2. Explain the model's purpose and key business logic\n",
                "3. For each column, provide a concise description focusing on "
                "business meaning\n",
                "4. Use professional, documentation-style language\n",
                "5. Avoid overly technical jargon unless necessary\n",
                "6. DO NOT include column descriptions in the model description\n",
                "7. DO NOT use markdown formatting (**, -, `) in descriptions\n",
                "8. Keep descriptions concise and focused\n",
                "9. Do not repeat the table name in the model description\n",
                "10. **CRITICAL:** You must preserve column names EXACTLY as "
                "they appear in the data/SQL. Do not alter underscores, "
                "capitalization, or any characters. Column names are database "
                "identifiers that must remain unchanged.\n",
                "11. **CRITICAL:** Privacy-protected values in sample data "
                "(including [PII_PLACEHOLDER:xxxxxxxx], [REDACTED], or masked "
                "values like 't***@e******.com') are temporary placeholders, NOT "
                "actual database values. Focus on the column's business purpose "
                "based on its name and SQL context, not these placeholders.\n\n",
                "**Response Format:**\n",
                "MODEL_DESCRIPTION: [Your model description here - do not "
                "include column details]\n\n",
                "COLUMN_DESCRIPTIONS:\n",
                "column_name: Description of the column\n",
                "another_column: Description of another column\n",
            ]
        )

        return "".join(prompt_parts)

    def _log_llm_prompt(self, prompt: str, model_name: str) -> None:
        """Log the prompt being sent to the LLM for transparency and debugging."""
        console.print(f"[dim]📤 LLM Prompt for {model_name}:[/dim]")
        console.print(f"[dim]   Provider: {self.provider.value}[/dim]")
        console.print(f"[dim]   Model: {self.model}[/dim]")
        console.print(f"[dim]   Prompt length: {len(prompt)} characters[/dim]")

        # Show a truncated version of the prompt for debugging
        if len(prompt) > 1000:
            truncated_prompt = (
                prompt[:500] + "\n\n[... truncated ...]\n\n" + prompt[-200:]
            )
        else:
            truncated_prompt = prompt

        console.print("[dim]📝 Prompt content:[/dim]")
        console.print(f"[dim]{truncated_prompt}[/dim]")
        console.print(f"[dim]{'─' * 50}[/dim]")

    def _call_openai(self, prompt: str) -> str:
        """Call OpenAI API."""
        model_to_use = self.model or self.DEFAULT_MODELS[LLMProvider.OPENAI]
        if not isinstance(self.client, openai.OpenAI):
            raise ValueError("Expected OpenAI client but got different type")
        response = self.client.chat.completions.create(
            model=model_to_use,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a meticulous data analyst tasked with "
                        "documenting data models. CRITICAL: Preserve all database "
                        "column names exactly as provided in the input; do not "
                        "alter them in any way. Column names are identifiers that "
                        "must match the database schema precisely. IMPORTANT: "
                        "Privacy-protected values (including "
                        "[PII_PLACEHOLDER:xxxxxxxx], [REDACTED], or masked values) "
                        "are temporary placeholders, not actual data - focus on "
                        "the column's business meaning from context, not these "
                        "placeholder values."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            max_tokens=1500,
            temperature=0.3,
        )
        content = response.choices[0].message.content
        if content is None:
            raise ValueError("OpenAI API returned empty response")
        return content

    def _call_anthropic(self, prompt: str) -> str:
        """Call Anthropic API."""
        if not isinstance(self.client, Anthropic):
            raise ValueError("Expected Anthropic client but got different type")
        model_to_use = self.model or self.DEFAULT_MODELS[LLMProvider.ANTHROPIC]
        response = self.client.messages.create(
            model=model_to_use,
            max_tokens=1500,
            temperature=0.3,
            messages=[{"role": "user", "content": prompt}],
        )
        content_block = response.content[0]
        if hasattr(content_block, "text"):
            return str(content_block.text) if content_block.text else ""
        else:
            return ""

    def _parse_response(self, response: str) -> DescriptionResult:
        """Parse the LLM response to extract model and column descriptions."""

        # Extract model description
        model_desc_match = re.search(
            r"MODEL_DESCRIPTION:\s*(.*?)(?=\n\nCOLUMN_DESCRIPTIONS:|"
            r"\n\n[A-Z_]+:|$)",
            response,
            re.DOTALL,
        )
        model_description = (
            model_desc_match.group(1).strip() if model_desc_match else ""
        )

        # Clean model description to remove any column descriptions that leaked in
        model_description = self._clean_model_description(model_description)

        # Extract column descriptions
        column_descriptions = {}
        column_section_match = re.search(
            r"COLUMN_DESCRIPTIONS:\s*(.*?)$", response, re.DOTALL
        )

        if column_section_match:
            column_text = column_section_match.group(1).strip()
            # Parse column descriptions (format: "column_name: description")
            for line in column_text.split("\n"):
                line = line.strip()
                if ":" in line and not line.startswith("#"):
                    column_name, description = line.split(":", 1)
                    # Get the raw column name from LLM response
                    raw_col_name = column_name.strip()
                    clean_description = description.strip().replace("`", "")

                    # CRITICAL: Only use column names that are valid
                    # database identifiers
                    # Don't apply any "cleaning" that might alter the
                    # actual column name
                    if (
                        self._is_valid_database_column_name(raw_col_name)
                        and clean_description
                    ):
                        column_descriptions[raw_col_name] = clean_description
                    elif raw_col_name and clean_description:
                        # Log when LLM has altered a column name
                        console.print(
                            f"[yellow]⚠️  LLM altered column name: "
                            f"'{raw_col_name}' - skipping[/yellow]"
                        )

        return DescriptionResult(
            model_description=model_description,
            column_descriptions=column_descriptions,
            confidence_score=0.8,  # Default confidence score
        )

    def _get_databricks_sample_data(
        self,
        model_context: ModelContext,
        pii_protection_level: str = "high",
        project_path: Optional[Path] = None,
    ) -> Optional[Dict]:
        """Get sample data from Databricks using the CLI with PII protection."""
        try:
            # Construct the full table name for the model (not its sources)
            # Format: catalog.schema.model_name
            full_table_name = self._build_model_table_name(
                model_context.name, project_path
            )
            if not full_table_name:
                console.print(
                    f"[yellow]Could not determine table name for model "
                    f"{model_context.name}[/yellow]"
                )
                return None

            console.print(
                f"[blue]Attempting to fetch sample data from model table: "
                f"{full_table_name}...[/blue]"
            )
            console.print(
                f"[blue]🔒 PII protection level: {pii_protection_level}[/blue]"
            )

            # Try to get actual sample data (when SQL warehouse is configured)
            sample_data = self._fetch_sample_data_from_databricks(
                full_table_name, project_path
            )

            if sample_data:
                # Apply PII protection before sending to LLM
                from ..security.pii_protection import create_pii_detector

                pii_detector = create_pii_detector(pii_protection_level)

                protected_data = pii_detector.sanitize_sample_data(sample_data)

                # Show protection summary
                protection_info = protected_data.get("pii_protection", {})
                if protection_info.get("protection_applied"):
                    console.print(
                        f"[green]🛡️  PII protection applied: "
                        f"{protection_info['high_risk_columns']} "
                        f"high-risk columns protected[/green]"
                    )
                else:
                    console.print("[green]✅ No PII detected in sample data[/green]")

                return protected_data
            else:
                # Fallback: provide table context without actual data
                console.print(
                    "[yellow]💡 Sample data querying requires Databricks SQL "
                    "warehouse setup[/yellow]"
                )
                console.print(f"[yellow]   Table: {full_table_name}[/yellow]")
                console.print(
                    f"[yellow]   Suggested query: SELECT * FROM "
                    f"{full_table_name} LIMIT 5[/yellow]"
                )

                return {
                    "source_table": full_table_name,
                    "suggested_query": f"SELECT * FROM {full_table_name} LIMIT 5",
                    "note": (
                        "To enable sample data fetching, configure "
                        "Databricks SQL warehouse access"
                    ),
                    "pii_protection": {
                        "level": pii_protection_level,
                        "method": "no_data_available",
                    },
                }

        except Exception as e:
            console.print(f"[yellow]Error setting up sample data context: {e}[/yellow]")
            return None

    def _fetch_sample_data_from_databricks(
        self, table_name: str, project_path: Optional[Path] = None
    ) -> Optional[Dict]:
        """Attempt to fetch actual sample data from Databricks."""
        try:
            from .databricks import DatabricksQueryError, create_databricks_connector

            # Create Databricks connector for the project (if project_path is available)
            if not project_path:
                console.print(
                    "[yellow]⚠️  No project path provided for "
                    "Databricks connection[/yellow]"
                )
                return None

            connector = create_databricks_connector(project_path)
            if not connector:
                console.print("[yellow]⚠️  Databricks connector not available[/yellow]")
                return None

            # Try to fetch table info and sample data
            console.print(f"[blue]🔍 Querying Databricks table: {table_name}[/blue]")

            # Build the query - handle different table name formats
            if "." not in table_name:
                # Add default catalog/schema if not specified
                query = f"SELECT * FROM {table_name} LIMIT 5"
            else:
                # Use table name as-is
                query = f"SELECT * FROM {table_name} LIMIT 5"

            # Execute the query
            result = connector.execute_query(query, max_rows=5)

            if result and result.get("rows"):
                # Transform to expected format for PII protection
                sample_data = {
                    "source_table": table_name,
                    "columns": result.get("columns", []),
                    "sample_rows": result.get("rows", []),
                    "query_executed": result.get("query", query),
                }

                console.print(
                    f"[green]✅ Successfully fetched "
                    f"{len(sample_data['sample_rows'])} rows from "
                    f"{table_name}[/green]"
                )
                return sample_data
            else:
                tbl_name = table_name
                console.print(f"[yellow]⚠️  No data returned from {tbl_name}[/yellow]")
                return None

        except DatabricksQueryError as e:
            console.print(f"[red]❌ Databricks query error: {e}[/red]")
            return None
        except Exception as e:
            console.print(f"[yellow]⚠️  Error fetching sample data: {e}[/yellow]")
            return None

    def _extract_source_from_sql(self, sql_content: str) -> Optional[Tuple[str, str]]:
        """Extract source schema and table name from SQL content."""

        # Sanitize SQL content to remove null bytes that could cause compilation errors
        if sql_content and "\x00" in sql_content:
            logger.warning("Removing null bytes from SQL content")
            sql_content = sql_content.replace("\x00", "")

        # 1. Look for dbt source() function calls
        source_pattern = (
            r"source\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)"
        )
        match = re.search(source_pattern, sql_content, re.IGNORECASE)
        if match:
            return match.group(1), match.group(2)

        # 2. Look for dbt ref() function calls to staging models (common pattern)
        ref_pattern = r"ref\s*\(\s*['\"]([^'\"]+)['\"]\s*\)"
        ref_matches = re.findall(ref_pattern, sql_content, re.IGNORECASE)
        if ref_matches:
            # Prefer staging models as they often reference sources
            for ref_model in ref_matches:
                if "stg_" in ref_model or "staging" in ref_model:
                    # Extract likely source info from staging model name
                    # Pattern: stg_source_table -> (source, table)
                    if ref_model.startswith("stg_"):
                        parts = ref_model[4:].split("_", 1)  # Remove 'stg_' prefix
                        if len(parts) >= 2:
                            return parts[0], parts[1]
                        elif len(parts) == 1:
                            return "staging", parts[0]

            # If no staging models, use the first ref as a fallback
            first_ref = ref_matches[0]
            return "models", first_ref

        # 3. Look for direct table references with schema
        table_pattern = r"FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)"
        match = re.search(table_pattern, sql_content, re.IGNORECASE)
        if match:
            return match.group(1), match.group(2)

        # 4. Look for catalog.schema.table patterns (Databricks/modern data warehouse)
        full_table_pattern = (
            r"FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)"
            r"\.([a-zA-Z_][a-zA-Z0-9_]*)"
        )
        match = re.search(full_table_pattern, sql_content, re.IGNORECASE)
        if match:
            return match.group(2), match.group(3)  # Return schema.table

        # 5. Handle special cases - look for table names in run_query() calls
        run_query_pattern = r"run_query\s*\(['\"].*?FROM\s+.*?\.(\w+)['\"]\)"
        match = re.search(run_query_pattern, sql_content, re.IGNORECASE | re.DOTALL)
        if match:
            table_name = match.group(1)
            return "dynamic_query", table_name

        # 6. Look for SHOW commands (Databricks specific)
        if "SHOW GROUPS" in sql_content.upper() or "SHOW TABLES" in sql_content.upper():
            return "databricks_metadata", "system_commands"

        return None

    def _build_model_table_name(
        self, model_name: str, project_path: Optional[Path] = None
    ) -> Optional[str]:
        """Build the full table name for a model using catalog and schema
        from profiles.yml.
        """
        try:
            if not project_path:
                logger.debug(f"No project path provided for model {model_name}")
                return None

            # Sanitize model name to prevent null byte issues
            if model_name and "\x00" in model_name:
                logger.warning(f"Removing null bytes from model name: {model_name}")
                model_name = model_name.replace("\x00", "")

            if not model_name or not model_name.strip():
                logger.warning("Empty or invalid model name after sanitization")
                return None

            from .databricks import create_databricks_connector

            # Create connector to get catalog and schema info
            connector = create_databricks_connector(project_path)
            if not connector or not connector.config:
                logger.debug(
                    f"No Databricks connector available for model {model_name}"
                )
                return None

            catalog = connector.config.catalog
            schema = connector.config.schema

            # Sanitize catalog and schema names
            if catalog and "\x00" in catalog:
                logger.warning(f"Removing null bytes from catalog name: {catalog}")
                catalog = catalog.replace("\x00", "")

            if schema and "\x00" in schema:
                logger.warning(f"Removing null bytes from schema name: {schema}")
                schema = schema.replace("\x00", "")

            if not catalog or not schema:
                logger.warning(
                    f"Missing catalog ({catalog}) or schema ({schema}) in "
                    f"profiles.yml for model {model_name}"
                )
                return None

            # Build full table name: catalog.schema.model_name
            full_table_name = f"{catalog}.{schema}.{model_name}"
            logger.debug(f"Built table name: {full_table_name}")

            return full_table_name

        except Exception as e:
            logger.error(f"Error building table name for {model_name}: {e}")
            import traceback

            logger.debug(f"Full traceback: {traceback.format_exc()}")
            return None

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

        # Remove markdown formatting
        cleaned = re.sub(r"\*\*([^*]+)\*\*", r"\1", cleaned)  # Remove bold
        cleaned = re.sub(r"`([^`]+)`", r"\1", cleaned)  # Remove code backticks

        # Clean up extra whitespace and newlines
        cleaned = re.sub(
            r"\n\s*\n\s*\n", "\n\n", cleaned
        )  # Multiple newlines to double
        cleaned = cleaned.strip()

        return cleaned

    def _is_valid_database_column_name(self, column_name: str) -> bool:
        """Check if a column name is a valid database identifier."""
        if not column_name:
            return False

        # Valid database identifiers: start with letter/underscore, contain
        # only alphanumeric + underscore
        # Allow hyphens for some databases, but be strict about format
        return re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", column_name) is not None

    def _clean_column_name(self, column_name: str) -> str:
        """Clean column name to remove formatting artifacts."""
        if not column_name:
            return column_name

        # Remove common formatting patterns
        cleaned = column_name

        # Remove bullet points and list formatting
        cleaned = re.sub(r"^[-*+]\s*", "", cleaned)  # Remove bullet points
        cleaned = re.sub(r"^\d+\.\s*", "", cleaned)  # Remove numbered lists

        # Remove markdown formatting
        cleaned = re.sub(r"\*\*([^*]+)\*\*", r"\1", cleaned)  # Remove bold
        cleaned = re.sub(r"`([^`]+)`", r"\1", cleaned)  # Remove backticks
        cleaned = re.sub(r"_([^_]+)_", r"\1", cleaned)  # Remove italic underscores

        # Remove extra whitespace
        cleaned = cleaned.strip()

        # Validate that it's a reasonable column name
        if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", cleaned):
            return cleaned
        else:
            return ""  # Return empty if not valid
