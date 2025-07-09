import os
from unittest.mock import MagicMock, patch

import pytest

from dbt_projects_cli.integrations.llm import (
    LLMDescriptionGenerator,
    LLMProvider,
    ModelContext,
)


class TestLLMIntegration:
    """Integration tests for LLM description generation."""

    @patch("openai.OpenAI")
    def test_llm_generator_initialization_openai(self, mock_openai):
        """Test LLM generator initialization with OpenAI provider."""
        with patch.dict(os.environ, {"OPENAI_API_KEY": "fake-key"}):
            generator = LLMDescriptionGenerator(provider=LLMProvider.OPENAI)
            assert generator.provider == LLMProvider.OPENAI
            assert generator.model == "gpt-4o"
            mock_openai.assert_called_once()

    def test_llm_generator_missing_api_key(self):
        """Test LLM generator initialization with missing API key."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(
                ValueError, match="OPENAI_API_KEY environment variable not set"
            ):
                LLMDescriptionGenerator(provider=LLMProvider.OPENAI)

    @patch("openai.OpenAI")
    def test_generate_description_with_pii(self, mock_openai):
        """Test generating descriptions with PII protection using OpenAI."""
        with patch.dict(os.environ, {"OPENAI_API_KEY": "fake-key"}):
            # Mock OpenAI client and response
            mock_client = MagicMock()
            mock_openai.return_value = mock_client
            mock_response = MagicMock()
            mock_response.choices = [MagicMock()]
            mock_response.choices[0].message.content = (
                "MODEL_DESCRIPTION: User data table\n\n"
                "COLUMN_DESCRIPTIONS:\nid: User identifier\nname: User name"
            )
            mock_client.chat.completions.create.return_value = mock_response

            generator = LLMDescriptionGenerator(provider=LLMProvider.OPENAI)

            # Mock the isinstance check to return True for OpenAI
            with patch(
                "dbt_projects_cli.integrations.llm.isinstance", return_value=True
            ):
                context = ModelContext(
                    name="sample_model",
                    sql_content="SELECT * FROM sensitive_data",
                    project_name="test_project",
                    schema_name="default",
                )

                # Mock the sample data method to avoid actual DB calls
                with patch.object(
                    generator, "_get_databricks_sample_data", return_value=None
                ):
                    result = generator.generate_descriptions(
                        context, expand_existing=True
                    )

            assert "User data table" in result.model_description
            assert "User identifier" in result.column_descriptions.get("id", "")

    @patch("openai.OpenAI")
    def test_prompt_building_with_sample_data(self, mock_openai):
        """Test building prompts with sample data integration."""
        with patch.dict(os.environ, {"OPENAI_API_KEY": "fake-key"}):
            generator = LLMDescriptionGenerator(provider=LLMProvider.OPENAI)

            context = ModelContext(
                name="users_table",
                sql_content="SELECT id, name FROM users",
                project_name="analytics",
                schema_name="staging",
            )

            # Mock sample data with PII protection
            mock_sample_data = {
                "source_table": "analytics.staging.users",
                "sample_rows": [
                    {"id": "123", "name": "[PII_PLACEHOLDER:abc123]"},
                    {"id": "456", "name": "[PII_PLACEHOLDER:def456]"},
                ],
                "pii_protection": {
                    "protection_applied": True,
                    "method": "hash",
                    "high_risk_columns": 1,
                },
            }

            with patch.object(
                generator, "_get_databricks_sample_data", return_value=mock_sample_data
            ):
                prompt = generator._build_description_prompt(
                    context, expand_existing=True
                )

            # Check that PII protection context is included
            assert "PII_PLACEHOLDER" in prompt
            assert "temporary placeholders" in prompt
            assert "NOT actual values" in prompt
            assert "describe the column's actual business purpose" in prompt

    @patch("openai.OpenAI")
    def test_error_handling_in_llm_call(self, mock_openai):
        """Test error handling when LLM API call fails."""
        with patch.dict(os.environ, {"OPENAI_API_KEY": "fake-key"}):
            # Mock OpenAI client to raise an exception
            mock_client = MagicMock()
            mock_openai.return_value = mock_client
            mock_client.chat.completions.create.side_effect = Exception("API Error")

            generator = LLMDescriptionGenerator(provider=LLMProvider.OPENAI)

            # Mock the isinstance check to return True for OpenAI
            with patch(
                "dbt_projects_cli.integrations.llm.isinstance", return_value=True
            ):
                context = ModelContext(
                    name="test_model",
                    sql_content="SELECT * FROM test_table",
                )

                with patch.object(
                    generator, "_get_databricks_sample_data", return_value=None
                ):
                    with pytest.raises(Exception, match="API Error"):
                        generator.generate_descriptions(context, expand_existing=True)

    @patch("dbt_projects_cli.integrations.llm.Anthropic")
    def test_anthropic_provider_initialization(self, mock_anthropic):
        """Test LLM generator initialization with Anthropic provider."""
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "fake-key"}):
            generator = LLMDescriptionGenerator(provider=LLMProvider.ANTHROPIC)
            assert generator.provider == LLMProvider.ANTHROPIC
            assert generator.model == "claude-3-5-sonnet-20241022"
            mock_anthropic.assert_called_once()

    @patch("openai.OpenAI")
    def test_custom_model_override(self, mock_openai):
        """Test using custom model override."""
        with patch.dict(os.environ, {"OPENAI_API_KEY": "fake-key"}):
            custom_model = "gpt-4o-mini"
            generator = LLMDescriptionGenerator(
                provider=LLMProvider.OPENAI, model=custom_model
            )
            assert generator.model == custom_model

    @patch("openai.OpenAI")
    def test_verbose_logging(self, mock_openai):
        """Test verbose logging during description generation."""
        with patch.dict(os.environ, {"OPENAI_API_KEY": "fake-key"}):
            # Mock OpenAI client and response
            mock_client = MagicMock()
            mock_openai.return_value = mock_client
            mock_response = MagicMock()
            mock_response.choices = [MagicMock()]
            mock_response.choices[0].message.content = (
                "MODEL_DESCRIPTION: Test model\n\n"
                "COLUMN_DESCRIPTIONS:\ncol1: Test column"
            )
            mock_client.chat.completions.create.return_value = mock_response

            generator = LLMDescriptionGenerator(provider=LLMProvider.OPENAI)

            # Mock the isinstance check to return True for OpenAI
            with patch(
                "dbt_projects_cli.integrations.llm.isinstance", return_value=True
            ):
                context = ModelContext(
                    name="test_model",
                    sql_content="SELECT * FROM test_table",
                )

                with patch.object(
                    generator, "_get_databricks_sample_data", return_value=None
                ):
                    with patch.object(generator, "_log_llm_prompt") as mock_log:
                        generator.generate_descriptions(
                            context, expand_existing=True, verbose=True
                        )
                        mock_log.assert_called_once()
