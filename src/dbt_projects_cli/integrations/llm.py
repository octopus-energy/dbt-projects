"""
LLM integration for generating dbt model and column descriptions.
"""

import os
import re
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

import openai
from anthropic import Anthropic
import google.generativeai as genai
from rich.console import Console

console = Console()


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
    columns: List[Dict[str, str]] = None
    dependencies: List[str] = None
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
    
    def __init__(self, provider: LLMProvider = LLMProvider.OPENAI):
        self.provider = provider
        self._setup_client()
    
    def _setup_client(self) -> None:
        """Setup the LLM client based on the provider."""
        if self.provider == LLMProvider.OPENAI:
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                raise ValueError("OPENAI_API_KEY environment variable not set")
            self.client = openai.OpenAI(api_key=api_key)
            self.model = "gpt-4"
        
        elif self.provider == LLMProvider.ANTHROPIC:
            api_key = os.getenv("ANTHROPIC_API_KEY")
            if not api_key:
                raise ValueError("ANTHROPIC_API_KEY environment variable not set")
            self.client = Anthropic(api_key=api_key)
            self.model = "claude-3-sonnet-20240229"
    
    def generate_descriptions(
        self, 
        model_context: ModelContext,
        expand_existing: bool = True
    ) -> DescriptionResult:
        """Generate descriptions for a dbt model and its columns."""
        
        prompt = self._build_description_prompt(model_context, expand_existing)
        
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
        expand_existing: bool
    ) -> str:
        """Build the prompt for the LLM to generate descriptions."""
        
        prompt_parts = [
            "You are a data analyst helping to document dbt models. ",
            "Generate clear, concise descriptions for the model and its columns based on the SQL logic.\n\n"
        ]
        
        # Model information
        prompt_parts.append(f"**Model Name:** {context.name}\n")
        if context.project_name:
            prompt_parts.append(f"**Project:** {context.project_name}\n")
        if context.schema_name:
            prompt_parts.append(f"**Schema:** {context.schema_name}\n")
        
        # Existing description context
        if context.existing_description and expand_existing:
            prompt_parts.append(f"**Existing Description:** {context.existing_description}\n")
            prompt_parts.append("Please expand and improve the existing description.\n\n")
        elif context.existing_description:
            prompt_parts.append(f"**Current Description:** {context.existing_description}\n")
            prompt_parts.append("Use this as context but provide a fresh, comprehensive description.\n\n")
        
        # SQL content
        prompt_parts.append("**SQL Content:**\n```sql\n")
        prompt_parts.append(context.sql_content)
        prompt_parts.append("\n```\n\n")
        
        # Dependencies
        if context.dependencies:
            prompt_parts.append("**Dependencies:** " + ", ".join(context.dependencies) + "\n\n")
        
        # Column information
        if context.columns:
            prompt_parts.append("**Known Columns:**\n")
            for col in context.columns:
                col_name = col.get('name', '')
                col_type = col.get('type', '')
                col_desc = col.get('description', '')
                prompt_parts.append(f"- {col_name} ({col_type})")
                if col_desc:
                    prompt_parts.append(f": {col_desc}")
                prompt_parts.append("\n")
            prompt_parts.append("\n")
        
        # Instructions
        prompt_parts.extend([
            "**Instructions:**\n",
            "1. Provide a clear, business-focused description of what this model represents\n",
            "2. Explain the model's purpose and key business logic\n",
            "3. For each column, provide a concise description focusing on business meaning\n",
            "4. Use professional, documentation-style language\n",
            "5. Avoid overly technical jargon unless necessary\n\n",
            "**Response Format:**\n",
            "MODEL_DESCRIPTION: [Your model description here]\n\n",
            "COLUMN_DESCRIPTIONS:\n",
            "column_name: Description of the column\n",
            "another_column: Description of another column\n"
        ])
        
        return "".join(prompt_parts)
    
    def _call_openai(self, prompt: str) -> str:
        """Call OpenAI API."""
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system", 
                    "content": "You are a helpful data analyst specializing in documenting data models."
                },
                {"role": "user", "content": prompt}
            ],
            max_tokens=1500,
            temperature=0.3
        )
        return response.choices[0].message.content
    
    def _call_anthropic(self, prompt: str) -> str:
        """Call Anthropic API."""
        response = self.client.messages.create(
            model=self.model,
            max_tokens=1500,
            temperature=0.3,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        return response.content[0].text
    
    def _parse_response(self, response: str) -> DescriptionResult:
        """Parse the LLM response to extract model and column descriptions."""
        
        # Extract model description
        model_desc_match = re.search(
            r"MODEL_DESCRIPTION:\s*(.*?)(?=\n\nCOLUMN_DESCRIPTIONS:|\n\n[A-Z_]+:|$)", 
            response, 
            re.DOTALL
        )
        model_description = model_desc_match.group(1).strip() if model_desc_match else ""
        
        # Extract column descriptions
        column_descriptions = {}
        column_section_match = re.search(
            r"COLUMN_DESCRIPTIONS:\s*(.*?)$", 
            response, 
            re.DOTALL
        )
        
        if column_section_match:
            column_text = column_section_match.group(1).strip()
            # Parse column descriptions (format: "column_name: description")
            for line in column_text.split('\n'):
                line = line.strip()
                if ':' in line and not line.startswith('#'):
                    column_name, description = line.split(':', 1)
                    column_descriptions[column_name.strip()] = description.strip()
        
        return DescriptionResult(
            model_description=model_description,
            column_descriptions=column_descriptions,
            confidence_score=0.8  # Default confidence score
        )
