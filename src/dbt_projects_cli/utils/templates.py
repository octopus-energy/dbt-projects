"""Template loading utilities for fabric CLI commands."""

from pathlib import Path
from typing import Optional


def get_template_path(template_name: str) -> Path:
    """Get the absolute path to a template file."""
    templates_dir = Path(__file__).parent.parent / "templates"
    return templates_dir / template_name


def load_template(template_name: str) -> str:
    """Load a template file and return its contents as a string."""
    template_path = get_template_path(template_name)
    
    if not template_path.exists():
        raise FileNotFoundError(f"Template '{template_name}' not found at {template_path}")
    
    with open(template_path, 'r', encoding='utf-8') as f:
        return f.read()


def load_template_safe(template_name: str, fallback: Optional[str] = None) -> str:
    """Load a template file safely, returning fallback text if template is missing."""
    try:
        return load_template(template_name)
    except FileNotFoundError:
        return fallback or f"# Template '{template_name}' not found"


# Template name constants for easier refactoring
SCHEMA_EXAMPLE = "schema_example.yml"
MULTI_FABRIC_EXAMPLE = "multi_fabric_example.json" 
PROFILES_EXAMPLE = "profiles_example.yml"
