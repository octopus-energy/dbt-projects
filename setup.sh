#!/bin/bash

echo "🐙 Setting up dbt-projects-cli..."

# Check if pipenv is available
if ! command -v pipenv &> /dev/null; then
    echo "❌ pipenv is required but not installed. Please install pipenv first:"
    echo "   pip install pipenv"
    exit 1
fi

# Install dependencies and CLI tool
echo "📦 Installing dependencies..."
pipenv install --dev

echo "🔧 Installing CLI tool in development mode..."
pipenv run pip install -e .

echo "✅ Setup complete!"
echo ""
echo "To use the CLI tool:"
echo "  pipenv shell           # Activate the virtual environment"
echo "  dbt-cli --help         # Show CLI help"
echo "  dbt-cli info           # Show project information"
echo ""
echo "Example usage:"
echo "  dbt-cli descriptions generate --project jaffle_shop --dry-run"
echo ""
echo "Make sure to set your LLM API keys:"
echo "  export OPENAI_API_KEY=your_key_here"
echo "  export ANTHROPIC_API_KEY=your_key_here"
