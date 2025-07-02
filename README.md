# dbt-projects

A comprehensive CLI tool for managing dbt projects at Octopus Energy, featuring LLM-powered description generation, project discovery, and model analysis.

## 🚀 Quick Start

```bash
# Clone and setup
git clone <repository-url>
cd dbt-projects
./setup.sh

# Activate environment
pipenv shell

# Explore your projects
dbt-cli info
dbt-cli projects list

# Generate descriptions with AI
export OPENAI_API_KEY=your_key_here
dbt-cli descriptions generate --project jaffle_shop --dry-run
```

## 🛠️ Features

### 📝 LLM-Powered Description Generation

Automatically generate or enhance descriptions for your dbt models and columns using OpenAI GPT-4 or Anthropic Claude.

**Key Features:**
- Analyzes SQL logic to understand model purpose
- Generates business-focused descriptions
- Preserves existing documentation while expanding it
- Supports both OpenAI and Anthropic providers
- Interactive mode for review before applying changes
- Dry-run mode to preview changes

### 🔍 Project Discovery

Automatically discovers and analyzes all dbt projects in your repository:
- Packages (reusable components)
- Fabrics (production projects)
- Model, macro, and test counting
- Configuration validation

### 📊 Model Analysis

Analyze individual models or entire projects:
- SQL complexity analysis
- dbt-specific syntax detection
- Dependency mapping
- Documentation coverage

## 📋 Commands

### Core Commands

```bash
# Show repository overview
dbt-cli info

# Get help
dbt-cli --help
dbt-cli descriptions --help
```

### Project Management

```bash
# List all projects
dbt-cli projects list

# Get project details
dbt-cli projects info <project-name>

# Validate projects
dbt-cli utils validate

# Clean generated files
dbt-cli utils clean
```

### Model Operations

```bash
# List models
dbt-cli models list                    # All models
dbt-cli models list --project jaffle_shop  # Project-specific

# Analyze a model
dbt-cli models analyze path/to/model.sql
```

### Description Generation

```bash
# Generate descriptions for all models in a project
dbt-cli descriptions generate --project jaffle_shop

# Process a specific model
dbt-cli descriptions generate --project jaffle_shop --model customers

# Expand existing descriptions
dbt-cli descriptions generate --project jaffle_shop --expand

# Use different LLM provider
dbt-cli descriptions generate --project jaffle_shop --provider anthropic

# Preview without making changes
dbt-cli descriptions generate --project jaffle_shop --dry-run

# Interactive mode (confirm each change)
dbt-cli descriptions generate --project jaffle_shop --interactive
```

### Data Mesh Domain Scaffolding

```bash
# Show data mesh patterns and guidance
dbt-cli scaffold info

# Create a source-aligned domain package
dbt-cli scaffold domain --alignment source-aligned --source-system stripe --domain-name payment-data

# Create a consumer-aligned domain package
dbt-cli scaffold domain --alignment consumer-aligned --business-area marketing --domain-name customer-analytics

# Create a utility package
dbt-cli scaffold domain --alignment utils --domain-name testing

# Interactive mode (prompts for all inputs)
dbt-cli scaffold domain
```

## ⚙️ Configuration

### LLM API Keys

Set your API keys as environment variables:

```bash
# For OpenAI (default)
export OPENAI_API_KEY=your_openai_key_here

# For Anthropic
export ANTHROPIC_API_KEY=your_anthropic_key_here
```

### Repository Structure

The CLI automatically discovers projects in:
- `packages/` - Reusable dbt packages
- `fabrics/` - Production dbt projects

## 🔧 Development

### Setup for Development

```bash
# Install in development mode
pipenv install --dev
pipenv run pip install -e .

# Run tests
pipenv run pytest

# Code formatting
pipenv run black src/
pipenv run isort src/
```

### Adding New Commands

1. Create a new command module in `src/dbt_projects_cli/commands/`
2. Add the command group to `main.py`
3. Follow the existing patterns for rich console output

### Architecture

```
src/dbt_projects_cli/
├── main.py                 # CLI entry point
├── commands/              # Command modules
│   ├── descriptions.py    # LLM description generation
│   ├── projects.py        # Project management
│   ├── models.py          # Model operations
│   └── utils.py           # Utility commands
├── core/                  # Core functionality
│   ├── project_discovery.py  # Project discovery
│   └── model_parser.py       # YAML parsing
└── integrations/          # External service integrations
    └── llm.py            # LLM providers
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Run the linter and tests
6. Submit a pull request

## 📄 License

Internal tool for Octopus Energy - see your internal guidelines for usage.
