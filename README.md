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
- **🔒 Enterprise PII protection** with configurable security levels
- **🎛️ Configurable models** via config files and environment variables
- **📊 Sample data integration** with Databricks CLI support

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

# Use specific LLM model
dbt-cli descriptions generate --project jaffle_shop --llm-model gpt-4o-mini

# Configure PII protection level
dbt-cli descriptions generate --project jaffle_shop --pii-protection high

# Test Databricks connection
dbt-cli descriptions test-databricks

# Test with specific dbt profile
dbt-cli descriptions test-databricks --profile my_profile --target dev

# Generate descriptions with Databricks sample data
dbt-cli descriptions generate --project my_project --dbt-profile my_profile
```

### 🔒 PII Protection

```bash
# Test PII protection capabilities
dbt-cli descriptions test-pii --level high

# Use different protection levels
dbt-cli descriptions generate --project my_project --pii-protection strict    # Maximum security
dbt-cli descriptions generate --project my_project --pii-protection high      # Default - hash PII
dbt-cli descriptions generate --project my_project --pii-protection medium    # Mask PII patterns
dbt-cli descriptions generate --project my_project --pii-protection schema_only  # Column names only
```

### 🎛️ Model Configuration

```bash
# View current LLM model configuration
dbt-cli descriptions show-models

# Use environment variables to set models
export OPENAI_MODEL=gpt-4o-mini
export ANTHROPIC_MODEL=claude-3-5-haiku-20241022
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

### Databricks Integration

The CLI can fetch real sample data from Databricks to improve LLM-generated descriptions.

#### Option 1: dbt profiles.yml (Recommended)

The CLI automatically discovers and uses dbt profiles.yml files with this priority:
1. **Project-specific**: `your-dbt-project/profiles.yml`
2. **Global**: `~/.dbt/profiles.yml`

**Example profiles.yml with OAuth:**
```yaml
my_profile:
  target: dev
  outputs:
    dev:
      type: databricks
      host: your-workspace.cloud.databricks.com
      http_path: /sql/1.0/warehouses/your-warehouse-id
      auth_type: oauth
      client_id: '{{ env_var("DATABRICKS_M2M_CLIENT_ID") }}'
      client_secret: '{{ env_var("DATABRICKS_M2M_CLIENT_SECRET") }}'
      catalog: your_catalog
      schema: your_schema
```

**Example profiles.yml with Token:**
```yaml
my_profile:
  target: dev
  outputs:
    dev:
      type: databricks
      host: your-workspace.cloud.databricks.com
      http_path: /sql/1.0/warehouses/your-warehouse-id
      auth_type: token
      token: your-databricks-token
      catalog: your_catalog
      schema: your_schema
```

#### Option 2: Environment Variables (Fallback)

```bash
export DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
export DATABRICKS_ACCESS_TOKEN=your-access-token
export DATABRICKS_CATALOG=your_catalog  # Optional
export DATABRICKS_SCHEMA=your_schema    # Optional
```

#### Testing the Connection

```bash
# Test with auto-discovered profiles
dbt-cli descriptions test-databricks

# Test specific profile and target
dbt-cli descriptions test-databricks --profile my_profile --target dev
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

## 🔒 PII Protection

The dbt-cli includes enterprise-grade PII protection to ensure sensitive data is never sent to LLM providers.

### Protection Levels

| Level | Method | Sample Rows | Use Case |
|-------|--------|-------------|----------|
| `strict` | Excludes all sensitive data | 2 rows, PII removed | Banking, healthcare, regulated environments |
| `high` | Hashes high-risk PII | 3 rows, emails/names hashed | Standard enterprise (default) |
| `medium` | Masks PII preserving structure | 3 rows, PII masked | Data analysis, debugging |
| `schema_only` | Column names only | No data | Maximum security environments |

### PII Detection

**Column-Based Detection:**
- **High Risk**: `email`, `first_name`, `last_name`, `phone`, `ssn`
- **Medium Risk**: `address`, `city`, `zip`, `customer_id`, `user_id`
- **Low Risk**: `created_date`, `amount`, `status`

**Content-Based Detection:**
- Email addresses: `user@domain.com`
- Phone numbers: `+1-555-123-4567`, `(555) 123-4567`
- SSN: `123-45-6789`
- Credit cards: `4111-1111-1111-1111`
- IP addresses: `192.168.1.1`

### Protection Examples

**Original Data:**
```json
{
  "email": "john.doe@example.com",
  "phone": "+1-555-123-4567",
  "balance": 1250.75
}
```

**High Protection (Hash):**
```json
{
  "email": "[HASH:a8cfcd74]",
  "phone": "[HASH:4793ec20]",
  "balance": 1250.75
}
```

**Medium Protection (Mask):**
```json
{
  "email": "j*******@e******.com",
  "phone": "*************67",
  "balance": 1250.75
}
```

### Security Guarantees

✅ **No raw PII sent to LLMs**  
✅ **Consistent hashing for data integrity**  
✅ **Comprehensive audit logging**  
✅ **Enterprise compliance ready** (GDPR, HIPAA, PCI-DSS)  
✅ **Configurable risk tolerance**  

## 🎛️ Model Configuration

Easily manage and update LLM models without code changes.

### Configuration Priority

1. **Command line**: `--llm-model model-name`
2. **Environment variable**: `OPENAI_MODEL` / `ANTHROPIC_MODEL`
3. **Config file**: `src/dbt_projects_cli/config/models.yaml`
4. **Hardcoded fallback**: Built-in defaults

### Default Models

- **OpenAI**: `gpt-4o` (latest GPT-4 Omni)
- **Anthropic**: `claude-3-5-sonnet-20241022` (latest Claude 3.5 Sonnet)

### Cost Optimization

| Use Case | Recommended Model | Reason |
|----------|------------------|--------|
| Bulk processing | `gpt-4o-mini` | Cost-effective, good quality |
| High-quality docs | `claude-3-5-sonnet-*` | Best balance of quality/cost |
| Quick testing | `gpt-3.5-turbo` | Fastest, cheapest |
| Maximum quality | `claude-3-opus-*` | Most capable (expensive) |

### Updating Models

To use the latest models, simply update `src/dbt_projects_cli/config/models.yaml`:

```yaml
providers:
  openai:
    default_model: "gpt-4o"  # Update this
    available_models:
      - "gpt-4o"
      - "gpt-4o-mini"
      # Add new models here
```

## 📄 License

Internal tool for Octopus Energy - see your internal guidelines for usage.
