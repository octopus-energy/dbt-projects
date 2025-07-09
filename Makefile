# Help
.PHONY: help

help:
	@grep -E '^[0-9a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

# Local installation
.PHONY: init clean lock update install

install: ## Initalise the virtual env installing deps
	pipenv install --dev && pipenv run pre-commit install && pipenv run pip install -e .

clean: ## Remove all the unwanted clutter
	find src -type d -name __pycache__ | xargs rm -rf
	find src -type d -name '*.egg-info' | xargs rm -rf
	pipenv clean

lint: ## Run linting tools (black, isort, flake8, mypy)
	pipenv run black --check --diff src tests
	pipenv run isort --check-only --diff src tests
	pipenv run flake8 src tests
	pipenv run mypy src

format: ## Auto-fix formatting issues (black, isort)
	pipenv run black src tests
	pipenv run isort src tests

lock: ## Lock dependencies
	pipenv lock

update: ## Update dependencies (whole tree)
	pipenv update --dev

sync: ## Install dependencies as per the lock file
	pipenv sync --dev

# Testing
.PHONY: test test-unit test-integration test-migration test-template test-coverage test-fast

test: test-unit test-integration ## Run all tests

test-unit: ## Run unit tests only
	pipenv run pytest tests/unit -v --tb=short

test-integration: ## Run integration tests only
	pipenv run pytest tests/integration -v --tb=short

test-coverage: ## Run tests with coverage report
	pipenv run pytest --cov=src/dbt_projects_cli --cov-report=html --cov-report=term-missing

test-fast: ## Run tests in parallel
	pipenv run pytest -n auto

test-cli: ## Test CLI installation and basic commands
	pipenv run dbt-cli --help
	pipenv run dbt-cli scaffold --help
	pipenv run dbt-cli migrate --help
