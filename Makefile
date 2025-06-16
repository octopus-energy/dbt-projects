# Help
.PHONY: help

help:
	@grep -E '^[0-9a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

# Local installation
.PHONY: init clean lock update install

install: ## Initalise the virtual env installing deps
	pipenv install --dev && pipenv run pre-commit install

clean: ## Remove all the unwanted clutter
	find src -type d -name __pycache__ | xargs rm -rf
	find src -type d -name '*.egg-info' | xargs rm -rf
	pipenv clean

lint:

lock: ## Lock dependencies
	pipenv lock

update: ## Update dependencies (whole tree)
	pipenv update --dev

sync: ## Install dependencies as per the lock file
	pipenv sync --dev

# Testing
.PHONY: test

test: