# Global DBT Package Guidance

This directory is for dbt projects and packages that provide **standardised models** for use across all territories.

## Key Principles

- **Standardised Models:**  
    Models should be designed to work identically in any territory, enabling easy import and use.

- **Standardised Sources:**  
    Where possible, use common, standardised sources for your models.

- **Source Contracts:**  
    If standardised sources are not feasible, define clear contracts (schemas, column names, data types, etc.) that territory-specific source packages must comply with.

- **Output Contracts:**  
    All packages should output data in a standardised format, defined by a contract. This ensures that a central project can import and aggregate data from all territories (e.g., via `union` or `aggregate`) **without additional transformation**.

## Implementation Guidance

- Document all contracts clearly in your package.
- Validate compliance with contracts using dbt tests.
- Avoid territory-specific logic in global models; push localisation to source packages.

## Example Workflow

1. **Define a contract** for a model (e.g., `customer` table schema).
2. **Territory packages** implement sources to match the contract.
3. **Global models** consume these sources, ensuring consistent outputs.
4. **Central project** unions or aggregates data from all territories seamlessly.

For questions or proposals for new contracts, please raise an issue or start a discussion in this repository.