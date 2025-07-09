# octoenergy-data-prod - core

Core data transformations

## Generated Project

This dbt project was automatically generated from a lightweight fabric configuration.

### Configuration Summary

- **Fabric Name**: octoenergy-data-prod
- **Project Name**: core
- **Version**: 1.0.0
- **dbt Version**: 1.7.0
- **Databricks Host**: octoenergy-oeuk.cloud.databricks.com
- **Project Schema**: default

### Packages Included

- Hub: elementary-data/elementary @ >=0.16.4

### Usage

1. Install dependencies: `dbt deps`
2. Test connection: `dbt debug`
3. Run models: `dbt run` (if any models are included in packages)

### Notes

This is a temporary deployment structure. The original configuration is managed
separately and this project should not be modified directly.

Catalog naming follows the convention:
- Dev: octoenergy-data-prod_data_prod_test
- Prod: octoenergy-data-prod_data_prod_prod
- Source: octoenergy-data-prod_data_prod_source
