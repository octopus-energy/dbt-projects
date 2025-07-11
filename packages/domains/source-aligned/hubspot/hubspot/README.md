# hubspot_hubspot

Package for transforming Hubspot source data.

## Overview

This is a **source-aligned** data product that extracts and models data from the hubspot system.

### Data Mesh Principles

- **Domain Ownership**: This package is owned by the team responsible for the
  hubspot system
- **Data as a Product**: Provides clean, documented, and reliable data products
  from hubspot
- **Self-Serve Infrastructure**: Can be independently deployed and maintained
- **Federated Governance**: Follows Octopus Energy's data standards and
  governance

## Getting Started

1. Install dependencies:
   ```bash
   dbt deps
   ```

2. Run the models:
   ```bash
   dbt run
   ```

3. Test the models:
   ```bash
   dbt test
   ```

## Structure

- `models/staging/` - Staging models (views, private access)
- `models/marts/` - Mart models (tables, public access) *(consumer-aligned only)*
- `macros/` - Reusable SQL macros
- `tests/` - Custom data tests
- `groups/` - Domain ownership configuration
- `seeds/` - Static reference data
- `snapshots/` - Slowly changing dimensions
- `analyses/` - Ad-hoc analytical queries

## Data Contracts

### Public Models

This domain exposes the following public data products:

*(Document your public models and their contracts here)*

### Dependencies

This domain depends on:

*(Document your upstream dependencies here)*

## Ownership

- **Domain**: Source Aligned
- **Owner**: Data Team
- **Contact**: data@octopusenergy.com

## Development

### Local Development

1. Set up your profiles.yml (see `profiles.yml` template)
2. Install dependencies: `dbt deps`
3. Run in development: `dbt run --target dev`
4. Test your changes: `dbt test --target dev`

### Deployment

This package is deployed as part of the Octopus Energy data mesh infrastructure.
