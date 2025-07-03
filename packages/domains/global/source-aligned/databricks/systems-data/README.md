Welcome to the databricks package!

This package contains models relating to Databricks. 

# Installation

This package should be installed into a dbt project using the syntax:
```
packages:
  - git: "https://{{ env_var('DBT_ENV_SECRET_GIT_CREDENTIAL') }}@github.com/octoenergy/shared-dbt-packages"
    subdirectory: "packages/data_platform/databricks/client"
    revision: "{{ env_var('SHARED_DBT_PACKAGES_KRAKEN_DBT_BRANCH_NAME', 'main') }}"
    warn-unpinned: false    
```

# Developing Packages

Please see our [guide on Notion](https://www.notion.so/kraken-tech/Designing-dbt-modules-08c23c0439c84579baaf63a949a2ca2c?pvs=4) for how to develop new dbt packages within the KTL framework. 