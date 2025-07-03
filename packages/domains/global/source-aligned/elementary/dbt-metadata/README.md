Welcome to data_platform_elementary!

A package serving as a staging layer between base elementary package tables and our unioned version of them by facilitating the sharing of those tables via Delta Shares. 

Any information sought on the tables being shared should be looked for in the base elementary package instead of here. 

# Installation

This package should be installed into a dbt project using the syntax:
```
packages:
  - git: "https://{{ env_var('DBT_ENV_SECRET_GIT_CREDENTIAL') }}@github.com/octoenergy/shared-dbt-packages"
    subdirectory: "packages/data_platform/elementary/client"
    revision: '{{ env_var('SHARED_DBT_PACKAGES_KRAKEN_DBT_BRANCH_NAME', 'main') }}'
    warn-unpinned: false    
```

# Developing Packages

Please see our [guide on Notion](https://www.notion.so/kraken-tech/Designing-dbt-modules-08c23c0439c84579baaf63a949a2ca2c?pvs=4) for how to develop new dbt packages within the KTL framework. 