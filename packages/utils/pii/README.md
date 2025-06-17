Welcome to utils_pii!

A package containing macros for handling PII data.

# Installation

This package should be installed into a dbt project using the syntax:
```
packages:
  - git: "https://{{ env_var('DBT_ENV_SECRET_GIT_CREDENTIAL') }}@github.com/octoenergy/shared-dbt-packages"
    subdirectory: "packages/utils/pii"
    revision: "{{ env_var('SHARED_DBT_PACKAGES_KRAKEN_DBT_BRANCH_NAME', 'main') }}"
    warn-unpinned: false    
```

Make sure that the environment variable `UTILS_PII_SALT` is set whenever using this package.

# Developing Packages

Please see our [guide on Notion](https://www.notion.so/kraken-tech/Designing-dbt-modules-08c23c0439c84579baaf63a949a2ca2c?pvs=4) for how to develop new dbt packages within the KTL framework. 

# Common Issues

When using the `hash_sensitive_columns` inside a package designed for importing into other projects - *you must use the `package_name` parameter*.

For example

When importing the `kraken_integrity` package into the `oeuk_core_kap` project - within the `kraken_integrity` package the model `stg_integrity_check_failures_all` needs to use the `hash_sensitive_columns` macro like this:
`{{ utils_pii.hash_sensitive_columns(ref('stg_integrity_check_failures_all_pii'), package_name='kraken_integrity') }}`
rather than like this:
`{{ utils_pii.hash_sensitive_columns(ref('stg_integrity_check_failures_all_pii') }}`

This is to prevent the macro looking for the source table that is being hashed within the execution projects namespace in the dbt graph context, where it gets the information on sensitive columns.

* To be safe - always provide the `package_name` parameter as a convention *