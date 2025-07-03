# Territories Domain README

This folder contains dbt projects that are **territory-specific**. Each project should focus on normalising local data sources to conform to a **contract specified by the global package**.

## Guidance

- **Purpose:**  
    Projects here are responsible for transforming and standardising raw, territory-specific data (e.g., meterpoints, customers) to match the schemas and conventions defined in the global package.

- **Scope:**  
    - Only include models and logic that are unique to the territory.
    - Do **not** include cross-territory analysis or business logic here; that should be handled in global or downstream packages.

- **Example:**  
    - Raw meterpoint data may differ by territory (e.g., UK vs. US).
    - Each territory project should normalise its meterpoint data to the global contract.
    - Analytical models using meterpoint data should be built on the global contract, not on territory-specific raw data.

## Best Practices

- Always reference the global package contract when designing models.
- Keep territory-specific logic isolated to this folder.
- Document any deviations or extensions to the global contract.

For further details, refer to the global package documentation.