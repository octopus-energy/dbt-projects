# Central DBT Projects

This directory contains dbt projects designed for **central analysis**. Projects here should focus on:

- **Aggregating** data from multiple localized dbt projects.
- **Unioning** datasets to create unified, organization-wide views.
- Providing models, marts, or reporting layers that combine data from various domains.

> **Note:**  
> Do not create localized or domain-specific models here. Instead, reference and combine outputs from localized dbt projects to enable centralized analytics.

## Best Practices

- Ensure naming conventions clearly indicate central/aggregated purpose.
- Document any dependencies on localized projects.

For questions or onboarding, contact @analytics-platform on Slack.