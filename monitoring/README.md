
# Monitoring
---
### Monitoring is used because the pipeline pulls fresh data from FDA website on every run. Automated checks confirm that the pipeline ran, tables loaded, joins worked, and no critical data quality issues were introduced.
---
Monitoring is executed automatically in Github Actions after the pipeline.
A report is generatd at runtime and uploaded as an artifact.

- **data_quality_checks.sql**

This file validates the joined dataset shortages_with_ndc, which is created by joining FDA drug shortage records with NDC product data. It checks how many shortage records successfully matched to an NDC (product_ndc) and how many did not.

It also identifies missing key fields such as package_ndc, company_name, and status, checks for duplicate shortage_id values, validates date formats, and summarizes shortages by status.

- **pipeline_health.sql**

This file verifies that the ETL pipeline completed successfully end to end. It confirms that all required raw tables and the joined table shortages_with_ndc exist and contain data.

It also checks that analytical views built from the joined data such as manufacturer and package-level views were created correctly, ensuring SQL transformations ran as expected.

raw_ndc rows > 0

raw_drug_shortages rows > 0

shortages_with_ndc exists

- **schema_snapshot.sql**

This file captures a snapshot of the database schema after the pipeline runs. It lists all tables, columns, and views in the database.

It also records the SHOW CREATE definitions for key raw tables and views derived from shortages_with_ndc.

- **run_monitoring.py**

This Python script orchestrates the entire monitoring process. It connects to MySQL, runs all monitoring SQL files (schema_snapshot.sql, pipeline_health.sql, data_quality_checks.sql, and analysis queries), and collects their outputs.

The script generates a Markdown and text report in monitoring/reports/ summarizing table loads, join success between drug shortages and NDC data, and key analytical results. If any check fails, the issue is recorded in the report for review.
