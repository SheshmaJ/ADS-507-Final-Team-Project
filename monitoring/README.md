
# Monitoring
This folder do monitoring checks for the FDA pipiline.

Monitoring is executed automatically in Github Actions after the pipeline.
A report is generatd at runtime and uploaded as an artifact.

Monitoring focuses on pipeline health and data quality. Business analytics are handled separately through SQL analysis queries and the Streamlit dashboard.

Purpose of monitoring is to analyse:
Did the pipeline run?
Did tables load?
Did the join work?
Is data missing or broken?
