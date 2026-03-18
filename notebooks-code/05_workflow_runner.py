# Databricks notebook source
# MAGIC %md
# MAGIC ## Claims Analytics Workflow runner

# COMMAND ----------

# Clear widgets (safe for interactive use; harmless in jobs)
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("reference_workspace_path", "/Workspace/Users/yikumshiferaw@gmail.com/healthcare-claims-analytics-platform/notebooks/")
notebook_path = dbutils.widgets.get("reference_workspace_path")
notebook_path

# COMMAND ----------

# This should only run once to create  Catalog and Schema Setup
dbutils.notebook.run(f"{notebook_path}01_catalog_schema_setup", 600)
# Run the healthcare claims bronze layer notebook
dbutils.notebook.run(f"{notebook_path}02_healthcare_claims_analytics_bronze", 600)
# Run the healthcare claims silver layer notebook
dbutils.notebook.run(f"{notebook_path}03_healthcare_claims_analytics_silver", 600)
# # # Run the healthcare claims gold layer notebook
dbutils.notebook.run(f"{notebook_path}04_healthcare_claims_analytics_gold", 600)