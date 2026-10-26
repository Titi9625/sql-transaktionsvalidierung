# Databricks notebook source
# Cell 1 - Inspect the environment (read-only).
# Aim: confirm Spark works and discover the current catalog/schema.
display(spark.sql("""
    SELECT
        current_catalog() AS current_catalog,
        current_schema() AS current_schema
"""))
display(spark.sql("SHOW CATALOGS"))

# COMMAND ----------

# Cell 2 - Create project containers and a managed input-file volume.
# Aim: prepare Bronze (raw), Silver (prepared/validated), Gold (reports).
# Expected: three reconciliation_* schemas and one incoming_files volume.
# No CSVs are loaded and no transaction tables are created in this cell.
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.reconciliation_bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.reconciliation_silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.reconciliation_gold")

spark.sql("""
    CREATE VOLUME IF NOT EXISTS
    workspace.reconciliation_bronze.incoming_files
""")

display(spark.sql("SHOW SCHEMAS IN workspace LIKE 'reconciliation_*'"))
display(spark.sql("SHOW VOLUMES IN workspace.reconciliation_bronze"))
