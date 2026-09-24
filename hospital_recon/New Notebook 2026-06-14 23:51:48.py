# Databricks notebook source
spark.sql("GRANT USE CATALOG ON CATALOG workspace TO `9d8c9c44-6d9e-4b75-a25b-e6ebb5db727e`")
spark.sql("GRANT USE SCHEMA ON SCHEMA workspace.hospital_recon_results TO `9d8c9c44-6d9e-4b75-a25b-e6ebb5db727e`")
spark.sql("GRANT SELECT ON SCHEMA workspace.hospital_recon_results TO `9d8c9c44-6d9e-4b75-a25b-e6ebb5db727e`")

# COMMAND ----------

recon_db = "hospital_recon_results"
dedup = spark.table(f"{recon_db}.reconciliation_summary").dropDuplicates(["run_id"])
dedup.write.format("delta").mode("overwrite").saveAsTable(f"{recon_db}.reconciliation_summary")
print("Deduplicated reconciliation_summary")

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG workspace TO `9d8c9c44-6d9e-4b75-a25b-e6ebb5db727e`;