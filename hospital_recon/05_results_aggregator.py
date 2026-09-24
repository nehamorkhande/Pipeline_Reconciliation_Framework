# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

recon_db = "hospital_recon_results"

# COMMAND ----------

checks = spark.table(f"{recon_db}.reconciliation_checks")
latest_run_id = checks.agg(F.max("run_id")).collect()[0][0]
 

# COMMAND ----------

if spark.catalog.tableExists(f"{recon_db}.reconciliation_summary"):
    existing_run_ids = [
        row["run_id"]
        for row in spark.table(f"{recon_db}.reconciliation_summary")
                        .select("run_id")
                        .distinct()
                        .collect()
    ]
else:
    existing_run_ids = []
 

# COMMAND ----------

if latest_run_id in existing_run_ids:
    print(f"Run {latest_run_id} already summarized - skipping (no duplicate row added).")
    print("If you want fresh numbers, re-run the full pipeline "
          "(generator -> bronze -> silver -> gold -> reconciliation_engine) "
          "to produce a NEW run_id first.")
else:
    latest_checks = checks.filter(F.col("run_id") == latest_run_id)
 
    summary = latest_checks.groupBy("run_id", "run_ts", "table_name") \
        .agg(
            F.count("*").alias("total_checks"),
            F.sum(F.when(F.col("status") == "PASS", 1).otherwise(0)).alias("passed"),
            F.sum(F.when(F.col("status") == "FAIL", 1).otherwise(0)).alias("failed"),
            F.sum("mismatch_count").alias("total_mismatches")
        ) \
        .withColumn(
            "overall_status",
            F.when(F.col("failed") == 0, "PASS").otherwise("FAIL")
        )
 
    summary.write.format("delta").mode("append") \
        .saveAsTable(f"{recon_db}.reconciliation_summary")
 
    print(f"Latest run: {latest_run_id}")
    summary.show(truncate=False)

# COMMAND ----------

latest_checks = checks.filter(F.col("run_id") == latest_run_id)
print("\nDetailed checks for latest run:")
latest_checks.select("check_type", "metric_name", "source_value", "target_value", "status", "mismatch_count") \
    .show(truncate=60)
 
print("\nResults aggregator complete. Tables ready for Streamlit dashboard:")
print(f"  {recon_db}.reconciliation_checks")
print(f"  {recon_db}.reconciliation_summary")
print(f"  {recon_db}.record_diff_details")

# COMMAND ----------

print("\nResults aggregator complete. Tables ready for Streamlit dashboard:")
print(f"  {recon_db}.reconciliation_checks")
print(f"  {recon_db}.reconciliation_summary")
print(f"  {recon_db}.record_diff_details")