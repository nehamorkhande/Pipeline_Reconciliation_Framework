# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

bronze_db = "hospital_recon_bronze"
silver_db = "hospital_recon_silver"
gold_db = "hospital_recon_gold"
recon_db = "hospital_recon_results"

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {recon_db}")

# COMMAND ----------

run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
run_ts = datetime.now()

# COMMAND ----------

results = []
src_count = spark.table(f"{bronze_db}.bronze_encounters").count()
tgt_count = spark.table(f"{gold_db}.fact_encounters").count()

row_count_status = "PASS" if src_count == tgt_count else "FAIL"

results.append({
    "run_id": run_id,
    "run_ts": run_ts,
    "check_type": "ROW_COUNT",
    "table_name": "encounters -> fact_encounters",
    "metric_name": "row_count",
    "source_value": str(src_count),
    "target_value": str(tgt_count),
    "status": row_count_status,
    "mismatch_count": abs(src_count - tgt_count)
})

# COMMAND ----------

print(results)

# COMMAND ----------

src_df = spark.table(f"{bronze_db}.bronze_encounters")
tgt_df = spark.table(f"{gold_db}.fact_encounters")

numeric_cols = ["billing_amount", "length_of_stay"]

for col in numeric_cols:
    src_sum = src_df.agg(F.sum(F.col(col).cast("double"))).collect()[0][0] or 0.0
    tgt_sum = tgt_df.agg(F.sum(F.col(col).cast("double"))).collect()[0][0] or 0.0

    diff = abs(round(src_sum - tgt_sum, 2))
    status = "PASS" if diff < 0.01 else "FAIL"

    results.append({
        "run_id": run_id,
        "run_ts": run_ts,
        "check_type": "COLUMN_SUM",
        "table_name": "encounters -> fact_encounters",
        "metric_name": f"sum_{col}",
        "source_value": f"{src_sum:.2f}",
        "target_value": f"{tgt_sum:.2f}",
        "status": status,
        "mismatch_count": 0 if status == "PASS" else 1
    })

# COMMAND ----------

print(results)

# COMMAND ----------

def row_hash_df(df, cols, id_col):
    return df.select(
        F.col(id_col).alias("encounter_id"),
        F.md5(F.concat_ws("||", *[F.col(c).cast("string") for c in cols])).alias("row_hash")
    )

hash_cols_src = ["encounter_id", "patient_id", "doctor_id", "dept_id", "billing_amount", "length_of_stay", "diagnosis_code"]
hash_cols_tgt = ["encounter_id", "patient_id", "doctor_id", "dept_id", "billing_amount", "length_of_stay", "diagnosis_code"]

src_hashes = row_hash_df(src_df, hash_cols_src, "encounter_id")
tgt_hashes = row_hash_df(tgt_df, hash_cols_tgt, "encounter_id")

# COMMAND ----------

src_hash_agg = src_hashes.select(
    F.conv(F.substring("row_hash", 1, 15), 16, 10).cast("decimal(38,0)").alias("h")
).agg(F.sum("h")).collect()[0][0]

# COMMAND ----------

tgt_hash_agg = tgt_hashes.select(
    F.conv(F.substring("row_hash", 1, 15), 16, 10).cast("decimal(38,0)").alias("h")
).agg(F.sum("h")).collect()[0][0]

# COMMAND ----------

src_fingerprint = str(src_hash_agg)
tgt_fingerprint = str(tgt_hash_agg)

# COMMAND ----------

hash_status = "PASS" if src_fingerprint == tgt_fingerprint else "FAIL"

results.append({
    "run_id": run_id,
    "run_ts": run_ts,
    "check_type": "COLUMN_HASH",
    "table_name": "encounters -> fact_encounters",
    "metric_name": "table_fingerprint_md5",
    "source_value": src_fingerprint,
    "target_value": tgt_fingerprint,
    "status": hash_status,
    "mismatch_count": 0 if hash_status == "PASS" else 1
})

# COMMAND ----------


compare_cols = ["patient_id", "doctor_id", "dept_id", "billing_amount", "length_of_stay", "diagnosis_code"]

src_renamed = src_df.select(
    "encounter_id",
    *[F.col(c).alias(f"src_{c}") for c in compare_cols]
)
tgt_renamed = tgt_df.select(
    "encounter_id",
    *[F.col(c).alias(f"tgt_{c}") for c in compare_cols]
)

joined = src_renamed.join(tgt_renamed, "encounter_id", "outer")

# COMMAND ----------

mismatch_conditions = []
for c in compare_cols:
    mismatch_conditions.append(
        (F.col(f"src_{c}") != F.col(f"tgt_{c}")) |
        (F.col(f"src_{c}").isNull() != F.col(f"tgt_{c}").isNull())
    )

# COMMAND ----------

combined_condition = mismatch_conditions[0]
for cond in mismatch_conditions[1:]:
    combined_condition = combined_condition | cond

# COMMAND ----------

mismatched_rows = joined.filter(combined_condition)
mismatch_count = mismatched_rows.count()

record_diff_status = "PASS" if mismatch_count == 0 else "FAIL"

# COMMAND ----------

results.append({
    "run_id": run_id,
    "run_ts": run_ts,
    "check_type": "RECORD_DIFF",
    "table_name": "encounters -> fact_encounters",
    "metric_name": "mismatched_rows",
    "source_value": str(src_df.count()),
    "target_value": str(tgt_df.count()),
    "status": record_diff_status,
    "mismatch_count": mismatch_count
})

# COMMAND ----------

print(results)

# COMMAND ----------

mismatched_rows.withColumn("run_id", F.lit(run_id)) \
    .limit(10000) \
    .write.format("delta").mode("overwrite") \
    .saveAsTable(f"{recon_db}.record_diff_details")

print(f"Mismatched rows: {mismatch_count}")

# COMMAND ----------

results_df = spark.createDataFrame(results)
results_df.write.format("delta").mode("append") \
    .saveAsTable(f"{recon_db}.reconciliation_checks")

print(f"\nRun ID: {run_id}")
results_df.show(truncate=80)