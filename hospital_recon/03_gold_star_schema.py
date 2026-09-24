# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

silver_db="hospital_recon_silver"
gold_db="hospital_recon_gold"

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {gold_db}")

# COMMAND ----------

dim_department = spark.table(f"{silver_db}.silver_departments") \
    .select("dept_id", "dept_name", "building")
dim_department.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{gold_db}.dim_department")
print(f"dim_department: {dim_department.count()} rows")


# COMMAND ----------

dim_doctor = spark.table(f"{silver_db}.silver_doctors") \
    .select("doctor_id", "name", "specialization", "dept_id")
dim_doctor.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{gold_db}.dim_doctor")
print(f"dim_doctor: {dim_doctor.count()} rows")

# COMMAND ----------

silver_patients = spark.table(f"{silver_db}.silver_patients") \
    .select("patient_id", "name", "age", "gender", "blood_group", "city")
    

# COMMAND ----------

scd_cols = ["name", "age", "gender", "blood_group", "city"]  
target_table = f"{gold_db}.dim_patient"


# COMMAND ----------

spark.table("hospital_recon_silver.silver_patients").printSchema()

# COMMAND ----------


if not spark.catalog.tableExists(target_table):
    dim_patient_init = silver_patients \
        .withColumn("patient_sk", F.monotonically_increasing_id().cast("bigint")) \
        .withColumn("effective_date", F.lit("2025-01-01").cast("date")) \
        .withColumn("end_date", F.lit(None).cast("date")) \
        .withColumn("is_current", F.lit(True))
 
    dim_patient_init.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
    print(f"dim_patient: initialized with {dim_patient_init.count()} rows (SCD2)")
 
else:
    from delta.tables import DeltaTable
 
    dim_patient_dt = DeltaTable.forName(spark, target_table)
    current_dim = dim_patient_dt.toDF().filter(F.col("is_current") == True)
 
    incoming = silver_patients.alias("src")
    current = current_dim.select("patient_id", *scd_cols).alias("cur")
 
    change_condition = " OR ".join([f"src.{c} <> cur.{c}" for c in scd_cols])
 
    changes = incoming.join(current, "patient_id", "left") \
        .where(f"cur.patient_id IS NULL OR ({change_condition})") \
        .select("src.*")
 
    change_count = changes.count()

    if change_count > 0:
        changed_ids = [r.patient_id for r in changes.select("patient_id").collect()]
 
        dim_patient_dt.update(
            condition=(F.col("is_current") == True) & (F.col("patient_id").isin(changed_ids)),
            set={
                "is_current": F.lit(False),
                "end_date": F.current_date()
            }
        )


        max_sk = dim_patient_dt.toDF().agg(F.max("patient_sk")).collect()[0][0] or 0
 
        new_versions = changes \
            .withColumn("row_num", F.row_number().over(Window.orderBy("patient_id"))) \
            .withColumn("patient_sk", (F.col("row_num") + F.lit(max_sk)).cast("bigint")) \
            .withColumn("effective_date", F.current_date()) \
            .withColumn("end_date", F.lit(None).cast("date")) \
            .withColumn("is_current", F.lit(True)) \
            .drop("row_num")
 
        new_versions.write.format("delta").mode("append").saveAsTable(target_table)
        print(f"dim_patient: {change_count} new/changed records added (SCD2)")
    else:
        print("dim_patient: no changes detected (SCD2)")


# COMMAND ----------

dim_patient_current = spark.table(target_table).filter(F.col("is_current") == True)
print(f"dim_patient: {dim_patient_current.count()} current rows, "
      f"{spark.table(target_table).count()} total rows (incl. history)")
 

# COMMAND ----------

enc = spark.table(f"{silver_db}.silver_encounters")
 
dim_date = enc.select(F.col("encounter_date").alias("date")).distinct() \
    .withColumn("date_id", F.date_format("date", "yyyyMMdd").cast("int")) \
    .withColumn("year", F.year("date")) \
    .withColumn("month", F.month("date")) \
    .withColumn("quarter", F.quarter("date")) \
    .withColumn("day_of_week", F.date_format("date", "EEEE")) \
    .select("date_id", "date", "year", "month", "quarter", "day_of_week")
 
dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{gold_db}.dim_date")
print(f"dim_date: {dim_date.count()} rows")

# COMMAND ----------

patient_sk_map = spark.table(f"{gold_db}.dim_patient") \
    .filter(F.col("is_current") == True) \
    .select("patient_id", "patient_sk")
 

# COMMAND ----------

fact_encounters = enc \
    .withColumn("date_id", F.date_format("encounter_date", "yyyyMMdd").cast("int")) \
    .join(patient_sk_map, "patient_id", "left") \
    .select(
        "encounter_id",
        "patient_id",
        "patient_sk",
        "doctor_id",
        "dept_id",
        "date_id",
        "billing_amount",
        "length_of_stay",
        "diagnosis_code"
    )

fact_encounters.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{gold_db}.fact_encounters")
print(f"fact_encounters: {fact_encounters.count()} rows")

# COMMAND ----------

print("\nGold layer (Star Schema) complete.")
print(f"  Fact: fact_encounters")
print(f"  Dims: dim_patient, dim_doctor, dim_department, dim_date")

# COMMAND ----------

from datetime import datetime as _dt

# COMMAND ----------

run_ts = _dt.now()

# COMMAND ----------

fact_encounters_history = fact_encounters \
    .withColumn("gold_run_ts", F.lit(run_ts).cast("timestamp")) \
    .withColumn("gold_run_id", F.lit(run_ts.strftime("%Y%m%d_%H%M%S")))

# COMMAND ----------

fact_encounters_history.write.format("delta").mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{gold_db}.fact_encounters_history")

# COMMAND ----------

total_history_rows = spark.table(f"{gold_db}.fact_encounters_history").count()
print(f"fact_encounters_history: +{fact_encounters_history.count()} rows this run "
      f"({total_history_rows} total across all runs)")

# COMMAND ----------

print("\nGold layer (Star Schema) complete.")
print(f"  Fact (current):  fact_encounters")
print(f"  Fact (history):  fact_encounters_history")
print(f"  Dims: dim_patient (SCD2), dim_doctor, dim_department, dim_date")

# COMMAND ----------

