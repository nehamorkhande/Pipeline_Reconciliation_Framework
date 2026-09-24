# Databricks notebook source
from pyspark.sql import functions as F


# COMMAND ----------

raw_path = "/Volumes/workspace/hospital_recon_bronze/raw_files"
bronze_db = "hospital_recon_bronze"

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {bronze_db}")

# COMMAND ----------

files = {
    "departments": "departments.csv",
    "doctors": "doctors.csv",
    "patients": "patients.csv",
    "encounters": "encounters.csv"
}

# COMMAND ----------

for table_name, file_name in files.items():
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(f"{raw_path}/{file_name}"))

    df = df.withColumn("_ingestion_ts", F.current_timestamp()) \
           .withColumn("_source_file", F.lit(file_name))

    df.write.format("delta").mode("overwrite") \
        .saveAsTable(f"{bronze_db}.bronze_{table_name}")

    print(f"bronze_{table_name}: {df.count()} rows written")

print("\nBronze layer complete.")

# COMMAND ----------

bronze_db = "hospital_recon_bronze"
silver_db = "hospital_recon_silver"

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_db}")

# COMMAND ----------

df=spark.table(f"{bronze_db}.bronze_departments")\
    .dropDuplicates(["dept_id"])\
        .select("dept_id", "dept_name", "building")


# COMMAND ----------

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{silver_db}.silver_departments")
print(f"silver_departments: {df.count()} rows")


# COMMAND ----------

df.display()

# COMMAND ----------

df=spark.table(f"{bronze_db}.bronze_doctors")\
    .dropDuplicates(["doctor_id"])\
    .select("doctor_id","name","specialization","dept_id")

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{silver_db}.silver_doctors")
print(f"silver_doctors: {df.count()} rows")


# COMMAND ----------

df = spark.table(f"{bronze_db}.bronze_patients") \
    .dropDuplicates(["patient_id"]) \
    .withColumn("age", F.col("age").cast("int")) \
    .dropDuplicates(["name", "age", "gender", "blood_group", "city"]) \
    .select("patient_id", "name", "age", "gender", "blood_group", "city")

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{silver_db}.silver_patients")
print(f"silver_patients: {df.count()} rows")
 

# COMMAND ----------

valid_patient_ids = df.select("patient_id")

# COMMAND ----------

df = spark.table(f"{bronze_db}.bronze_encounters") \
    .dropDuplicates(["encounter_id"]) \
    .withColumn("encounter_date", F.to_date("encounter_date")) \
    .withColumn("billing_amount", F.col("billing_amount").cast("double")) \
    .withColumn("length_of_stay", F.col("length_of_stay").cast("int")) \
    .filter(
        F.col("billing_amount").isNotNull()
        & (F.col("billing_amount") > 0)
        & F.col("patient_id").isNotNull()
    ) \
    .join(valid_patient_ids, "patient_id", "inner") \
    .select("encounter_id", "patient_id", "doctor_id", "dept_id",
            "encounter_date", "billing_amount", "length_of_stay", "diagnosis_code")
 
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{silver_db}.silver_encounters")
print(f"silver_encounters: {df.count()} rows")
 
print("\nSilver layer complete.")
 

# COMMAND ----------

