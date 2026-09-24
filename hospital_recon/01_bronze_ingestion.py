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