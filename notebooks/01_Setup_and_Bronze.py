# 01_Setup_and_Bronze.py
from pyspark.sql.functions import input_file_name, current_timestamp

raw_data_path = "/mnt/healthcare/landing/hl7_adt/"
checkpoint_path = "/mnt/healthcare/checkpoints/bronze_hl7/"

# Autoloader - Incremental ingestion
df_bronze = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "text") # HL7 is text-based
  .load(raw_data_path)
  .select(
    F.col("value").alias("raw_content"),
    input_file_name().alias("source_file"),
    current_timestamp().alias("ingested_at")
  ))

# Write to Bronze Table
(df_bronze.writeStream
  .format("delta")
  .option("checkpointLocation", checkpoint_path)
  .outputMode("append")
  .table("clinical_db.bronze_hl7_raw"))
