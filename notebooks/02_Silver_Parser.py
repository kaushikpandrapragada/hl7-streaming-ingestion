# 02_Silver_Parser.py
from pyspark.sql.functions import split, col, element_at

def parse_hl7_stream(df):
    # HL7 segments are separated by Carriage Returns (\r)
    # We create a structured view of the most important segments: MSH, PID, PV1
    return df.withColumn("segments", split(col("raw_content"), "\r")) \
             .withColumn("MSH", col("segments")[0]) \
             .withColumn("PID", F.expr("filter(segments, x -> x LIKE 'PID%')")[0]) \
             .withColumn("PV1", F.expr("filter(segments, x -> x LIKE 'PV1%')")[0]) \
             .select(
                # Extract Event Type from MSH segment (e.g., ADT^A01)
                split(col("MSH"), "\|")[8].alias("event_type"),
                # Extract Patient ID (PID-3)
                split(col("PID"), "\|")[3].alias("patient_id"),
                # Extract Patient Name (PID-5)
                split(col("PID"), "\|")[5].alias("patient_name"),
                # Extract Assigned Location (PV1-3)
                split(col("PV1"), "\|")[3].alias("assigned_location"),
                col("ingested_at")
             )

silver_stream = spark.readStream.table("clinical_db.bronze_hl7_raw")
parsed_df = parse_hl7_stream(silver_stream)

# Save to Silver (Structured Delta)
(parsed_df.writeStream
  .format("delta")
  .option("checkpointLocation", "/mnt/healthcare/checkpoints/silver_hl7/")
  .outputMode("append")
  .table("clinical_db.silver_hl7_structured"))
