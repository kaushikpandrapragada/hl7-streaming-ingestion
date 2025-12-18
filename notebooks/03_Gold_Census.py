# 03_Gold_Census.py

def upsert_to_census(batch_df, batch_id):
    # Logic: 
    # A01 (Admit) -> Insert/Update
    # A03 (Discharge) -> Delete from Active Census
    
    batch_df.createOrReplaceTempView("updates")
    
    batch_df._jdf.sparkSession().sql("""
        MERGE INTO clinical_db.gold_active_census AS target
        USING (
            -- Get only the latest event per patient in this batch
            SELECT * FROM (
                SELECT *, row_number() OVER(PARTITION BY patient_id ORDER BY ingested_at DESC) as rank
                FROM updates
            ) WHERE rank = 1
        ) AS source
        ON target.patient_id = source.patient_id
        
        -- If patient is discharged (A03), remove them from current census
        WHEN MATCHED AND source.event_type = 'ADT^A03' THEN 
            DELETE
            
        -- If patient is already in table and it's a transfer (A02) or update, update info
        WHEN MATCHED THEN
            UPDATE SET 
                target.current_location = source.assigned_location,
                target.last_event_time = source.ingested_at
        
        -- If it's a new admission (A01), insert record
        WHEN NOT MATCHED AND source.event_type != 'ADT^A03' THEN
            INSERT (patient_id, patient_name, current_location, last_event_time)
            VALUES (source.patient_id, source.patient_name, source.assigned_location, source.ingested_at)
    """)

# Execute the stream
(spark.readStream.table("clinical_db.silver_hl7_structured")
  .writeStream
  .foreachBatch(upsert_to_census)
  .start())
