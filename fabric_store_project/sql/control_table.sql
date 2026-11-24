-- Execute in a writable Fabric Lakehouse Spark SQL session, not the read-only SQL endpoint.
CREATE TABLE IF NOT EXISTS lh_bronze.etl_control (
 source_schema STRING, source_table STRING, target_table STRING,
 load_type STRING, watermark_column STRING, last_watermark STRING,
 is_active BOOLEAN, last_loaded_at TIMESTAMP, last_rows BIGINT
) USING DELTA;
-- Seed one row per actual source table after inspecting PostgreSQL information_schema.
-- Example (replace names):
-- INSERT INTO lh_bronze.etl_control VALUES ('shop','orders','orders','full',NULL,NULL,true,NULL,NULL);
