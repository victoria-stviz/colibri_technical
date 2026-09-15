# colibri_technical
Technical test for Colibri

Processing model: Batch processing as the data is updated and received daily.
Files are ingested daily into parquet files in a storage container. Files are named with following format: data_group_<group_number>
<group_number>: group number of the file, it can be 1, 2, 3, etc. It comes defined from the source.
Architecture: Medallion architecture
Layers
Bronze: Insert of raw data with a 24-hour cadence. There’s a bronze table for each file received.
Table names have the following format: bronze_turbine_output_data_group_<data_group>
Metadata columns added include:
record_load_timestamp: timestamp when the record was inserted into the bronze table.
Assumptions: 
-	The data comes only once a day. 
-	There’s only one file per group per day and files and records don’t change after they’re sent.
-	There’s no overlap of timestamp between received files i.e. files in consecutive days only contain measurements corresponding to the last 24 hours from the received date.
Silver: Delta load of records from the bronze tables. The reference column for the delta load is the record_load_timestamp. As the records don’t change once they’re created, there’s no merging of records.
Table names have the following format: silver_turbine_output
Column names are changed to the following:
Timestamp -> measurement_timestamp. Reason: to make clear what the timestamp corresponds to.
turbine_id -> turbine_number. Reason: reserved word ‘id’ for metadata and keys nomenclature that could be used in the gold tables.
wind_speed -> [remains the same]
wind_direction -> [remains the same]
power_output -> [remains the same]
Metadata columns:
delta_load_timestamp: timestamp when the record was loaded into the silver table
invalid: flag to signal a record that didn’t meet a DQ expectation
invalid_reason: short text describing which DQ expectation wasn’t met.
Data Quality handling:
-	Expected schema: 
-	Ideal: using Databricks’ assessSchema functions, a schema validation is performed to validate column names and number:
o	Columns not expected are stored in a rescued_data column
o	Schemas with missing columns from the expected are rejected and a notification sent to a contact person.
-	Implemented: forced schema as I don’t have all the Databricks setup.
-	Data completeness: When there’s a missing hour-slot for any of the turbines, a record is created with NULL values in the measurement columns (wind_speed, wind_direction, power_output) for data completeness.
-	Data Quality expectations: 
o	power_output +/- 2 standard deviations in the last 24 hours: when the record doesn’t meet this condition, the attribute keeps the source value, invalid = TRUE, invalid_reason = ‘measurement outside of accepted tolerance’
Gold layer: Two tables make up the Gold layer:
gold_turbine_output: Gold table with the same attributes as silver_turbine_output which contains only silver records with invalid = FALSE.
gold_summary_turbine_output: Summary gold view that contains the mean, min, max and average power_output for the following summary timeframes: last 24 hours, last 48 hours and last 72 hours calculated for each measurement_timestamp of each turbine.

