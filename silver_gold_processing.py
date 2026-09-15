# import libraries
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, 
    current_timestamp, 
    col, 
    lit, 
    stddev, 
    when,
    coalesce,
    explode,
    min as spark_min,
    max as spark_max,
    sequence,
    expr
    )
import os
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    IntegerType,
    DecimalType,
    BooleanType,
    StringType,
    DoubleType,
)
from functools import reduce

spark = (
    SparkSession.builder
    .appName("ReadLocalCSV")
    .master("local[*]")
    .getOrCreate()
)


%% [markdown]
# # SILVER PROCESSING
#
# This section creates the SILVER_turbine_output from all the existing silver dataframes created from bronze

# %%
#### Set silver processing variables and schema

# Set silver schema
silver_schema = StructType([
    StructField("measurement_timestamp", TimestampType(), True),
    StructField("turbine_number", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", DoubleType(), True),
    StructField("power_output", DoubleType(), True),
    StructField("delta_load_timestamp", TimestampType(), True),
    StructField("invalid", BooleanType(), True),
    StructField("invalid_reason", StringType(), True),
])

# silver dataframes name
silver_dataframes = []

%% [markdown]
# # # BRONZE PROCESSING
#
# This section reads the source files and creates a dataframe for each which are processed into silver dataframes
# Ideally, it would write each bronze dataframe into a bronze delta table, for which the code is commented out
# and write into a silver delta table from the unioned silver dataframe

# %%

####### Extract all files in path: "C:\Users\vicky\colibri_turbine\" #######
### In a real-life situation, the path would be on a data lake and files would be ingested from it.
### There would be an agreed process on the location of the daily files (folder with date in the name, for example) and the logic to ingest would need to consider it


turbine_group_list = []

for filename in os.listdir(r"C:\Users\vicky\colibri_turbine"):
    if filename.endswith(".csv"):
        turbine_group_list.append(filename)


# Read all CSV files in the list, create a DataFrame and store in a bronze table for each file

for turbine_group in turbine_group_list:
    csv_path = os.path.join(r"C:\Users\vicky\colibri_turbine", turbine_group)

    bronze_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .csv(csv_path)
    )
    bronze_df = bronze_df.withColumn("record_load_timestamp", current_timestamp())

    ###### SILVER PROCESSING ######
    ## When using delta tables, the silver processing would be done in a separate notebook that reads bronze and writes to silver.
    ## Ideally, there would be a delta load into silver using a last_delta_load_date as reference to get the latest records from bronze

    # Calculate 2x standard deviations for power_output grouped by turbine_id
    turbine_stdev_df = (
        bronze_df.groupBy("turbine_id")
        .agg(
            avg("power_output").alias("power_output_mean"),
            stddev("power_output").alias("power_output_stddev"),
        )
    )

    lower_limit = col("power_output_mean") - 2 * col("power_output_stddev")
    upper_limit = col("power_output_mean") + 2 * col("power_output_stddev")

    # print(f"Silver std dev for {turbine_group}:")
    # turbine_stdev_df.show()

    silver_df = (
        bronze_df
        .join(turbine_stdev_df, on="turbine_id", how="left")
        .withColumn("delta_load_timestamp", current_timestamp())
        # validate that power_output is within 2x standard deviations of the mean for each turbine_id
        .withColumn(
            "invalid", 
            when(
                (col("power_output") < lower_limit) | (col("power_output") > upper_limit),
                lit(True),
            ).otherwise(lit(False)),
            )
        .withColumn(
            "invalid_reason",
            when(
                (col("power_output") < lower_limit) | (col("power_output") > upper_limit),
                lit("power_output is outside the 2x standard deviation above mean range"),
            ).otherwise(lit(None))
        )
        .withColumnRenamed("timestamp", "measurement_timestamp")
        .withColumnRenamed("turbine_id", "turbine_number")
        .select(
            "measurement_timestamp",
            "turbine_number",
            "wind_speed",
            "wind_direction",
            "power_output",
            "delta_load_timestamp",
            "invalid",
            "invalid_reason"
        )
    )

    silver_dataframes.append(silver_df)

    # print(f"Silver dataframe for {turbine_group}:")
    # silver_df.show()

####### Create the silver dataframe by merging all silver dataframes and removing duplicates based on turbine_number and measurement_timestamp
## Ideally, this would be a merge of the latest records from bronze into silver.
## As I'm assuming there's no data overlap between files and once the files are received, they're not changed, this is a union of all silver dataframes

merged_silver_df = (
    reduce(
        lambda first_df, next_df: first_df.unionByName(next_df),
        silver_dataframes,
    )
    .dropDuplicates(["turbine_number", "measurement_timestamp"])
)

####### Data completeness 
## expected hourly range for each turbine
turbine_ranges_df = (
    merged_silver_df
    .groupBy("turbine_number")
    .agg(
        spark_min("measurement_timestamp").alias("first_timestamp"),
        spark_max("measurement_timestamp").alias("last_timestamp"),
    )
)

# print(f"Expected hourly range for each turbine:")
# turbine_ranges_df.show()

## generate one expected timestamp per hour for each turbine
expected_readings_df = (
    turbine_ranges_df
    .withColumn(
        "measurement_timestamp",
        explode(
            sequence(
                col("first_timestamp"),
                col("last_timestamp"),
                expr("INTERVAL 1 HOUR"),
            )
        ),
    )
    .select("turbine_number", "measurement_timestamp")
)

# print(f"Expected readings for each turbine:")
# expected_readings_df.show()

## retain existing records and identify missing timestamps
complete_silver_df = (
    expected_readings_df
    .join(
        merged_silver_df,
        on=["turbine_number", "measurement_timestamp"],
        how="left",
    )
    .withColumn(
        "invalid",
        coalesce(col("invalid"), lit(True)),
    )
    .withColumn(
        "invalid_reason",
        coalesce(
            col("invalid_reason"),
            lit("missing measurement reading"),
        ),
    )
    .select(
        "measurement_timestamp",
        "turbine_number",
        "wind_speed",
        "wind_direction",
        "power_output",
        "delta_load_timestamp",
        "invalid",
        "invalid_reason",
    )
)

## Write silver table into a delta table

# table_name = "silver_turbine_output"

# df.write.mode("append").format("delta").saveAsTable(table_name)

%% [markdown]
# # GOLD PROCESSING
#
# This section creates the gold_turbine_output and gold_summary_turbine_output dataframes and indicates how it'd merge into a gold delta table

# %%

##### create target gold dataframe

gold_schema = StructType([
    StructField("turbine_number", IntegerType(), True),
    StructField("measurement_timestamp", TimestampType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", DoubleType(), True),
    StructField("power_output", DoubleType(), True),
    StructField("delta_load_timestamp", TimestampType(), True),
])

gold_turbine_output_df = spark.createDataFrame([], gold_schema)

##### create silver_temp_view to be used in gold processing
## this temp view is created to be able to illustrate how the gold processing would be done. Ideally, the processing would be using a delta table
complete_silver_df.createOrReplaceTempView("silver_turbine_output")

##### calculate last_delta_load_timestamp for gold, if there isn't any, set it to '1900-01-01 00:00:00' so it loads all records
gold_exists = spark._jsparkSession.catalog().tableExists("gold_turbine_output")
if gold_exists:
    last_delta_load_timestamp = (
        spark.sql("select max(delta_load_timestamp) as last_delta_load_timestamp from gold_turbine_output")
        .collect()[0]["last_delta_load_timestamp"]
    )
else:
    last_delta_load_timestamp = "1900-01-01 00:00:00"

print(f"Last delta load timestamp for gold: {last_delta_load_timestamp}")

## create source dataframe with delta logic to load only new and valid records from silver to gold.
gold_source_df = spark.sql(f"""
SELECT
    silver_turbine.turbine_number,
    silver_turbine.measurement_timestamp,
    silver_turbine.wind_speed,
    silver_turbine.wind_direction,
    silver_turbine.power_output,
    current_timestamp() AS delta_load_timestamp
FROM silver_turbine_output AS silver_turbine
WHERE 
    silver_turbine.delta_load_timestamp > '{last_delta_load_timestamp}'
    AND silver_turbine.invalid = false
    """)

## Ideally, we would be merging the source dataframe into the gold delta table

# gold_turbine_output.alias("target").merge(
#     gold_source_df.alias("source"), 
#     "target.turbine_number = source.turbine_number AND target.measurement_timestamp = source.measurement_timestamp") \
# .whenMatchedUpdateAll() \
# .whenNotMatchedInsertAll() \
# .execute()

## But since we don't have the ability to create delta tables, I'll just append the source dataframe to the gold dataframe
gold_turbine_output_df = gold_turbine_output_df.unionByName(gold_source_df)

###### create the gold_summary_turbine_output view

gold_turbine_output_df.createOrReplaceTempView("gold_turbine_output")

gold_summary_turbine_output_df = spark.sql(f"""
WITH base AS (
    SELECT
        turbine_number,
        measurement_timestamp,
        power_output,

        CAST(measurement_timestamp AS BIGINT) AS timestamp_seconds,

        LAG(measurement_timestamp, 24)
            OVER (
                PARTITION BY turbine_number
                ORDER BY measurement_timestamp
            ) AS lag_24h,

        LAG(measurement_timestamp, 48)
            OVER (
                PARTITION BY turbine_number
                ORDER BY measurement_timestamp
            ) AS lag_48h,

        LAG(measurement_timestamp, 72)
            OVER (
                PARTITION BY turbine_number
                ORDER BY measurement_timestamp
            ) AS lag_72h

FROM gold_turbine_output
)
, rolling_windows AS (
    SELECT
        *,

        AVG(power_output) OVER (
            PARTITION BY turbine_number
            ORDER BY timestamp_seconds
            RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW
        ) AS avg_24h,

        MIN(power_output) OVER (
            PARTITION BY turbine_number
            ORDER BY timestamp_seconds
            RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW
        ) AS min_24h,

        MAX(power_output) OVER (
            PARTITION BY turbine_number
            ORDER BY timestamp_seconds
            RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW
        ) AS max_24h,

        COUNT(power_output) OVER (
            PARTITION BY turbine_number
            ORDER BY timestamp_seconds
            RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW
        ) AS count_24h,

        AVG(power_output) OVER (
            PARTITION BY turbine_number
            ORDER BY timestamp_seconds
            RANGE BETWEEN 172800 PRECEDING AND CURRENT ROW
        ) AS avg_48h,

        MIN(power_output) OVER (
            PARTITION BY turbine_number
            ORDER BY timestamp_seconds
            RANGE BETWEEN 172800 PRECEDING AND CURRENT ROW
        ) AS min_48h,

        MAX(power_output) OVER (
            PARTITION BY turbine_number
            ORDER BY timestamp_seconds
            RANGE BETWEEN 172800 PRECEDING AND CURRENT ROW
        ) AS max_48h,

        COUNT(power_output) OVER (
            PARTITION BY turbine_number
            ORDER BY timestamp_seconds
            RANGE BETWEEN 172800 PRECEDING AND CURRENT ROW
        ) AS count_48h,

        AVG(power_output) OVER (
            PARTITION BY turbine_number
            ORDER BY timestamp_seconds
            RANGE BETWEEN 259200 PRECEDING AND CURRENT ROW
        ) AS avg_72h,

        MIN(power_output) OVER (
            PARTITION BY turbine_number
            ORDER BY timestamp_seconds
            RANGE BETWEEN 259200 PRECEDING AND CURRENT ROW
        ) AS min_72h,

        MAX(power_output) OVER (
            PARTITION BY turbine_number
            ORDER BY timestamp_seconds
            RANGE BETWEEN 259200 PRECEDING AND CURRENT ROW
        ) AS max_72h,

        COUNT(power_output) OVER (
            PARTITION BY turbine_number
            ORDER BY timestamp_seconds
            RANGE BETWEEN 259200 PRECEDING AND CURRENT ROW
        ) AS count_72h

    FROM base
)

SELECT
    turbine_number,
    measurement_timestamp,
    '24 hours' AS summary_timeframe,
    CASE WHEN lag_24h IS NOT NULL AND count_24h = 25
         THEN avg_24h END AS mean_power_output,
    CASE WHEN lag_24h IS NOT NULL AND count_24h = 25
         THEN min_24h END AS min_power_output,
    CASE WHEN lag_24h IS NOT NULL AND count_24h = 25
         THEN max_24h END AS max_power_output
FROM rolling_windows

UNION ALL

SELECT
    turbine_number,
    measurement_timestamp,
    '48 hours',
    CASE WHEN lag_48h IS NOT NULL AND count_48h = 49
         THEN avg_48h END,
    CASE WHEN lag_48h IS NOT NULL AND count_48h = 49
         THEN min_48h END,
    CASE WHEN lag_48h IS NOT NULL AND count_48h = 49
         THEN max_48h END
FROM rolling_windows

UNION ALL

SELECT
    turbine_number,
    measurement_timestamp,
    '72 hours',
    CASE WHEN lag_72h IS NOT NULL AND count_72h = 73
         THEN avg_72h END,
    CASE WHEN lag_72h IS NOT NULL AND count_72h = 73
         THEN min_72h END,
    CASE WHEN lag_72h IS NOT NULL AND count_72h = 73
         THEN max_72h END
FROM rolling_windows

""").createOrReplaceTempView('gold_summary_turbine_output')

# spark.sql("select * from gold_summary_turbine_output where mean_power_output is not null").show()
# spark.sql("select * from gold_turbine_output where turbine_number = 1").show()