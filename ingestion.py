# import libraries
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, lit
import os

spark = (
    SparkSession.builder
    .appName("ReadLocalCSV")
    .master("local[*]")
    .getOrCreate()
)

# Extract all files in path: "C:\Users\vicky\colibri_turbine\"

turbine_group_list = []

for filename in os.listdir(r"C:\Users\vicky\colibri_turbine"):
    if filename.endswith(".csv"):
        turbine_group_list.append(filename)

# print(turbine_group_list)


# Read all CSV files in the list, create a DataFrame and store in a bronze table for each file

for turbine_group in turbine_group_list:
    csv_path = os.path.join(r"C:\Users\vicky\colibri_turbine", turbine_group)

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .csv(csv_path)
    )
    df = df.withColumn("record_load_timestamp", lit(current_timestamp()))


    df.write.mode("overwrite").format("delta").saveAsTable(f"bronze_turbine_output_{turbine_group.split('.')[0]}")
    