import os
import sys
import pyspark
from pyspark.sql import SparkSession

print("Python:", sys.version)
print("PySpark:", pyspark.__version__)
print("JAVA_HOME:", os.environ.get("JAVA_HOME"))