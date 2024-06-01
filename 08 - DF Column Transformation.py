# Databricks notebook source
# DBTITLE 1,Check Sample Databricks Datasets
# MAGIC %fs
# MAGIC ls /databricks-datasets/airlines/

# COMMAND ----------

# DBTITLE 1,Check First Part of a File
# MAGIC %fs
# MAGIC head /databricks-datasets/airlines/part-00000

# COMMAND ----------

# DBTITLE 1,Extract a Sample from the File
sample_airlines_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("samplingRatio", "0.0001")
    .load("/databricks-datasets/airlines/part-00000")
)

sample_airlines_df.display()

# COMMAND ----------

# DBTITLE 1,Select Columns Using Column Strings
sample_airlines_df.select("Origin", "Dest", "Distance").show(10)

# COMMAND ----------

# DBTITLE 1,Select Columns Using Column Objects
from pyspark.sql.functions import *

sample_airlines_df.select(
    column("Origin"), col("Dest"), sample_airlines_df.Distance
).show(10)

# COMMAND ----------

# DBTITLE 1,Create Column Expressions using Strings
sample_airlines_df.select(
    "Origin",
    "Dest",
    "Distance",
    expr("TO_DATE(CONCAT(Year, Month, DayOfMonth), 'yyyyMMdd') AS FlightDate")
).show(10)

# COMMAND ----------

# DBTITLE 1,Create Column Expressions Using Objects
sample_airlines_df.select(
    "Origin",
    "Dest",
    "Distance",
    to_date(concat("Year", "Month", "DayOfMonth"), "yyyyMMdd").alias("FlightDate")
).show(10)