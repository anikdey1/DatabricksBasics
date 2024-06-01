# Databricks notebook source
# DBTITLE 1,Load File No. 1 from DBFS
data_frame_1 = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("dbfs:/FileStore/sample_data/industry_census_2018.csv")
)

display(data_frame_1)

# COMMAND ----------

# DBTITLE 1,Load File No. 2 from DBFS
data_frame_2 = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("dbfs:/FileStore/sample_data/occupation_census_2018.csv")
)

display(data_frame_2)