# Databricks notebook source
# DBTITLE 1,Convert Strings into Dates in a DF Column
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *


def string_to_date(input_df, date_format, field_name):
    return input_df.withColumn(field_name, to_date(col(field_name), date_format))

# COMMAND ----------

# DBTITLE 1,Creating the Input DF
data_schema = StructType(
    [
        StructField("id", StringType()),
        StructField("event_date", StringType()),
    ]
)

sample_rows = [
    Row("101", "04/05/2020"),
    Row("102", "4/5/2020"),
    Row("103", "02/7/2020"),
    Row("104", "2/07/2020"),
]

# This is being used to partition (or "distribute") the data, which was just a simple list before.
rdd = spark.sparkContext.parallelize(sample_rows, 2)

sample_data_frame = spark.createDataFrame(rdd, data_schema)

# COMMAND ----------

# DBTITLE 1,Show Initial Data Frame
sample_data_frame.printSchema()
sample_data_frame.show()

# COMMAND ----------

# DBTITLE 1,Call UDF and Check Output
transformed_data_frame = string_to_date(sample_data_frame, "M/d/y", "event_date")

transformed_data_frame.printSchema()
transformed_data_frame.show()