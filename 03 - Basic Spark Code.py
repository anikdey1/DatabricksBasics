# Databricks notebook source
# DBTITLE 1,Create A Basic Spark Session and Data Frame
from pyspark.sql import *

spark_session = (
    SparkSession.builder.appName("BasicSpark").master("local[2]").getOrCreate()
)

input_data_list = [("Alice", 20), ("Brian", 21), ("Celia", 22)]

name_age_df = spark_session.createDataFrame(input_data_list).toDF("Name", "Age")

display(name_age_df)