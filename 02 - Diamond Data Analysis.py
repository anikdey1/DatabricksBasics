# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Loading

# COMMAND ----------

# DBTITLE 1,Load Data File from DBFS
diamond_data = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("dbfs:/FileStore/sample_data/diamonds.csv")
)

# diamond_data.show(12)
display(diamond_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Analysis and Visualisation

# COMMAND ----------

# DBTITLE 1,Figure out Average Price by Colour
from pyspark.sql.functions import avg

average_price_by_colour = (
    diamond_data.select("color", "price")
    .groupBy("color")
    .agg(avg("price"))
    .sort("color")
)

# average_price_by_colour.show()
display(average_price_by_colour)