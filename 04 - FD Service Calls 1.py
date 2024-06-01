# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Loading

# COMMAND ----------

# DBTITLE 1,Load Data from the CSV File
service_call_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("dbfs:/FileStore/sample_data/fire_department_service_calls_25k.csv")
)

# The 'display()' function is not a Spark feature; it's a Databricks feature.
display(service_call_df)

# COMMAND ----------

# DBTITLE 1,Load CSV Data (Using an Alternative Method)
alternate_service_call_df = spark.read.csv(
    "dbfs:/FileStore/sample_data/fire_department_service_calls_25k.csv",
    header="true",
    inferSchema="true",
)

display(alternate_service_call_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Temporary View Creation

# COMMAND ----------

# DBTITLE 1,Create a Temporary View from the DF
# This creates a temporary view from the DF, on which SQL statements can be executed.
service_call_df.createGlobalTempView('fd_service_calls_temporary_view')

# COMMAND ----------

# DBTITLE 1,Query the Temporary View using SQL
# MAGIC %sql
# MAGIC -- All temporary objects are stored in the hidden 'global_temp' DB.
# MAGIC SELECT
# MAGIC   `Call Type`,
# MAGIC   COUNT(`Call Type`) AS `Call Type Count`
# MAGIC FROM
# MAGIC   global_temp.fd_service_calls_temporary_view
# MAGIC GROUP BY
# MAGIC   `Call Type`
# MAGIC ORDER BY
# MAGIC   2 DESC;