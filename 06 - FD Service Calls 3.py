# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Loading

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Load Data Frame from DBFS
raw_fd_service_calls = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("dbfs:/FileStore/sample_data/fire_department_service_calls_25k.csv")
)

display(raw_fd_service_calls)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Sanitisation

# COMMAND ----------

# MAGIC %md
# MAGIC Note: Spark data frames are immutable, hence new data frames are used to store transformed data.

# COMMAND ----------

# DBTITLE 1,Update Column Names
renamed_fd_service_calls = (
    raw_fd_service_calls
    .withColumnRenamed("Call Number", "call_number")
    .withColumnRenamed("Unit ID", "unit_id")
    .withColumnRenamed("Incident Number", "incident_number")
    .withColumnRenamed("Call Type", "call_type")
    .withColumnRenamed("Call Date", "call_date")
    .withColumnRenamed("Watch Date", "watch_date")
    .withColumnRenamed("Received DtTm", "received_ts")
    .withColumnRenamed("Entry DtTm", "entry_ts")
    .withColumnRenamed("Dispatch DtTm", "dispatch_ts")
    .withColumnRenamed("Response DtTm", "response_ts")
    .withColumnRenamed("On Scene DtTm", "on_scene_ts")
    .withColumnRenamed("Transport DtTm", "transport_ts")
    .withColumnRenamed("Hospital DtTm", "hospital_ts")
    .withColumnRenamed("Call Final Disposition", "call_final_disposition")
    .withColumnRenamed("Available DtTm", "available_ts")
    .withColumnRenamed("Address", "address")
    .withColumnRenamed("City", "city")
    .withColumnRenamed("Zipcode of Incident", "zip_code_of_incident")
    .withColumnRenamed("Battalion", "battalion")
    .withColumnRenamed("Station Area", "station_area")
    .withColumnRenamed("Box", "box")
    .withColumnRenamed("Original Priority", "original_priority")
    .withColumnRenamed("Priority", "priority")
    .withColumnRenamed("Final Priority", "final_priority")
    .withColumnRenamed("ALS Unit", "als_unit")
    .withColumnRenamed("Call Type Group", "call_type_group")
    .withColumnRenamed("Number of Alarms", "number_of_alarms")
    .withColumnRenamed("Unit Type", "unit_type")
    .withColumnRenamed(
        "Unit sequence in call dispatch", "unit_sequence_in_call_dispatch"
    )
    .withColumnRenamed("Fire Prevention District", "fire_prevention_district")
    .withColumnRenamed("Supervisor District", "supervisor_district")
    .withColumnRenamed(
        "Neighborhooods - Analysis Boundaries", "neighborhood_analysis_boundaries"
    )
    .withColumnRenamed("RowID", "row_id")
    .withColumnRenamed("case_location", "case_location")
    .withColumnRenamed("data_as_of", "data_as_of")
    .withColumnRenamed("data_loaded_at", "data_loaded_on")
    .withColumnRenamed("Analysis Neighborhoods", "analysis_of_neighborhoods")
)

display(renamed_fd_service_calls)

# COMMAND ----------

# DBTITLE 1,Check Schema of Data Frame
renamed_fd_service_calls.printSchema()

# COMMAND ----------

# DBTITLE 1,Update Data Type of Date and Time Columns
fd_service_calls = (
    renamed_fd_service_calls.withColumn("call_date", to_date("call_date", "MM/dd/yyyy"))
    .withColumn("watch_date", to_date("watch_date", "MM/dd/yyyy"))
    .withColumn("received_ts", to_timestamp("received_ts", "MM/dd/yyyy hh:mm:ss a"))
    .withColumn("entry_ts", to_timestamp("entry_ts", "MM/dd/yyyy hh:mm:ss a"))
    .withColumn("dispatch_ts", to_timestamp("dispatch_ts", "MM/dd/yyyy hh:mm:ss a"))
    .withColumn("response_ts", to_timestamp("response_ts", "MM/dd/yyyy hh:mm:ss a"))
    .withColumn("on_scene_ts", to_timestamp("on_scene_ts", "MM/dd/yyyy hh:mm:ss a"))
    .withColumn("transport_ts", to_timestamp("transport_ts", "MM/dd/yyyy hh:mm:ss a"))
    .withColumn("hospital_ts", to_timestamp("hospital_ts", "MM/dd/yyyy hh:mm:ss a"))
    .withColumn("available_ts", to_timestamp("available_ts", "MM/dd/yyyy hh:mm:ss a"))
)

fd_service_calls.printSchema()

# COMMAND ----------

# DBTITLE 1,Validate Data in Updated Columns
display(fd_service_calls)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Analysis

# COMMAND ----------

# DBTITLE 1,Cache Main Data Frame
# Since the data frame is going to queried multiple times, it makes sense to cache it.
fd_service_calls.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC **Question 1:** How many distinct types of calls were made to the FD?
# MAGIC

# COMMAND ----------

# DBTITLE 1,Answer to Question 1
# Here, the 'where()', 'select()', and 'distinct()' functions are transformations.
query_output = (
    fd_service_calls.where("call_type IS NOT NULL").select("call_type").distinct()
)

# Here, the 'count()' function is an action, which doesn't return a data frame.
# An action kicks off a Spark job, returns the results to the driver node and then to the user.
print(query_output.count())

# COMMAND ----------

# MAGIC %md
# MAGIC **Question 2:** What were distinct types of calls made to the FD?
# MAGIC

# COMMAND ----------

# DBTITLE 1,Answer to Question 2
query_output = (
    fd_service_calls.where("call_type IS NOT NULL")
    .select(expr("call_type AS distinct_call_type"))
    .distinct()
)

display(query_output)

# COMMAND ----------

# MAGIC %md
# MAGIC **Question 3:** Which are the responses where the original priority less than 2?
# MAGIC

# COMMAND ----------

# DBTITLE 1,Answer to Question 3
query_output = (
    fd_service_calls.where("original_priority <= 2")
    .select("call_number", "original_priority")
    .orderBy("call_number")
)

display(query_output)

# COMMAND ----------

# MAGIC %md
# MAGIC **Question 4:** What were the most common call types?
# MAGIC

# COMMAND ----------

# DBTITLE 1,Answer to Question 4
# In this DF transformation, the 'count()' function is a transformation.
# It acts on the 'GroupedData' object returned by the 'groupBy()' method
query_output = (
    fd_service_calls.where("call_type IS NOT NULL")
    .select("call_type")
    .groupBy("call_type")
    .count()
    .orderBy("count", ascending=False)
)

display(query_output)

# COMMAND ----------

# MAGIC %md
# MAGIC **Question 5:** Which ZIP codes accounted for the most common calls?
# MAGIC

# COMMAND ----------

# DBTITLE 1,Answer to Question 5


# COMMAND ----------

# MAGIC %md
# MAGIC **Question 6:** Which San Francisco neighborhoods are in the ZIP codes 94102 and 94103?
# MAGIC

# COMMAND ----------

# DBTITLE 1,Answer to Question 6


# COMMAND ----------

# MAGIC %md
# MAGIC **Question 7:** What is the total number of call alarms?
# MAGIC

# COMMAND ----------

# DBTITLE 1,Answer to Question 7


# COMMAND ----------

# MAGIC %md
# MAGIC **Question 8:** How many distinct years of data is present in the dataset?
# MAGIC

# COMMAND ----------

# DBTITLE 1,Answer to Question 8


# COMMAND ----------

# MAGIC %md
# MAGIC **Question 9:** Which week of the year 2022 had the most fire calls?
# MAGIC

# COMMAND ----------

# DBTITLE 1,Answer to Question 9


# COMMAND ----------

# MAGIC %md
# MAGIC **Question 10:** Which neighborhoods had the largest difference between reception and dispatch times in 2018?

# COMMAND ----------

# DBTITLE 1,Answer to Question 10
