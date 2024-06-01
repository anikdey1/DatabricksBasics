-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### DB and Table Creation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note: In Databricks CE, after the cluster is terminated, the cluster VM (compute layer), and the Spark metadata (metadata layer) will be deleted. But the DB directory and files will persist, which will throw up errors when DB and table creation or load statements are executed with a new cluster.

-- COMMAND ----------

-- DBTITLE 1,Drop Extant Database
DROP DATABASE
  IF EXISTS
    sample_data


-- COMMAND ----------

-- DBTITLE 1,Drop Extant Table
DROP TABLE
  IF EXISTS
    sample_data.fd_service_calls

-- COMMAND ----------

-- DBTITLE 1,Delete DB Directory and Files
-- MAGIC %fs
-- MAGIC rm -r "/user/hive/warehouse/sample_data.db/"

-- COMMAND ----------

-- DBTITLE 1,Create a Database
CREATE DATABASE
  IF NOT EXISTS
    sample_data

-- COMMAND ----------

-- DBTITLE 1,Create a Table
CREATE TABLE
	IF NOT EXISTS sample_data.fd_service_calls (
		call_number LONG,
		unit_id STRING,
		incident_number LONG,
		call_type STRING,
		call_date DATE,
		watch_date DATE,
		received_dttm STRING,
		entry_dttm STRING,
		dispatch_dttm STRING,
		response_dttm STRING,
		on_scene_dttm STRING,
		transport_dttm STRING,
		hospital_dttm STRING,
		call_final_disposition STRING,
		available_dttm STRING,
		address STRING,
		city STRING,
		zipcode_of_incident STRING,
		battalion STRING,
		station_area STRING,
		box STRING,
		original_priority STRING,
		priority STRING,
		final_priority STRING,
		als_unit BOOLEAN,
		call_type_group STRING,
		number_of_alarms INTEGER,
		unit_type STRING,
		unit_sequence_in_call_dispatch INTEGER,
		fire_prevention_district STRING,
		supervisor_district STRING,
		neighborhooods_analysis_boundaries STRING,
		rowid STRING,
		case_location STRING,
		data_as_of STRING,
		data_loaded_at STRING,
		analysis_neighborhoods INTEGER
	) USING PARQUET

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Data Ingestion

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note: In order to load the temporary view, please run the [FD Service Calls 1](https://community.cloud.databricks.com/?o=2803773821482952#notebook/2125387215316563/command/2125387215316564) notebook.

-- COMMAND ----------

-- DBTITLE 1,Load Table from Temporary View
INSERT
	INTO sample_data.fd_service_calls
		SELECT
			*
		FROM
			global_temp.fd_service_calls_temporary_view

-- COMMAND ----------

-- DBTITLE 1,Query the Newly Loaded Table
SELECT
  *
FROM
  sample_data.fd_service_calls

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Data Analysis

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Question 1:** How many distinct types of calls were made to the FD?

-- COMMAND ----------

-- DBTITLE 1,Answer to Question 1
SELECT
  COUNT(DISTINCT call_type) AS distinct_call_type_count
FROM
  sample_data.fd_service_calls
WHERE
  call_type IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Question 2:** What were distinct types of calls made to the FD?

-- COMMAND ----------

-- DBTITLE 1,Answer to Question 2
SELECT
  DISTINCT call_type AS distinct_call_types
FROM
  sample_data.fd_service_calls
WHERE
  call_type IS NOT NULL
ORDER BY
  1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Question 3:** Which are the responses where the original priority less than 2?

-- COMMAND ----------

-- DBTITLE 1,Answer to Question 3
SELECT
  call_number,
  original_priority
FROM
  sample_data.fd_service_calls
WHERE
  original_priority <= 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Question 4:** What were the most common call types?

-- COMMAND ----------

-- DBTITLE 1,Answer to Question 4
SELECT
  call_type,
  COUNT(call_type) AS call_type_count
FROM
  sample_data.fd_service_calls
WHERE
  call_type IS NOT NULL
GROUP BY
  call_type
ORDER BY
  2 DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC **Question 5:** Which ZIP codes accounted for the most common calls?

-- COMMAND ----------

-- DBTITLE 1,Answer to Question 5
SELECT
  call_type,
  zipcode_of_incident AS zip_code_of_incident,
  COUNT(call_type) AS call_type_count
FROM
  sample_data.fd_service_calls
WHERE
  call_type IS NOT NULL
  AND zipcode_of_incident IS NOT NULL
GROUP BY
  call_type,
  zipcode_of_incident
ORDER BY
  2,
  1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC **Question 6:** Which San Francisco neighborhoods are in the ZIP codes 94102 and 94103?

-- COMMAND ----------

-- DBTITLE 1,Answer to Question 6
SELECT
  zipcode_of_incident AS zip_code,
  neighborhooods_analysis_boundaries AS neighbourhood
FROM
  sample_data.fd_service_calls
WHERE
  zipcode_of_incident IN ('94102', '94103')
GROUP BY
  zipcode_of_incident,
  neighborhooods_analysis_boundaries
ORDER BY
  1, 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC **Question 7:** What is the total number of call alarms?

-- COMMAND ----------

-- DBTITLE 1,Answer to Question 7
SELECT
  SUM(number_of_alarms) AS total_alarm_sum
FROM
  sample_data.fd_service_calls

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC **Question 8:** How many distinct years of data is present in the dataset?

-- COMMAND ----------

-- DBTITLE 1,Answer to Question 8
SELECT
  DISTINCT YEAR(TO_DATE(call_date, 'MM/yyyy/dd')) AS distinct_years
FROM
  sample_data.fd_service_calls
ORDER BY
  1 DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC **Question 9:** Which week of the year 2022 had the most fire calls?

-- COMMAND ----------

-- DBTITLE 1,Answer to Question 9
SELECT
  WEEKOFYEAR(TO_DATE(call_date, 'MM/yyyy/dd')) AS week_number,
  COUNT(WEEKOFYEAR(TO_DATE(call_date, 'MM/yyyy/dd'))) AS call_count
FROM
  sample_data.fd_service_calls
WHERE
  YEAR(TO_DATE(call_date, 'MM/yyyy/dd')) = 2022
GROUP BY
  WEEKOFYEAR(TO_DATE(call_date, 'MM/yyyy/dd'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Question 10:** Which calls have the the largest delay between reception and dispatch times?

-- COMMAND ----------

SELECT
  call_number,
  TO_TIMESTAMP(received_dttm, 'MM/dd/yyyy hh:mm:ss a') AS reception_ts,
  TO_TIMESTAMP(dispatch_dttm, 'MM/dd/yyyy hh:mm:ss a') AS dispatch_ts,
  ROUND(((UNIX_TIMESTAMP(dispatch_ts) - UNIX_TIMESTAMP(reception_ts)) / 60), 2) AS response_time_in_minutes
FROM
  sample_data.fd_service_calls
WHERE
  received_dttm IS NOT NULL
  AND dispatch_dttm IS NOT NULL
ORDER BY
  response_time_in_minutes DESC
LIMIT
  50