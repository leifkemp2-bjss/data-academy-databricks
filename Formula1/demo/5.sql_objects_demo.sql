-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo

-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

DROP TABLE IF EXISTS demo.race_results_python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

DESCRIBE race_results_python

-- COMMAND ----------

SELECT * FROM demo.race_results_python WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS
SELECT *
  FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

DROP TABLE IF EXISTS demo.race_results_ext_py

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

describe extended demo.race_results_ext_py

-- COMMAND ----------

DROP TABLE IF EXISTS demo.race_results_ext_sql;

CREATE TABLE demo.race_results_ext_sql 
LOCATION "/mnt/formula1dl/presentation/race_results_ext_sql" AS
  SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

SELECT * FROM demo.race_results_ext_sql

-- COMMAND ----------

DROP DATABASE demo CASCADE
