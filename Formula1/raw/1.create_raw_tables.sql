-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitsId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS (path "/mnt/formula1dllk/raw/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;

CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING CSV
OPTIONS (path "/mnt/formula1dllk/raw/races.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Making JSON

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING json
OPTIONS(path "/mnt/formula1dllk/raw/constructors.json")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
OPTIONS(path "/mnt/formula1dllk/raw/drivers.json")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points INT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId STRING
)
USING json
OPTIONS(path "/mnt/formula1dllk/raw/results.json")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
)
USING json
OPTIONS(path "/mnt/formula1dllk/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;

CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING CSV
OPTIONS (path "/mnt/formula1dllk/raw/lap_times")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;

CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  constructorId INT,
  driverId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING,
  qualifyId INT,
  raceId INT
)
USING JSON
OPTIONS (path "/mnt/formula1dllk/raw/qualifying", multiLine true)

-- COMMAND ----------


