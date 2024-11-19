# Databricks notebook source
# MAGIC %md
# MAGIC Upsert using merge

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

drivers_day1_df = spark.read \
  .option("inferSchema", True) \
  .json("/mnt/formula1dllk/raw/drivers.json") \
  .filter("driverId <= 10") \
  .select("driverId", "dob", col("name.forename").alias("forename"), col("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

drivers_day2_df = spark.read \
  .option("inferSchema", True) \
  .json("/mnt/formula1dllk/raw/drivers.json") \
  .filter("driverId BETWEEN 6 and 15") \
  .select("driverId", "dob", col("name.forename").alias("forename"), col("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

drivers_day3_df = spark.read \
  .option("inferSchema", True) \
  .json("/mnt/formula1dllk/raw/drivers.json") \
  .filter("driverId BETWEEN 1 AND 5 or driverId BETWEEN 16 and 20") \
  .select("driverId", "dob", col("name.forename").alias("forename"), col("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC               tgt.forename = upd.forename,
# MAGIC               tgt.surname = upd.surname,
# MAGIC               tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC               tgt.forename = upd.forename,
# MAGIC               tgt.surname = upd.surname,
# MAGIC               tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS f1_demo.drivers_merge;
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dllk/demo/drivers_merge") 

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename" : "upd.forename", 
      "surname" : "upd.surname", 
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------


