# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read multiple CSV files and apply schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

lap_times_schema = StructType(
  fields = [
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
  ]
)

# COMMAND ----------

# lap_times_df = spark.read.csv("/mnt/formula1dllk/raw/lap_times/", schema=lap_times_schema)
lap_times_df = spark.read.csv(f"{raw_folder_path}/lap_times/lap_times_split*.csv", schema=lap_times_schema) # Multiple files that match the format in case folder contains many different files

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename and add columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

lap_times_ingestion_date_df = add_ingestion_date(lap_times_df)

lap_times_final_df = lap_times_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
                                  .withColumnRenamed("raceId", "race_id") \
                                  .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write to output

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_processed.lap_times

# COMMAND ----------

# lap_times_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")
lap_times_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.lap_times")


# COMMAND ----------

dbutils.notebook.exit("Success")
