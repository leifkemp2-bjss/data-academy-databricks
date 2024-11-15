# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read JSON file and apply schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

pitstops_schema = StructType(
  fields = [
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", DoubleType(), True),
    StructField("milliseconds", IntegerType(), True),
  ]
)

# COMMAND ----------

pitstops_df = spark.read.json(f"{raw_folder_path}/pit_stops.json", schema = pitstops_schema, multiLine = True)

# COMMAND ----------

display(pitstops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename and add columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pitstops_ingestion_date_df = add_ingestion_date(pitstops_df)

pitstops_final_df = pitstops_ingestion_date_df.withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write to output

# COMMAND ----------

pitstops_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dllk/processed/pit_stops"))

# COMMAND ----------

dbutils.notebook.exit("Success")
