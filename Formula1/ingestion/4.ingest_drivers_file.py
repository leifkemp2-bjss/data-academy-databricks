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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(
  fields = [
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
  ]
)

drivers_schema = StructType(
  fields = [
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema, True),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
  ]
)

# COMMAND ----------

drivers_df = spark.read.json(f"{raw_folder_path}/drivers.json", schema=drivers_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Step](path) 2 - Rename and add columns

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_ingestion_date_df = add_ingestion_date(drivers_df)

drivers_with_columns_df = drivers_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drop columns

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write to output

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_processed.drivers

# COMMAND ----------

# drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")
drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")


# COMMAND ----------

dbutils.notebook.exit("Success")
