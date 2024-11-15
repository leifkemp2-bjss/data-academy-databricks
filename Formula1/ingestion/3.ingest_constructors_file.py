# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file

# COMMAND ----------

# defining schema by DDL rather than StructType
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.json(f"{raw_folder_path}/constructors.json", schema=constructors_schema)

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename and drop columns

# COMMAND ----------


constructor_dropped_df = constructor_df.drop("url")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

constructor_ingestion_date_df = add_ingestion_date(constructor_dropped_df)

constructor_final_df = constructor_ingestion_date_df.withColumnRenamed("constructorId", "constructor_id") \
  .withColumnRenamed("constructorRef", "constructor_ref").withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write to Parquet

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}d/constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
