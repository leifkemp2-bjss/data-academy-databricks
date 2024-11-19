# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - read the CSV file using spark

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[
  StructField("circuitId", IntegerType(), False),
  StructField("circuitRef", StringType(), True),
  StructField("name", StringType(), True),
  StructField("location", StringType(), True),
  StructField("country", StringType(), True),
  StructField("lat", DoubleType(), True),
  StructField("long", DoubleType(), True),
  StructField("alt", DoubleType(), True),
  StructField("url", StringType(), True)
])

# COMMAND ----------

# Infer schema is good for small amounts of data that never changes types/breaks
# circuits_df = spark.read.csv("/mnt/formula1dllk/raw/circuits.csv", header=True, inferSchema=True)
circuits_df = spark.read.schema(circuits_schema).csv(f"{raw_folder_path}/circuits.csv", header=True)

# COMMAND ----------

# display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Select only the required columns

# COMMAND ----------

# circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "last", "long", "alt")
# circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.last, circuits_df.long, circuits_df.alt)

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("long"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename the columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
                                          .withColumnRenamed("circuitRef", "circuit_ref") \
                                          .withColumnRenamed("lat", "latitude") \
                                          .withColumnRenamed("long", "longitude") \
                                          .withColumnRenamed("alt", "altitude") \
                                          .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Add ingestion date column

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Write to file system as Parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_processed.circuits

# COMMAND ----------

# circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")
# circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")
# mode("overwrite") is necessary to prevent the code from producing an error over the data already existing

# COMMAND ----------

dbutils.notebook.exit("Success")
