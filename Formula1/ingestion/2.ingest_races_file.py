# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Ingest the data and apply schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

races_schema = StructType(fields=[
  StructField("raceId", IntegerType(), False),
  StructField("year", IntegerType(), True),
  StructField("round", IntegerType(), True),
  StructField("circuitId", IntegerType(), True),
  StructField("name", StringType(), True),
  StructField("date", DateType(), True),
  StructField("time", StringType(), True),
  StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read.schema(races_schema).csv(f"{raw_folder_path}/races.csv", header=True)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Select only necessary columns and apply aliases

# COMMAND ----------

from pyspark.sql.functions import col
races_selected_df = races_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"), col("name"), col("date"), col("time"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Transform the data - merging date and time into a timestamp column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, date_format, lit

races_ingestion_date_df = add_ingestion_date(races_selected_df)
races_transformed_df = races_ingestion_date_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(" "), col('time')), 'yyyy-MM-dd HH:mm:ss'))


# COMMAND ----------

races_final_df = races_transformed_df.select(col("race_id"), col("race_year"), col("round"), col("circuit_id"), col("name"), col("race_timestamp"), col("ingestion_date")).withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write to data lake

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_processed.races

# COMMAND ----------

# races_final_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/races")
races_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")
# mode("overwrite") is necessary to prevent the code from producing an error over the data already existing

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dllk/processed/races

# COMMAND ----------

df = spark.read.load("/mnt/formula1dllk/processed/races")
display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
