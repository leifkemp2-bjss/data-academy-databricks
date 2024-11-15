# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
  .withColumnRenamed("name", "race_name")
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").filter("circuit_id < 70") \
  .withColumnRenamed("name", "circuit_name")

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "outer") \
  .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)
races_circuits_l_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
  .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)
races_circuits_r_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
  .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)
# last parameter defaults to inner

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

display(races_circuits_l_df)

# COMMAND ----------

display(races_circuits_r_df)
