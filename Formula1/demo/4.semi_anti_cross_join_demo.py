# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
  .withColumnRenamed("name", "race_name")
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
  .withColumnRenamed("name", "circuit_name").filter("circuit_id < 70")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

races_circuits_semi_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi") # gets the contents of a join but only returns the left table


# COMMAND ----------

display(races_circuits_semi_df)

# COMMAND ----------

races_circuits_anti_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti") # does the opposite of a semi join

# COMMAND ----------

display(races_circuits_anti_df)

# COMMAND ----------

races_circuits_cross_df = circuits_df.crossJoin(races_df)


# COMMAND ----------

display(races_circuits_cross_df)
