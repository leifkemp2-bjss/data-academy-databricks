# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# SQL method
# races_filtered_df = races_df.filter("race_year = 2019 and round <= 5")
# Pythonic method, each condition has to be wrapped in brackets and use & to join them, rather than using and
races_filtered_df = races_df.filter((races_df.race_year == 2019) & (races_df.round <= 5))

# COMMAND ----------

display(races_filtered_df)
