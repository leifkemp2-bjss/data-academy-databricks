# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# The video uses this race as a reference guide, so I'll use this function to compare my results against what is known, and to also produce smaller queries that take less time
def compareAgainstADGP(df):
  output_df = df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'")
  return output_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Retrieve all 5 of the necessary data frames and rename columns as appropriate

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races")
results_df = spark.read.format("delta").load(f"{processed_folder_path}/results")
drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers")
circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")
constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors")

# COMMAND ----------

from pyspark.sql.functions import to_date, current_timestamp

races_renamed_df = races_df.withColumnRenamed("name", "race_name") \
                          .withColumn("race_date", to_date("race_timestamp", "yyyy-MM-dd"))
circuits_renamed_df = circuits_df.withColumnRenamed("name", "circuit_name") \
                                  .withColumnRenamed("location", "circuit_location")
drivers_renamed_df = drivers_df.withColumnRenamed("name", "driver_name") \
                              .withColumnRenamed("number", "driver_number") \
                              .withColumnRenamed("nationality", "driver_nationality")
results_renamed_df = results_df.withColumnRenamed("time", "race_time")
constructors_renamed_df = constructors_df.withColumnRenamed("name", "team")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join the races and circuits tables together

# COMMAND ----------

races_circuit_df = races_renamed_df.join(circuits_renamed_df, races_renamed_df.circuit_id == circuits_renamed_df.circuit_id) \
  .select(races_renamed_df.race_id, races_renamed_df.race_name, races_renamed_df.race_year, races_renamed_df.race_date, circuits_renamed_df.circuit_location)

display(compareAgainstADGP(races_circuit_df))

# COMMAND ----------

race_results_df = results_renamed_df.join(races_circuit_df, results_df.race_id == races_circuit_df.race_id) \
  .join(drivers_renamed_df, results_df.driver_id == drivers_df.driver_id) \
  .join(constructors_renamed_df, results_df.constructor_id == constructors_df.constructor_id)

display(compareAgainstADGP(race_results_df))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create the final table, with columns in right order and created_date column

# COMMAND ----------

final_df = race_results_df \
    .select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position") \
    .withColumn("created_date", current_timestamp()) \
    .orderBy("points", ascending=False) \
    

display(compareAgainstADGP(final_df))
# display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write everything to a parquet file in the Presentation container

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_presentation.race_results

# COMMAND ----------

# races_final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")
final_df.write.mode("overwrite").format("delta").saveAsTable("f1_presentation.race_results")


# COMMAND ----------

display(compareAgainstADGP(spark.read.format("delta").load(f"{presentation_folder_path}/race_results")))
