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

races_df = spark.read.parquet(f"{processed_folder_path}/races")
results_df = spark.read.parquet(f"{processed_folder_path}/results")
drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")
constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")

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

# MAGIC %md
# MAGIC #### Then join with the results dataframe

# COMMAND ----------

races_circuit_results_df = races_circuit_df.join(results_renamed_df, races_circuit_df.race_id == results_renamed_df.race_id) \
                                          .select(races_circuit_df["*"], results_renamed_df.driver_id, results_renamed_df.constructor_id, results_renamed_df.grid, results_renamed_df.fastest_lap, results_renamed_df.race_time, results_renamed_df.points) \
                                            # .orderBy(results_df.points.desc())

races_circuit_results_df = races_circuit_results_df.drop("race_id") # drop race id as we no longer need it
display(compareAgainstADGP(races_circuit_results_df))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Then join with the drivers dataframe

# COMMAND ----------

races_circuit_results_drivers_df = races_circuit_results_df \
  .join(drivers_renamed_df, results_df.driver_id == drivers_renamed_df.driver_id) \
  .select(races_circuit_results_df["*"], drivers_renamed_df.driver_name, drivers_renamed_df.driver_number, drivers_renamed_df.driver_nationality)

races_circuit_results_drivers_df = races_circuit_results_drivers_df.drop("driver_id") # drop race id as we no longer need it
display(compareAgainstADGP(races_circuit_results_drivers_df))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Then join with the constructors table

# COMMAND ----------

all_tables_df = races_circuit_results_drivers_df \
  .join(constructors_renamed_df, results_df.constructor_id == constructors_renamed_df.constructor_id) \
  .select(races_circuit_results_drivers_df["*"], constructors_renamed_df.team)

all_tables_df = all_tables_df.drop("constructor_id")
display(compareAgainstADGP(all_tables_df))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create the final table, with columns in right order and created_date column

# COMMAND ----------

final_df = all_tables_df \
    .select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points") \
    .withColumn("created_date", current_timestamp()) \
    .orderBy("points", ascending=False) \
    

# display(compareAgainstADGP(final_df))
display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write everything to a parquet file in the Presentation container

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(compareAgainstADGP(spark.read.parquet(f"{presentation_folder_path}/race_results")))
