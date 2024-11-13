# Databricks notebook source
# MAGIC %fs
# MAGIC ls

# COMMAND ----------

dbutils.fs.ls("/") # Using this rather than the fs magic command allows for more flexibility as we can combine this with other Python code

# COMMAND ----------

for files in dbutils.fs.ls('/databricks-datasets/COVID'):
  if files.name.endswith("/"):
    print(files)

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()
