# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="formula1-scope")

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.formula1dllk.dfs.core.windows.net", dbutils.secrets.get(scope = "formula1-scope", key = "formula1dllk-account-key")
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dllk.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dllk.dfs.core.windows.net/circuits.csv"))
