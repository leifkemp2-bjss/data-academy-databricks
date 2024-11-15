# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.formula1dllk.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dllk.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dllk.dfs.core.windows.net", dbutils.secrets.get(scope="formula1-scope", key = "formula1dllk-sas-token"))

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dllk.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dllk.dfs.core.windows.net/circuits.csv"))
