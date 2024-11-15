# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 3. Call file system utlity mount to mount the storage
# MAGIC 4. Explore other file system utlities related to mount (list all mounts, unmount)

# COMMAND ----------

# client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
# tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')
# client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret')

# COMMAND ----------

# This method uses WASBS rather than ABFSS as I do not have access to Entra ID to create a service principal, and is not the right practice
def mount_adls(storage_account_name, container_name):   
    account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dllk-account-key')

    # If a mount already exists on the container, unmount it and the re-mount it
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
      dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    dbutils.fs.mount(
      source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": account_key}
    )

    display(dbutils.fs.mounts())

# COMMAND ----------

container_names = ["demo", "raw", "processed", "presentation"]
for name in container_names:
  mount_adls("formula1dllk", name)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dllk/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dllk/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# for name in container_names:
#   if any(mount.mountPoint == f"/mnt/formula1dllk/{name}" for mount in dbutils.fs.mounts()):
#     dbutils.fs.unmount(f'/mnt/formula1dllk/{name}')

# COMMAND ----------


