# Databricks notebook source
containername = "customcontainer"
storagename = "East-US-Providing"
mountpoint = "/mnt/custommount"

try:
    dbutils.fs.mount(source = "wasbs://" + containername + "@" + storaganame + ".blob.core.windows.net",
                    mount_point = mountpoint,
                    extra_configs = {"fs.azure.account.key." + storagename + ".blob.core.windows.net":dbutils.secrets.get(scope = "dbstoragescope", key = "secret")}
except Exception as e:
    print("Already mounted, please unmount using dbutils.fs.unmount")

# COMMAND ----------

if any(mount.mountPoint == mountpoint for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount("/mnt/custommount")

# COMMAND ----------

