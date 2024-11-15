# Databricks notebook source
container_names = ["demo", "raw", "processed", "presentation"]

for name in container_names:
  if any(mount.mountPoint == f"/mnt/formula1dllk/{name}" for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(f'/mnt/formula1dllk/{name}')
