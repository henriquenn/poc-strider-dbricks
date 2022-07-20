# Databricks notebook source
dbutils.fs.unmount("/mnt/processed") 

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "ddbbcac7-fa27-40f7-a90a-12fd99c672df",
           "fs.azure.account.oauth2.client.secret": "pwP8Q~dX76Dar-4.XiQOfIADh.GqIvcvG9Jocb9x",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/7e3c64db-b450-4a57-8855-2a8b6060a522/oauth2/token"}

dbutils.fs.mount(
  source = "abfss://processed@sastrider.dfs.core.windows.net/",
  mount_point = "/mnt/processed",
  extra_configs = configs)

# COMMAND ----------

df = spark.read.parquet("dbfs:/mnt/processed/movies/dt=20220719/movies-00001.parquet")
display(df)

dbutils.fs.ls("/mnt/processed")

#dbutils.fs.unmount("/mnt/processed") 
