# Databricks notebook source
# MAGIC %md
# MAGIC ##Environment Assessment
# MAGIC - Assessing current Databricks environment on AWS
# MAGIC - Please Set Cluster Configuration as such:-  
# MAGIC   Policy :Unrestricted |
# MAGIC   Access mode:Shared
# MAGIC - Please set the dapiToken from an Admin user Account, also set the Container and Storag Account Names.
# MAGIC - Please make sure storage is mounted and is accessible using cluster. 
# MAGIC - Also if you are using Cred Pass Through make sure Table Access Controls are enables on workspace level

# COMMAND ----------

import requests

# COMMAND ----------

databricks_host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()

# COMMAND ----------

dbutils.widgets.text("access_token", "Enter dapi token here", "Access Token")
access_token = dbutils.widgets.get("access_token")
# dbutils.widgets.text("FILE_STORE", "Please enter FILE_STORE loc", "FOLDER NAME")
# bucket_name = dbutils.widgets.get("bucket_name")

# COMMAND ----------

if not access_token or access_token.lower() == 'enter dapi token here':
    dbutils.notebook.exit("Access token Empty")

# COMMAND ----------

headers = {"Authorization": f"Bearer {access_token}"}

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import explode_outer

# COMMAND ----------

# storage_path = f"dbfs:/FileStore/Environment_Assessment"

# COMMAND ----------



# COMMAND ----------

try:
    mount_points = dbutils.fs.mounts()
    # print(mount_points)
    mount_points_df = spark.createDataFrame(mount_points)
    display(mount_points_df)
    mount_points_path = f"{storage_path}/mount_points.parquet"
    mount_points_df.write.parquet(mount_points_path, mode="overwrite")
except Exception as e:
    print(e)

# COMMAND ----------



