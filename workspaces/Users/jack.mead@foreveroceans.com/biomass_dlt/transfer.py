# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE biomass_aggregations USING DELTA LOCATION '/delta/biomass_aggregations'

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE water_quality OWNER TO `jack.mead@foreveroceans.com`;

# COMMAND ----------

df = spark\
    .read\
    .format('delta')\
    .table('biomassresults')

count = df.drop_duplicates().count()

count

# COMMAND ----------

import pyspark.sql.functions as f

df = spark\
    .read\
    .format('delta')\
    .table('biomassresults')

df = df.withColumn('date', f.to_timestamp(f.from_unixtime(f.col('timestamp') / 1000, 'yyyy-MM-dd')))
df = df.withColumn('year', f.year('date'))
df = df.withColumn('month', f.month('date'))
df = df.withColumn('day', f.dayofmonth('date'))
df = df.drop('date')

df\
    .write\
    .format("delta")\
    .mode("overwrite")\
    .partitionBy("cameraName", "year", "month", "day")\
    .option("overwriteSchema", "true")\
    .save('/delta/biomass_results')


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM water_quality_aggregations

# COMMAND ----------


