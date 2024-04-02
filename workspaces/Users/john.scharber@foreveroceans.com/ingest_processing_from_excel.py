# Databricks notebook source
pip install pandas xlrd openpyxl


# COMMAND ----------

from pyspark.sql import SparkSession
import pandas

file_path = "/Workspace/Users/john.scharber@foreveroceans.com/biomass trueup/Harvest Data.xlsx"
column_names = [
    'date',

    'count2To4',
    'count4To6',
    'count6Plus',

    'finalWeightLbs2To4',
    'finalWeightLbs4To6',
    'finalWeightLbs6Plus',

    'weight2To4',
    'weight4To6',
    'weight6Plus',

    'avgWeight2To4',
    'avgWeight4To6',
    'avgWeight6Plus',

    'cohort']

spark = SparkSession.builder.appName("Test").getOrCreate()
pdf = pandas.read_excel(file_path)
df = spark.createDataFrame(pdf)

df.write.mode("overwrite")\
    .format("delta")\
    .option("mergeSchema", "true")\
    .partitionBy("cohort", "date")\
    .saveAsTable("hive_metastore.default.harvetProcessingData")

# COMMAND ----------



