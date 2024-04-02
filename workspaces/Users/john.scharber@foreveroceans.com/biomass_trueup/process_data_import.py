# Databricks notebook source
pip install openpyxl

# COMMAND ----------


from pyspark.sql import SparkSession
import pandas as pd

file_path = "/Workspace/Users/john.scharber@foreveroceans.com/biomass trueup/Harvest Data.xlsx"
column_names = ['row','date','Count2To4','Count4To6,Count6Plus','finalWeightLbs2To4','finalWeightLbs4To6','finalWeightLbs6Plus','weight2To4','weight4To6','weight6Plus','avgWeight2To4','avgWeight4To6','avgWeight6Plus','cohort']

el = pd.read_excel(file_path, header=None, names=column_names)

df = pd.DataFrame(el)

print(df.columns)
print(df.head(4))


display(df.to_string())

#spark_df = spark.createDataFrame(df)


#spark_df.write.mode("overwrite")\
#    .format("delta")\
#    .option("mergeSchema", "true")\
#    .partitionBy("cohort", "date")\
#    .saveAsTable("hive_metastore.default.harvetProcessingData")



# COMMAND ----------



