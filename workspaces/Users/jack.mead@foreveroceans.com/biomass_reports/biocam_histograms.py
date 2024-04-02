# Databricks notebook source
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy.stats as stats
import calendar

# COMMAND ----------

df_90 = spark.sql("SELECT offset_weight, timestamp FROM biomass_filtered WHERE camera_name = 'biocamsvid90'").toPandas()

df_90['date'] = df_90.timestamp.dt.date
df_90.offset_weight = df_90.offset_weight.astype(float)

df_90

# COMMAND ----------

df_may = df_90[(df_90['date'] > pd.to_datetime('2023-05-01')) & (df_90['date'] < pd.to_datetime('2023-05-31'))]

plt.hist(df_may.offset_weight, bins=20)
plt.show()

df_april = df_90[(df_90['date'] > pd.to_datetime('2023-04-01')) & (df_90['date'] < pd.to_datetime('2023-04-30'))]

plt.hist(df_april.offset_weight, bins=20)
