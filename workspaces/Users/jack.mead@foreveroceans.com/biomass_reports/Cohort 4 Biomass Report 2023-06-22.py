# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Cohort 4 (SR-C0421Mx, C04A) Manual Biomass Samplings Report
# MAGIC
# MAGIC Within this report contains an anaylsis of Cohorts 4 from 2022 biomass sampling measurements.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Sampling Frequency
# MAGIC
# MAGIC Since FiveTrain was connected (the data ingestion tool used to ingest data points from Aquamanager into the Datalake) there has not been a new measurement inserted since February of 2023. Below is a transaction log of all the new data points that Fivetran has detected.
# MAGIC
# MAGIC The bar graph below is a general overview of all the recording manual biomass measurements. Recorded is the total number of samples per date grouped by the cohort.

# COMMAND ----------

# MAGIC %sql WITH t AS (
# MAGIC     DESCRIBE HISTORY hive_metastore.aquamanager_growout_dbo.cltranssampledetails
# MAGIC ) SELECT to_date(timestamp, 'yyyy-MM-dd'), operation, operationMetrics.numOutputRows as num_output_rows FROM t

# COMMAND ----------

import seaborn as sn, matplotlib.pyplot as plt, numpy as np, pandas as pd

def select_samplings(cohort_id, start_date, end_date):
    return spark.sql(f"""
SELECT
    sampleweight as sampled_weight, batch, to_date(date, 'MM-dd-yy') as date
FROM
    hive_metastore.aquamanager_growout_dbo.view_sampling_details
WHERE 
    batch = '{cohort_id}' AND to_date(date, 'MM-dd-yy') BETWEEN to_date('{start_date}', 'yyyy-MM-dd') AND to_date('{end_date}', 'yyyy-MM-dd')
ORDER BY
    date
ASC              
    """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   count(sampleweight) as total_sampled,
# MAGIC   batch,
# MAGIC   to_date(date, 'MM-dd-yy') as date
# MAGIC FROM
# MAGIC   hive_metastore.aquamanager_growout_dbo.view_sampling_details
# MAGIC GROUP BY
# MAGIC   date, batch
# MAGIC ORDER BY 
# MAGIC   date
# MAGIC ASC    

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Histograms
# MAGIC
# MAGIC Starting on 2021-11-22 and ending in 2023-03-11, below is a 30, 60, 90 day analysis of the Cohort's sampling histograms.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## First 30 Days Post Stocking
# MAGIC
# MAGIC Samples were recorded on December 30th, after 30 days.

# COMMAND ----------

display(select_samplings('SR-C0421Mx', '2021-12-01', '2022-01-01'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 60 Days Post Stocking
# MAGIC
# MAGIC Samples were recorded on January 27th.

# COMMAND ----------

display(select_samplings('SR-C0421Mx', '2022-01-01', '2022-01-31'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 90 Days Post Stocking

# COMMAND ----------

display(select_samplings('SR-C0421Mx', '2022-02-01', '2022-02-28'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 60-90 Days Post Stocking: 2022-03-22 to 2022-06-22

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   sampleweight as sampled_weight, batch, to_date(date, 'MM-dd-yy') as date
# MAGIC FROM
# MAGIC   hive_metastore.aquamanager_growout_dbo.view_sampling_details
# MAGIC WHERE 
# MAGIC   batch = 'SR-C0421Mx' AND to_date(date, 'MM-dd-yy') BETWEEN to_date('2022-03-22', 'yyyy-MM-dd') AND to_date('2022-06-22', 'yyyy-MM-dd')
# MAGIC ORDER BY
# MAGIC   date
# MAGIC ASC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 90-120 Days Post Stocking: 2022-06-22 to 2022-09-22

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   sampleweight as sampled_weight, batch, to_date(date, 'MM-dd-yy') as date
# MAGIC FROM
# MAGIC   hive_metastore.aquamanager_growout_dbo.view_sampling_details
# MAGIC WHERE 
# MAGIC   batch = 'SR-C0421Mx' AND to_date(date, 'MM-dd-yy') BETWEEN to_date('2022-06-22', 'yyyy-MM-dd') AND to_date('2022-09-22', 'yyyy-MM-dd')
# MAGIC ORDER BY
# MAGIC   date
# MAGIC ASC
