# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Cohort 4 (SR-C0522MxEc, C05A) Manual Biomass Samplings Report
# MAGIC
# MAGIC Within this report contains an anaylsis of Cohorts 5 from 2022 biomass sampling measurements.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Histograms
# MAGIC
# MAGIC Starting on 2022-03-20 and still in production, below is a 30, 60, 90 day analysis of the Cohort's sampling histograms.
# MAGIC
# MAGIC In the first 30 days, we see two distintive populations with the 480 data points observed.
# MAGIC
# MAGIC Post 30 days, we still continue to see two distintive populations with 1,499 data points observed.
# MAGIC
# MAGIC Post 60 days, we see a much denser population than previously that is both left and right leaning with 498 data points observed.
# MAGIC
# MAGIC Post 90 days, we see a denser population yet that is more normal.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## First 30 Days Post Stocking: 2022-03-20 to 2022-04-22

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   sampleweight as sampled_weight, batch, to_date(date, 'MM-dd-yy') as date
# MAGIC FROM
# MAGIC   hive_metastore.aquamanager_growout_dbo.view_sampling_details
# MAGIC WHERE 
# MAGIC   batch = 'SR-C0522MxEc' AND to_date(date, 'MM-dd-yy') BETWEEN to_date('2022-03-20', 'yyyy-MM-dd') AND to_date('2022-04-22', 'yyyy-MM-dd')
# MAGIC ORDER BY
# MAGIC   date
# MAGIC ASC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 30-60 Days Post Stocking: 2022-01-22 to 2022-03-22

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   sampleweight as sampled_weight, batch, to_date(date, 'MM-dd-yy') as date
# MAGIC FROM
# MAGIC   hive_metastore.aquamanager_growout_dbo.view_sampling_details
# MAGIC WHERE 
# MAGIC   batch = 'SR-C0421Mx' AND to_date(date, 'MM-dd-yy') BETWEEN to_date('2022-01-22', 'yyyy-MM-dd') AND to_date('2022-03-22', 'yyyy-MM-dd')
# MAGIC ORDER BY
# MAGIC   date
# MAGIC ASC

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
