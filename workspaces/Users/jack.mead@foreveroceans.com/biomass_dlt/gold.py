# Databricks notebook source
import dlt
import json
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField

silver_table_name = spark.conf.get('spark.custom.gold.silver_table_name')
table_name = spark.conf.get('spark.custom.gold.table_name')
table_path = spark.conf.get('spark.custom.gold.table_path')
watermark = spark.conf.get('spark.custom.gold.watermark')

# COMMAND ----------

@dlt.table(
    name=table_name,
    comment="""
    Daily AI Biomass Aggregations

    This dataset contains daily aggregations produced from the biomass_filtered table.
    """,
    path=table_path,
    partition_cols=["camera_name", "year", "month", "day"],
    spark_conf={
        'spark.sql.sources.partitionOverwriteMode': 'dynamic',
        'spark.sql.session.timeZone': 'UTC'
    },
    table_properties={
        'pipelines.autoOptimize.managed': 'true',
        'pipelines.autoOptimize.zOrderCols': 'model_version'
    }
)
def gold_data():
    df = spark\
        .read\
        .format('delta')\
        .table(silver_table_name)
    
    df = df.withColumn('date', f.to_date(f.col('timestamp')))
    df = df.filter(f'date >= date_sub(current_date(), {watermark})')

    df = df\
        .groupBy(
            f.col('camera_name'),
            f.col('model_version'),
            f.window('timestamp', '1 day')
        )\
        .agg(
            f.count_distinct('unique_key').alias('instances_detected'),
    
            f.percentile_approx('offset_weight', 0.5).alias('median_offset_weight'),
            f.stddev('offset_weight').alias('std_offset_weight'),
            f.mean('offset_weight').alias('avg_offset_weight'),
            f.min('offset_weight').alias('min_offset_weight'),
            f.max('offset_weight').alias('max_offset_weight'),
        
            f.percentile_approx('weight', 0.5).alias('median_weight'),
            f.stddev('weight').alias('std_weight'),
            f.mean('weight').alias('avg_weight'),
            f.min('weight').alias('min_weight'),
            f.max('weight').alias('max_weight'),
        
            f.percentile_approx('avg_depth', 0.5).alias('median_depth'),
            f.stddev('avg_depth').alias('std_depth'),
            f.mean('avg_depth').alias('avg_depth'),
            f.min('avg_depth').alias('min_depth'),
            f.max('avg_depth').alias('max_depth'),
        
            f.percentile_approx('length', 0.5).alias('median_length'),
            f.stddev('length').alias('std_length'),
            f.mean('length').alias('avg_length'),
            f.min('length').alias('min_length'),
            f.max('length').alias('max_length'),
        
            f.percentile_approx('offset_length', 0.5).alias('median_offset_length'),
            f.stddev('offset_length').alias('std_offset_length'),
            f.mean('offset_length').alias('avg_offset_length'),
            f.min('offset_length').alias('min_offset_length'),
            f.max('offset_length').alias('max_offset_length'),
        )
    
    df = df\
        .withColumn('year', f.year('window.start'))\
        .withColumn('month', f.month('window.start'))\
        .withColumn('day', f.dayofmonth('window.start'))\
        .withColumn('date', f.to_date(f.col('window.start')))\
        .drop('window')
    
    return df
    
    
    
