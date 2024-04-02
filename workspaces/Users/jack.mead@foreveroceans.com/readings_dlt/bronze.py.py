# Databricks notebook source
import dlt
import json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pyspark.sql.functions as f

kafka_brokers = spark.conf.get('spark.custom.bronze.kafka_brokers')
consume_topic = spark.conf.get('spark.custom.bronze.consume_topic')
table_name = spark.conf.get('spark.custom.bronze.table_name')
table_path = spark.conf.get('spark.custom.bronze.table_path')

reading_schema = StructType([
    StructField("farmCode", StringType(), True),
    StructField("siteId", StringType(), True),
    StructField("shortName", StringType(), True),
    StructField("assetShortName", StringType(), True),
    StructField("value", DoubleType(), False),
    StructField("timestamp", StringType(), True),
])

# COMMAND ----------

@dlt.table(
    name=table_name,
    comment="""
    Raw C2V2 Sensor Data

    This table contains every single data point used by the C2V2 system.
    Data is organized by farm_code, site_id, asset_short_name, and reading_short_name.
    This table replaces the original "readings" table with better optimization.
    """,
    path=table_path,
    partition_cols=["site_id", "year", "month", "day"],
    spark_conf={
        'spark.sql.session.timeZone': 'UTC'
    },
    table_properties={
        'pipelines.autoOptimize.managed': 'true',
        'pipelines.autoOptimize.zOrderCols': 'timestamp',
        'pipelines.reset.allowed': 'false'
    }
)
def bronze_data():
    df = spark \
        .readStream \
        .format('kafka') \
        .option("kafka.bootstrap.servers", kafka_brokers)\
        .option("subscribe", consume_topic)\
        .option("startingOffsets", "earliest")\
        .option("failOnDataLoss", "false")\
        .load()
    
    df = df \
        .selectExpr("CAST(value AS STRING)") \
        .withColumn('value', f.from_json(f.col('value'), reading_schema))\
        .select('value.*')

    df = df.withColumn('timestamp', f.to_timestamp(f.col('timestamp')))
    df = df.withColumn('year', f.year('timestamp'))
    df = df.withColumn('month', f.month('timestamp'))
    df = df.withColumn('day', f.dayofmonth('timestamp'))

    df = df.selectExpr(
        "timestamp",
        "year",
        "month",
        "day",
        "farmCode as farm_code",
        "siteId as site_id",
        "assetShortName as asset_short_name",
        "shortName as reading_short_name",
        "value"
    )

    return df
