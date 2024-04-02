# Databricks notebook source
import dlt
import pyspark.sql.functions as f

watermark = spark.conf.get('spark.custom.silver.watermark')
bronze_table_name = spark.conf.get('spark.custom.silver.bronze_table_name')
table_name = spark.conf.get('spark.custom.silver.table_name')
table_path = spark.conf.get('spark.custom.silver.table_path')

# COMMAND ----------

@dlt.table(
    name=table_name,
    comment="""
        C2 Water Quality Sensor Data from Panama Avalon Operations

        All data points for water quality sensors: Exo Sonde YSI, Manta Sonde, and the TChain sensor.
        This dataset is optimized by cage location (site_id) the year, month, and day. It contains
        data from the sensors for the Avalon systems in Panama. This dataset is scheduled to update every day
    """,
    path=table_path,
    partition_cols=["site_id", "year", "month", "day"],
    spark_conf={
        'spark.sql.sources.partitionOverwriteMode': 'dynamic',
        'spark.sql.session.timeZone': 'UTC'
    },
    table_properties={
        'pipelines.autoOptimize.managed': 'true',
        'pipelines.autoOptimize.zOrderCols': 'sensor,timestamp',
        'pipelines.reset.allowed': 'false'
    }
)
def silver_table():
    df = spark\
        .read\
        .format('delta')\
        .table(bronze_table_name)

    # df = df.filter("`asset_short_name` IN ('tchain', 'ysi1', 'ysi2', 'ysi3', 'tritonswq01')")
    df = df.filter("`assetShortName` IN ('tchain', 'ysi1', 'ysi2', 'ysi3', 'tritonswq01')")

    # df = df.withColumn('date', f.to_date(f.col('timestamp')))

    df = df.filter(f'date >= date_sub(current_date(), {watermark})')

    df = df.drop_duplicates()

    df = df.withColumn('timestamp', f.to_timestamp(f.from_unixtime(f.col('timestamp'))))\
        .withColumn('year', f.year('timestamp'))\
        .withColumn('month', f.month('timestamp'))\
        .withColumn('day', f.dayofmonth('timestamp'))

    # df = df.drop_duplicates()

    return df.selectExpr(
        # 'asset_short_name as sensor',
        # 'reading_short_name as reading',
        # 'farm_code',
        # 'site_id',
        'assetShortName as sensor',
        'shortName as reading',
        'farmCode as farm_code',
        'siteId as site_id',
        'value',
        'timestamp',
        'year',
        'month',
        'day'
    )

