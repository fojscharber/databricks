# Databricks notebook source
import dlt
import pyspark.sql.functions as f

watermark = spark.conf.get('spark.custom.silver.waterquality.watermark')
bronze_table_name = spark.conf.get('spark.custom.silver.waterquality.bronze_table_name')
table_name = spark.conf.get('spark.custom.silver.waterquality.table_name')
table_path = spark.conf.get('spark.custom.silver.waterquality.table_path')

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

    df = df.filter("`asset_short_name` IN ('tchain', 'ysi1', 'ysi2', 'ysi3', 'tritonswq01')")

    df = df.withColumn('date', f.to_date(f.col('timestamp')))

    df = df.filter(f'date >= date_sub(current_date(), {watermark})')

    # check for erroneous data from tchain sensor
    # a typical value range for the dissolved oxygen sensor is 0-20
    # a typical value range for the temperature sensor is 0-40
    # we are deciding on a comfortable range of -100 to 100
    df = df.filter("NOT (asset_short_name = 'tchain' AND (value >= 100 OR value <= -100))")

    df = df.drop_duplicates()

    return df.selectExpr(
        'asset_short_name as sensor',
        'reading_short_name as reading',
        'farm_code',
        'site_id',
        'value',
        'timestamp',
        'year',
        'month',
        'day'
    )
