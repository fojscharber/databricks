# Databricks notebook source
import dlt
import pyspark.sql.functions as f

watermark = spark.conf.get('spark.custom.gold.waterquality.watermark')
silver_table_name = spark.conf.get('spark.custom.gold.waterquality.silver_table_name')
table_name = spark.conf.get('spark.custom.gold.waterquality.table_name')
table_path = spark.conf.get('spark.custom.gold.waterquality.table_path')

# COMMAND ----------

@dlt.table(
    name=table_name,
    comment="""
        Aggregated 5 minute Water Quality Readings from C2 Avalon Panama Operations

        Water quality data points for water quality sensors: Exo Sonde YSI, Manta Sonde, and the 
        TChain sensor aggregated every 5 minutes. This dataset is scheduled to update every day
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
def gold_table():
    df = spark\
        .read\
        .format('delta')\
        .table(silver_table_name)

    df = df.withColumn('date', f.to_date(f.col('timestamp')))
    df = df.filter(f'date >= date_sub(current_date(), {watermark})')

    df = df.drop_duplicates()

    return df.groupBy(
        f.col("farm_code"),
        f.col("site_id"),
        f.col("sensor"),
        f.col("reading"),
        f.window('timestamp', '5 minutes'),
    )\
    .agg(
        f.mean('value').alias('avg'),
        f.percentile_approx('value', 0.5).alias('median'),
        f.sum('value').alias('sum'),
        f.min('value').alias('min'),
        f.max('value').alias('max'),
        f.stddev('value').alias('std'),
        f.count('value').alias('total')
    )\
    .withColumn('metrics', f.struct(
        f.col('avg'),
        f.col('sum'),
        f.col('min'),
        f.col('max'),
        f.col('std'),
        f.col('total'),
    ))\
    .withColumn('year', f.year('window.start'))\
    .withColumn('month', f.month('window.start'))\
    .withColumn('day', f.dayofmonth('window.start'))\
    .select('farm_code', 'site_id', 'sensor', 'reading', 'year', 'month', 'day', 'metrics', 'window')
