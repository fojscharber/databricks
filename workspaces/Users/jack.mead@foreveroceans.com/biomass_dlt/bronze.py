# Databricks notebook source
import dlt
import json
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField

kafka_brokers = spark.conf.get('spark.custom.bronze.kafka_brokers')
consume_topic = spark.conf.get('spark.custom.bronze.consume_topic')
table_name = spark.conf.get('spark.custom.bronze.table_name')
table_path = spark.conf.get('spark.custom.bronze.table_path')

data_schema = """
{"fields":[{"metadata":{},"name":"schemaVersion","nullable":false,"type":"string"},{"metadata":{},"name":"cameraName","nullable":false,"type":"string"},{"metadata":{},"name":"fileUrl","nullable":false,"type":"string"},{"metadata":{},"name":"uniqueKey","nullable":false,"type":"string"},{"metadata":{},"name":"modelVersion","nullable":false,"type":"string"},{"metadata":{},"name":"instanceIndex","nullable":false,"type":"integer"},{"metadata":{},"name":"timestamp","nullable":false,"type":"long"},{"metadata":{},"name":"poseCheck","nullable":true,"type":"boolean"},{"metadata":{},"name":"depthCheck","nullable":true,"type":"boolean"},{"metadata":{},"name":"boundingBox","nullable":true,"type":{"containsNull":true,"elementType":"integer","type":"array"}},{"metadata":{},"name":"maskSize","nullable":true,"type":"integer"},{"metadata":{},"name":"depthSize","nullable":true,"type":"integer"},{"metadata":{},"name":"pixelLength","nullable":true,"type":"double"},{"metadata":{},"name":"stdDepth","nullable":true,"type":"double"},{"metadata":{},"name":"nosePoint","nullable":true,"type":{"containsNull":true,"elementType":"double","type":"array"}},{"metadata":{},"name":"noseDepth","nullable":true,"type":"double"},{"metadata":{},"name":"tailPoint","nullable":true,"type":{"containsNull":true,"elementType":"double","type":"array"}},{"metadata":{},"name":"tailDepth","nullable":true,"type":"double"},{"metadata":{},"name":"avgDepth","nullable":true,"type":"double"},{"metadata":{},"name":"length3d","nullable":true,"type":"double"},{"metadata":{},"name":"biomass","nullable":true,"type":"double"},{"metadata":{},"name":"minDepth","nullable":true,"type":"double"},{"metadata":{},"name":"maxDepth","nullable":true,"type":"double"}],"type":"struct"}
"""
data_schema = StructType.fromJson(json.loads(data_schema))

event_schema = StructType([
    StructField("data", data_schema, False)
])

# COMMAND ----------

@dlt.table(
    name=table_name,
    comment="""
    Raw Biomass Results

    Biomass results for all frames taken from the biocams. This contains all false and true positive
    instances. This table may contain duplicates and instances that did not pass certain thresholds
    within the models.
    """,
    path=table_path,
    partition_cols=["cameraName", "year", "month", "day"],
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
    
    df = df\
        .selectExpr(
            "CAST(value AS STRING)"
        )\
        .withColumn(
            'value', f.from_json(f.col('value'), event_schema)
        )\
        .select('value.*')\
        .select('data.*')
    
    df = df.withColumn('date', f.to_timestamp(f.from_unixtime(f.col('timestamp') / 1000, 'yyyy-MM-dd')))
    df = df.withColumn('year', f.year('date'))
    df = df.withColumn('month', f.month('date'))
    df = df.withColumn('day', f.dayofmonth('date'))
    df = df.drop('date')

    return df
