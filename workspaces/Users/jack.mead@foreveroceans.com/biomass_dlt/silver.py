# Databricks notebook source
import dlt
import json
import pyspark.sql.functions as f
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField

bronze_table_name = spark.conf.get('spark.custom.silver.bronze_table_name')
table_name = spark.conf.get('spark.custom.silver.table_name')
table_path = spark.conf.get('spark.custom.silver.table_path')
watermark = spark.conf.get('spark.custom.silver.watermark')
biomass_slope = spark.conf.get('spark.custom.bio.slope')
biomass_power = spark.conf.get('spark.custom.bio.power')
percent_offset = spark.conf.get('spark.custom.bio.offset')

max_depth_difference = 50
min_depth = 35
max_depth = 140
min_length = 5
max_length = 50
zscore = 3

# COMMAND ----------

@dlt.table(
    name=table_name,
    comment="""
    Filtered Biomass Results

    This table has been filtered with different mechanisms to produce only the
    true positives: the fish instances that were detected and a weight and
    length were produced. This table should be used to compare to manual
    samples, and to create aggregations over time.
    """,
    path=table_path,
    partition_cols=["camera_name", "year", "month", "day"],
    spark_conf={
        'spark.sql.session.timeZone': 'UTC',
        'spark.sql.sources.partitionOverwriteMode': 'dynamic',
    },
    table_properties={
        'pipelines.autoOptimize.managed': 'true',
        'pipelines.autoOptimize.zOrderCols': 'timestamp'
    }
)
def silver_data():
    df = spark\
        .read\
        .format('delta')\
        .table(bronze_table_name)
    
    df = df.withColumn('timestamp', f.to_timestamp(f.from_unixtime(f.col('timestamp') / 1000)))
    df = df.withColumn('date', f.to_date(f.col('timestamp'), 'yyy-MM-dd'))
    
    # filter so we don't reprocess the entire dataset
    df = df.filter(f'date >= date_sub(current_date(), {watermark})')

    # drop duplicate instances
    df = df.drop_duplicates()
    
    # partition on the date because it's set using a day interval
    camera_model_partition = Window.partitionBy(
        f.col("cameraName"),
        f.col("modelVersion"),
        f.col("date")
    )
    
    # filter invalid instances. Length was not detected
    df = df.filter('length3d is not null')
    
    # filter based on difference between the max and min depth. The difference
    # between the max and min depth shouldn't be drastically different due to
    # the fact that the fish should be at a given orientation and pose that is
    # "mostly" parallel to the camera
    df = df.filter(f'maxDepth - minDepth <= {max_depth_difference}')
    
    # filter based on a depth range. The stereo camera works most effectively
    # when the fish is positioned at a certain range of depths.
    df = df.filter(f'avgDepth > {min_depth} and avgDepth < {max_depth}')
    
    # filter based on a length range. A simple test of min and max length can
    # remove outliers for fish that can never grow beyond a certain range of lengths
    # anyways.
    df = df.filter(f'length3d > {min_length} and length3d < {max_length}')
    
    # filter out lengths not within a certain z-score from the normal over a day period. This is
    # a simple test to remove outliers based on pure statistics.
    df = df.withColumn('medianLength', f.percentile_approx('length3d', 0.5).over(camera_model_partition))
    df = df.withColumn('stdDevLength', f.stddev('length3d').over(camera_model_partition))
    df = df.withColumn('minLength', f.col('medianLength') - zscore * f.col('stdDevLength'))
    df = df.withColumn('maxLength', f.col('medianLength') + zscore * f.col('stdDevLength'))
    df = df.filter('medianLength > minLength and medianLength < maxLength')
    
    df = df.selectExpr(
        'cameraName as camera_name',
        'fileUrl as file_url',
        'uniqueKey as unique_key',
        'modelVersion as model_version',
        'instanceIndex as instance_index',
        'timestamp',
        'boundingBox as bounding_box',
        'maskSize as total_mask_pixels',
        'depthSize as total_depth_pixels',
        'pixelLength as pixel_length',
        'stdDepth as std_depth',
        'nosePoint as nose_point',
        'noseDepth as nose_depth',
        'tailPoint as tail_point',
        'tailDepth as tail_depth',
        'avgDepth as avg_depth',
        'length3d as length',
        'minDepth as min_depth',
        'maxDepth as max_depth',
    )
    
    # offset the length using a static percent offset rule
    df = df.withColumn('offset_length', f.col('length') * percent_offset)
    
    # Compute the biomass from the length! Yay
    df = df.withColumn('offset_weight', biomass_slope * (f.col('offset_length') ** biomass_power))
    df = df.withColumn('weight', biomass_slope * (f.col('length') ** biomass_power))
    
    df = df.withColumn('year', f.year('timestamp'))\
        .withColumn('month', f.month('timestamp'))\
        .withColumn('day', f.dayofmonth('timestamp'))
    
    return df
