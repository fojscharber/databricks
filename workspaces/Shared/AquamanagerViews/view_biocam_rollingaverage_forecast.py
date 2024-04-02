# Databricks notebook source
import pandas as pd
import logging
import pdb

from pyspark.sql.types import StringType, ArrayType, IntegerType, StructType, StructField, DateType, DoubleType
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col, max as max_, row_number, to_date, date_add, year, month, dayofmonth
from datetime import timedelta


def create_buckets(buckets, data):
    rdd = sc.parallelize(data)
    histogram = rdd.histogram(buckets)
    return histogram

def add_result(lastrow, offsetBuckets, weightBuckets, lengthBuckets,offsetrollingaverageBuckets,offsetWeight_lb_Buckets, 
 offsetWeight_lb_Buckets_two):
    newRow = dict()

    newRow["camera"] = lastrow.camera_name
    newRow["timestamp"] = lastrow.timestamp
    newRow["year"] = lastrow.year
    newRow["month"] = lastrow.month
    newRow["day"] = lastrow.day
    newRow["offsetWeightHistogram"] = offsetBuckets[1]
    newRow["weightHistogram"] = weightBuckets[1]
    newRow["lengthHistogram"] = lengthBuckets[1]
    newRow["offsetWeightBuckets"] = offsetBuckets[0]
    newRow["weightBuckets"] = weightBuckets[0]
    newRow["lengthBuckets"] = lengthBuckets[0]
    newRow["offsetWeightrollingaverageHistogram"] = offsetrollingaverageBuckets[1]
    newRow["offsetWeightrollingaverageBuckets"] = offsetrollingaverageBuckets[0]
    newRow["offsetWeight_lb_Histogram"] = offsetWeight_lb_Buckets[1]
    newRow["offsetWeight_lb_Buckets"] = offsetWeight_lb_Buckets[0]
    newRow["offsetWeight_lb_Histogram_no_two"] = offsetWeight_lb_Buckets_two[1]
    newRow["offsetWeight_lb_Buckets_no_two"] = offsetWeight_lb_Buckets_two[0]

    histogramData.append(newRow)


df = spark.read.table("hive_metastore.default.biomass_filtered")
# Convert Grams values into lb ad create new column of it
# df = df.withColumn('offset_weight_lbs', F.col('offset_weight') * 0.0022046)
df = df.withColumn("date",to_date("timestamp"))
# display(df)


#define window of 7 for calculating rolling mean
w = Window.partitionBy('camera_name').orderBy('day').rowsBetween(-6, 0)
df = df.withColumn('offset_weight_rolling_average', F.avg('offset_weight').over(w))
df = df.withColumn('offset_weight_lbs_rolling_average', F.col('offset_weight_rolling_average') * 0.0022046)

# Find max Date
max_date = df.agg(max_("date").alias("MaxDate"))
max_date_value = max_date.collect()[0]["MaxDate"]

# Show the result
# max_date.show()

filtered_df = df.filter(col("date") == max_date_value)
# display(filtered_df)
forecast_df = spark.createDataFrame([], filtered_df.schema)
# forecast_df = forecast_df.unionByName(df, allowMissingColumns=True)
num_days = 120

for i in range(1, num_days + 1):
    new_date = max_date_value + timedelta(days=i)
    # filtered_df = filtered_df.withColumn("date", max_date)
    filtered_df = filtered_df.withColumn("date", date_add(col("date"), 1))
    filtered_df = filtered_df.withColumn("offset_weight_rolling_average", col("offset_weight_rolling_average") + 5)
    filtered_df = filtered_df.withColumn("offset_weight_lbs_rolling_average", col("offset_weight_lbs_rolling_average") + (5 * 0.0022046)  )
    filtered_df = filtered_df.withColumn("year",year("date"))
    filtered_df = filtered_df.withColumn("month",month("date"))
    filtered_df = filtered_df.withColumn("day",dayofmonth("date"))
    # filtered_df['month'] = max_date_value.month
    # filtered_df['day'] = max_date_value.day
    forecast_df = forecast_df.unionByName(filtered_df, allowMissingColumns=True)

forecast_df = forecast_df.unionByName(df, allowMissingColumns=True)
# display(forecast_df)

# df.groupBy("camera_name").max_("date").show(truncate=False)
# result = df.groupBy("camera_name").agg(max_("date").alias("MaxDate"))
# display(result)


sdf = forecast_df.orderBy("camera_name", "year", "month", "day").collect()

# For final write back to delta
histogramData = []

# buckets to create
accumulatedOffsetWeight = []
accumulatedOffsetWeightrollingaverage = []
accumulatedOffsetWeight_lb = []
accumulatedOffsetWeight_lb_two = []
accumulatedWeight = []
accumulatedLength = []
weight_buckets=[0,250,500,750,1000,1250,1500,1750,2000,2250,2500,2750,3000,3250,3500,3750,99999]
lenght_buckets=[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
lb_weight_buckets =[0, 2, 4, 6, 100]
lb_weight_buckets_no_two =[0, 1, 2, 3, 4, 5, 6, 100]


# Get unique camera names
camera_names = set(row.camera_name for row in sdf)

for camera_name in camera_names:

    # Filter rows for the current camera
    camera_rows = [row for row in sdf if row.camera_name == camera_name]
    # track the current day
    lastrow = None
    lastitem = camera_rows[-1]

    # only need to track if the day has changed
    # Iterate through each row for the current camera
    for row in camera_rows:
        if lastrow == None:
            lastrow = row
        
        if lastrow.year != row.year or lastrow.month != row.month or lastrow.day != row.day:
            # create buckets and add to histogramData frame 
            offsetWeightBuckets = create_buckets(weight_buckets, accumulatedOffsetWeight)
            offsetWeightrollingaverageBuckets = create_buckets(weight_buckets, accumulatedOffsetWeightrollingaverage)
            offsetWeight_lb_Buckets = create_buckets(lb_weight_buckets, accumulatedOffsetWeight_lb)
            offsetWeight_lb_Buckets_two = create_buckets(lb_weight_buckets_no_two, accumulatedOffsetWeight_lb_two)


            weightBuckets = create_buckets(weight_buckets, accumulatedWeight)
            lengthBuckets = create_buckets(lenght_buckets, accumulatedLength)
            add_result(lastrow, offsetWeightBuckets, weightBuckets, lengthBuckets,offsetWeightrollingaverageBuckets,offsetWeight_lb_Buckets,offsetWeight_lb_Buckets_two)

            # clear accumulated data 
            accumulatedOffsetWeight = []
            accumulatedOffsetWeightrollingaverage = []
            accumulatedOffsetWeight_lb = []
            accumulatedOffsetWeight_lb_two = []
            accumulatedWeight = []
            accumulatedLength = []

            # update the last row
            lastrow = row

        if  row == lastitem:
            # create buckets and add to histogramData frame 
            offsetWeightBuckets = create_buckets(weight_buckets, accumulatedOffsetWeight)
            offsetWeightrollingaverageBuckets = create_buckets(weight_buckets, accumulatedOffsetWeightrollingaverage)
            offsetWeight_lb_Buckets = create_buckets(lb_weight_buckets, accumulatedOffsetWeight_lb)
            offsetWeight_lb_Buckets_two = create_buckets(lb_weight_buckets_no_two, accumulatedOffsetWeight_lb_two)


            weightBuckets = create_buckets(weight_buckets, accumulatedWeight)
            lengthBuckets = create_buckets(lenght_buckets, accumulatedLength)
            add_result(lastrow, offsetWeightBuckets, weightBuckets, lengthBuckets,offsetWeightrollingaverageBuckets,offsetWeight_lb_Buckets,offsetWeight_lb_Buckets_two)


            # clear accumulated data 
            accumulatedOffsetWeight = []
            accumulatedOffsetWeightrollingaverage = []
            accumulatedOffsetWeight_lb = []
            accumulatedOffsetWeight_lb_two = []
            accumulatedWeight = []
            accumulatedLength = []

            # update the last row
            lastrow = row


        else:
            # accumulate data to bucket
            accumulatedOffsetWeight.append(row.offset_weight)
            accumulatedOffsetWeightrollingaverage.append(row.offset_weight_rolling_average)
            accumulatedOffsetWeight_lb.append(row.offset_weight_lbs_rolling_average)
            accumulatedOffsetWeight_lb_two.append(row.offset_weight_lbs_rolling_average)
            accumulatedWeight.append(row.weight)
            accumulatedLength.append(row.length)


wdf = sc.parallelize(histogramData).toDF()

wdf.write.mode("overwrite")\
    .format("delta")\
    .option("mergeSchema", "true")\
    .partitionBy("camera", "year", "month")\
    .saveAsTable("hive_metastore.default.view_biocam_rollingaverage_forecast")

# COMMAND ----------



