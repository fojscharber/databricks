# Databricks notebook source
import pandas as pd
import logging
import pdb

from pyspark.sql.types import StringType, ArrayType, IntegerType, StructType, StructField


def create_buckets(buckets, data):
    rdd = sc.parallelize(data)
    histogram = rdd.histogram(buckets)
    return histogram

def add_result(lastrow, offsetBuckets, weightBuckets, lengthBuckets):
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
    histogramData.append(newRow)


df = spark.read.table("hive_metastore.default.biomass_filtered")

# sdf = df.orderBy("camera_name", "year", "month", "day").take(100000)
sdf = df.orderBy("camera_name", "year", "month", "day").collect()

# track the current day
lastrow = None
lastitem = sdf[-1]

# For final write back to delta
histogramData = []

# buckets to create
accumulatedOffsetWeight = []
accumulatedWeight = []
accumulatedLength = []
weight_buckets= [0,250,500,750,1000,1250,1500,1750,2000,2250,2500,2750,3000,3250,3500,3750,99999]
lenght_buckets= [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

# only need to track if the day has changed
for row in sdf:
    if lastrow == None:
        lastrow = row
    #
    if lastrow.day != row.day or row == lastitem:
        # create buckets and add to histogramData frame
        offsetWeightBuckets = create_buckets(weight_buckets, accumulatedOffsetWeight)
        weightBuckets = create_buckets(weight_buckets, accumulatedWeight)
        lengthBuckets = create_buckets(lenght_buckets, accumulatedLength)
        add_result(lastrow, offsetWeightBuckets, weightBuckets, lengthBuckets)


        # clear accumulated data 
        accumulatedOffsetWeight = []
        accumulatedWeight = []
        accumulatedLength = []

        # update the last row
        lastrow = row

    else:
        # accumulate data to bucket
        accumulatedOffsetWeight.append(row.offset_weight)
        accumulatedWeight.append(row.weight)
        accumulatedLength.append(row.length)



wdf = sc.parallelize(histogramData).toDF()
wdf.write.mode("overwrite").saveAsTable("default.view_biomass_daily_histograms_PB")

# COMMAND ----------



