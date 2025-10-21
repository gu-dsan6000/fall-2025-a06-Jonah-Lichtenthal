import os
import subprocess
import sys
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import try_to_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import regexp_extract, col
from pyspark.sql.functions import (
    year, month, dayofmonth,
    avg, count, max as spark_max, min as spark_min,
    expr, ceil, percentile_approx
)
import pandas as pd
import re

def create_spark_session():
    """Create a Spark session"""

    spark = (
        SparkSession.builder
        .appName("Problem1")
        .getOrCreate()
    )

    print("✅ Spark session created successfully for Problem 2")
    return spark


spark = create_spark_session()

path = "data/sample/application_1485248649253_0052"

def problem2(path=path):
    
    logs_df = spark.read.text(path)

    print("✅Problem 2, Step 1: Read log data")

    df = logs_df.withColumn('file_path', input_file_name())
    df = df.withColumn('application_id', regexp_extract('file_path', r'application_(\d+_\d+)', 0))
    df = df.withColumn("cluster_id", regexp_extract(col("application_id"), r"application_(\d+)_\d+", 1))
    df = df.withColumn('container_id', regexp_extract('file_path', r'(container_\d+_\d+_\d+_\d+)', 1))
    df = df.withColumn('timestamp', regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1))
    
    print("✅Problem 2, Step 2: Extract application and container IDs")

      

    df = df.withColumn("timestamp", expr("try_to_timestamp(timestamp, 'yy/MM/dd HH:mm:ss')"))

    print("✅Problem 2, Step 3: Convert timestamp to timestamp format")


    app_times = (
        df.groupBy("cluster_id", "application_id")
        .agg(
            spark_min("timestamp").alias("start_time"),
            spark_max("timestamp").alias("end_time")
        )
        .orderBy("application_id")
    )



    app_times = app_times.withColumn("app_number", row_number().over(Window.partitionBy("cluster_id").orderBy("start_time")))


    app_times.toPandas().to_csv('problem2_timeline.csv', index=False)

    
    print("✅Problem 2, Step 4: Application start and end times")

    


problem2()

    













#
#