import os
import subprocess
import sys
import time
import logging
from pyspark.sql import SparkSession
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

    print("✅ Spark session created successfully for Problem 1")
    return spark


spark = create_spark_session()

path = "data/sample/application_1485248649253_0052"

def problem1(path=path):
    
    logs_df = spark.read.text(path)

    print("✅Problem 1, Step 1: Read log data")

    logs_parsed = logs_df.select(
        regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
        regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)', 1).alias('log_level'),
        regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)\s+([^:]+):', 2).alias('component'),
        col('value').alias('message')
    )

    print("✅Problem 1, Step 2: Parse log data")

    log_counts = logs_parsed.groupBy('log_level').count().orderBy('log_level')

    log_counts.show()

    log_counts.toPandas().to_csv('problem1_counts.csv', index=False)

    print("✅Problem 1, Step 3: Count log levels")


    sample_logs = logs_parsed.select('message', 'log_level').orderBy(expr("rand()")).limit(10)
    sample_logs.show(truncate=False)

    sample_logs.toPandas().to_csv('problem1_sample.csv', index=False)

    print("✅Problem 1, Step 4: 10 random sample log entries with their levels")



    

    print(f"Total log lines processed: {logs_df.count()}")
    print(f"Total lines with log levels: {logs_parsed.filter(col('log_level') != '').count()}")
    print(f"Unique log levels found: {log_counts.count()}")

    print("Log level distribution:")
    print(f"INFO : {log_counts.filter(col('log_level') == 'INFO').select('count').collect()[0][0]}  ({log_counts.filter(col('log_level') == 'INFO').select('count').collect()[0][0]/logs_df.count():.2f} %)")
    print(f"ERROR : {log_counts.filter(col('log_level') == 'ERROR').select('count').collect()[0][0]}  ({log_counts.filter(col('log_level') == 'ERROR').select('count').collect()[0][0]/logs_df.count():.2f} %)")
    print(f"WARN : {log_counts.filter(col('log_level') == 'WARN').select('count').collect()[0][0]}  ({log_counts.filter(col('log_level') == 'WARN').select('count').collect()[0][0]/logs_df.count():.2f} %)")
    print(f"DEBUG: {log_counts.filter(col('log_level') == '').select('count').collect()[0][0]}  ({log_counts.filter(col('log_level') == '').select('count').collect()[0][0]/logs_df.count():.2f} %)")

    Summary = f"""
    Total log lines processed: {logs_df.count()}
    Total lines with log levels: {logs_parsed.filter(col('log_level') != '').count()}
    Unique log levels found: {log_counts.count()}

    Log level distribution:
    INFO : {log_counts.filter(col('log_level') == 'INFO').select('count').collect()[0][0]}  ({log_counts.filter(col('log_level') == 'INFO').select('count').collect()[0][0]/logs_df.count():.2f} %
    ERROR : {log_counts.filter(col('log_level') == 'ERROR').select('count').collect()[0][0]}  ({log_counts.filter(col('log_level') == 'ERROR').select('count').collect()[0][0]/logs_df.count():.2f} %
    WARN : {log_counts.filter(col('log_level') == 'WARN').select('count').collect()[0][0]}  ({log_counts.filter(col('log_level') == 'WARN').select('count').collect()[0][0]/logs_df.count():.2f} %
    DEBUG: {log_counts.filter(col('log_level') == '').select('count').collect()[0][0]}  ({log_counts.filter(col('log_level') == '').select('count').collect()[0][0]/logs_df.count():.2f} %
    """

    with open ("problem1_summary.txt", "w") as outfile:
        outfile.write(Summary)
    print("✅Problem 1, Step 5: Summary statistics")

problem1()





#from pyspark.sql.functions import input_file_name

# Extract from file path
#df = logs_df.withColumn('file_path', input_file_name())
#df = df.withColumn('application_id', regexp_extract('file_path', r'application_(\d+_\d+)', 0))
#df = df.withColumn('container_id', regexp_extract('file_path', r'(container_\d+_\d+_\d+_\d+)', 1))


