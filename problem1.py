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

master_url = sys.argv[1]

def create_spark_session(master_url):
    """Create a Spark session"""

    spark = (
        SparkSession.builder
        .appName("Problem1")
        .master(master_url).getOrCreate()
    )

    print("✅ Spark session created successfully for Problem 1")
    return spark


spark = create_spark_session(master_url)

path = "s3a://jgl72-assignment-spark-cluster-logs/data/**"

def problem1(path=path):
    
    logs_df = spark.read.text(path)

    print("✅Problem 1, Step 1: Read log data")


    print("✅ Showing sample raw lines:")
    logs_df.show(5, truncate=False)

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


    info_count = log_counts.filter(col('log_level') == 'INFO').select('count').collect()[0][0]
    error_count = log_counts.filter(col('log_level') == 'ERROR').select('count').collect()[0][0]
    warn_count = log_counts.filter(col('log_level') == 'WARN').select('count').collect()[0][0]
    debug_count = log_counts.filter(col('log_level') == '').select('count').collect()[0][0]

    info_percent = (info_count / logs_df.count()) * 100
    error_percent = (error_count / logs_df.count()) * 100
    warn_percent = (warn_count / logs_df.count()) * 100
    debug_percent = (debug_count / logs_df.count()) * 100

    print("Log level distribution:")
    print(f"INFO : {info_count}  ({info_percent:.2f} %)")
    print(f"ERROR : {error_count}  ({error_percent:.2f} %)")
    print(f"WARN : {warn_count}  ({warn_percent:.2f} %)")
    print(f"DEBUG: {debug_count}  ({debug_percent:.2f} %)")


    Summary = f"""
    Total log lines processed: {logs_df.count()}
    Total lines with log levels: {logs_parsed.filter(col('log_level') != '').count()}
    Unique log levels found: {log_counts.count()}

    Log level distribution:
    INFO : {info_count}  ({info_percent:.2f} %)
    ERROR : {error_count}  ({error_percent:.2f} %)
    WARN : {warn_count}  ({warn_percent:.2f} %)
    DEBUG: {debug_count}  ({debug_percent:.2f} %)
    """

    with open ("problem1_summary.txt", "w") as outfile:
        outfile.write(Summary)
    print("✅Problem 1, Step 5: Summary statistics")

problem1()




