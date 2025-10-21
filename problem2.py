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
import matplotlib.pyplot as plt
import seaborn as sns

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


    df_time = (
        df.groupBy("cluster_id", "application_id")
        .agg(
            spark_min("timestamp").alias("start_time"),
            spark_max("timestamp").alias("end_time")
        )
        .orderBy("application_id")
    )



    df_time = df_time.withColumn("app_number", row_number().over(Window.partitionBy("cluster_id").orderBy("start_time")))


    df_time.toPandas().to_csv('problem2_timeline.csv', index=False)

    
    print("✅Problem 2, Step 4: Create time-series data for each application")

    



    df_cluster = (
        df_time.groupBy("cluster_id")
        .agg(count("application_id").alias("num_applications"),
            spark_min("start_time").alias("cluster_first_app"),
            spark_max("end_time").alias("cluster_last_app")
        ).orderBy(col("num_applications").desc())
    )


    df_cluster.toPandas().to_csv('problem2_cluster_summary.csv', index=False)

    largest_cluster_id = df_cluster.toPandas().iloc[0]['cluster_id']

    print("✅Problem 2, Step 5: Aggregated cluster statistics")



    print(f"Total unique clusters: {df_cluster.count()}")
    print(f"Total applications: {df_time.count()}")
    print(f"Average applications per cluster: {df_time.count()/df_cluster.count():.2f}")

    print("Most heavily used clusters:")

    Summary = f"""
    Total unique clusters: {df_cluster.count()}
    Total applications: {df_time.count()}
    Average applications per cluster: {df_time.count()/df_cluster.count():.2f}

    Most heavily used clusters:
    """



    for i in range(df_cluster.count()):
        row = df_cluster.collect()[i]
        print(f"Cluster {row['cluster_id']}: {row['num_applications']} applications")
        Summary = Summary + f"Cluster {row['cluster_id']}: {row['num_applications']} applications\n"

    with open ("problem2_stats.txt", "w") as outfile:
        outfile.write(Summary)

    print("✅Problem 2, Step 6: Summary statistics")

    
    bar_df = df_cluster.select("cluster_id", "num_applications").toPandas()

    sns.barplot(x="cluster_id", y="num_applications", hue="cluster_id", data=bar_df, palette="viridis")

    plt.xlabel("Cluster")
    plt.ylabel("Number of applications")
    plt.title("Number of applications per cluster")
    plt.xticks()
    plt.savefig("problem2_bar_chart.png")

    print("✅Problem 2, Step 7: Bar Chart")




    density_df = df_time.select("application_id", "start_time", "end_time").where(col("cluster_id") == largest_cluster_id)


    density_pd = density_df.toPandas()
    density_pd['duration'] = (density_pd['end_time'] - density_pd['start_time']).dt.total_seconds() / 60
    sns.kdeplot(density_pd['duration'], fill=True, color="skyblue", alpha=0.5)
    plt.xlabel("Application Duration (minutes)")
    plt.ylabel("Density")
    plt.title(f"Application Duration Density for Cluster {largest_cluster_id}")
    plt.savefig("problem2_density_plot.png")
    print("✅Problem 2, Step 8: Density Plot")

    





problem2()

    













#
#