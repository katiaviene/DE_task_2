import pandas as pd
from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F
import time
from sqlalchemy import create_engine

from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count
import sqlite3
from sqlite3 import Connection
from functools import wraps
from datetime import date
from pyspark.conf import SparkConf

spark_home = os.environ.get("SPARK_HOME")
spark = SparkSession.builder \
    .config("spark.driver.host", "localhost") \
    .appName("mysession") \
    .getOrCreate()
df = spark.read.csv("data/ghtorrent-2019-05-20.csv", header=True)
df.printSchema()


def calculate_running_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Function '{func.__name__}' ran in {elapsed_time:.6f} seconds.")
        return result

    return wrapper


@calculate_running_time
def write(result_df, name):
    engine = create_engine("mysql+pyodbc://root:root@mysql_data:3306/ghdatawh?driver=ODBC+Driver+17+for+SQL+Server")
    pd_df = result_df.toPandas()
    pd_df.to_sql(name, engine, if_exists="replace", index=False)


@calculate_running_time
def get_data(file):
    df = spark.read.csv(file, header=True)
    return df


@calculate_running_time
def popular_repos(df):
    result_df = df.groupBy("repo").agg(F.count("pr_id").alias("count")).orderBy("count").desc().limit(50)
    return result_df


@calculate_running_time
def most_active_users(df):
    result_df = df.groupBy("author_login").agg(F.count("pr_id").alias("count")).orderBy("count").desc().limit(3)
    return result_df


if __name__ == "__main__":
    df = get_data("data/ghtorrent-2019-05-20.csv")
    pop = popular_repos(df)
    pop.show()
    users = most_active_users(df)
    users.show()
