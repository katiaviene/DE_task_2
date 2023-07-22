import pandas as pd
from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F
import time
import sys
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
db_file = 'data/dwh_db.db'
conn = sqlite3.connect(db_file)
database_url = "jdbc:sqlite:copy.db"
connection_properties = {
    "driver": "org.sqlite.JDBC",
    "url": database_url
}
cursor = conn.cursor()


class TextFileWriter:
    def __init__(self, file_path):
        self.file_path = file_path

    def write(self, text):
        with open(self.file_path, 'a') as file:
            file.write(text)


def redirect_output_to_txt(file_path):
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Save the current standard output stream (console) to a variable
            original_stdout = sys.stdout

            try:
                # Create the custom file-like object to capture the output
                txt_writer = TextFileWriter(file_path)

                # Redirect the standard output to the custom object
                sys.stdout = txt_writer

                # Call the function with its original arguments
                result = func(*args, **kwargs)

            except Exception as e:
                print("Error:", e)
                result = None

            finally:
                # Reset the standard output to the console
                sys.stdout = original_stdout

            return result

        return wrapper

    return decorator


def calculate_running_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Function '{func.__name__}' ran in {elapsed_time:.6f} seconds.")
        return result

    return wrapper


@redirect_output_to_txt("data/output.txt")
@calculate_running_time
def write_to_db(result_df, name, conn):
    # engine = create_engine("mysql+pyodbc://root:root@mysql_data:3306/ghdatawh?driver=ODBC+Driver+17+for+SQL+Server")
    pd_df = result_df.toPandas()
    pd_df.to_sql(name, conn, if_exists="replace", index=False)


@redirect_output_to_txt("/dataoutput.txt")
@calculate_running_time
def get_data(file):
    df = spark.read.csv(file, header=True)
    return df


@redirect_output_to_txt("data/output.txt")
@calculate_running_time
def popular_repos(df):
    result_df = df.groupBy("repo") \
        .agg(F.count("pr_id").alias("count")) \
        .orderBy(F.desc("count")) \
        .limit(50)
    return result_df


@redirect_output_to_txt("data/output.txt")
@calculate_running_time
def most_active_users(df):
    result_df = df.groupBy("author_login") \
        .agg(F.count("pr_id").alias("count")) \
        .orderBy(F.desc("count")) \
        .limit(3)
    return result_df


@redirect_output_to_txt("data/output.txt")
@calculate_running_time
def show_result(table_name):
    print("Copied data in DATABASE")
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
    tables = cursor.fetchall()
    print(pd.DataFrame(tables))


if __name__ == "__main__":
    df = get_data("data/ghtorrent-2019-05-20.csv")
    pop = popular_repos(df)
    write_to_db(pop, 'top_repos', conn)
    show_result('top_repos')
    users = most_active_users(df)
    write_to_db(users, 'top_users', conn)
    show_result('top_users')
