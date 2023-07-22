import pandas as pd
from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F
import time
import sys
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, broadcast
import sqlite3
from sqlite3 import Connection
from functools import wraps
from datetime import date
from pyspark.conf import SparkConf

spark_home = os.environ.get("SPARK_HOME")
driver_jar = "mysql-connector-j_8.1.0-1ubuntu23.04_all.deb"
driver_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), driver_jar)
spark_conf = SparkConf()
spark_conf.set("spark.jars", driver_path)

# DB_FILE = 'output/dwh_db.db'

mysql_jdbc_url = "jdbc:mysql://localhost:3306/githubpr"
mysql_properties = {
    "driver": "com.mysql.jdbc.Driver",
    "user": "user1",
    "password": "user1"
}
class TextFileWriter:
    def __init__(self, file_path):
        self.file_path = file_path

    def write(self, text):
        with open(self.file_path, 'a') as file:
            file.write(text)


def redirect_output_to_txt(file_path):
    def decorator(func):
        def wrapper(*args, **kwargs):
            original_stdout = sys.stdout
            try:
                txt_writer = TextFileWriter(file_path)
                sys.stdout = txt_writer
                result = func(*args, **kwargs)
            except Exception as e:
                print("Error:", e)
                result = None
            finally:
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
        print(f"{datetime.now()} : Function '{func.__name__}' ran in {elapsed_time:.6f} seconds.")
        return result

    return wrapper


@redirect_output_to_txt("output/report.txt")
@calculate_running_time
def write_to_db(result_df, name):
    result_df.write.jdbc(url=mysql_jdbc_url, table=name, mode="overwrite", properties=mysql_properties)
    # jdbc_url = f"jdbc:sqlite://{conn}"
    # result_df.write \
    #     .format("jdbc") \
    #     .option("url", jdbc_url) \
    #     .option("dbtable", name) \
    #     .mode("overwrite") \
    #     .save()


@redirect_output_to_txt("output/report.txt")
@calculate_running_time
def get_data(file):
    df = spark.read.csv(file, header=True)
    df.printSchema()
    return df


@redirect_output_to_txt("output/report.txt")
@calculate_running_time
def popular_repos(df):
    result_df = df.groupBy("repo") \
        .agg(F.count("pr_id").alias("count")) \
        .orderBy(F.desc("count")) \
        .limit(50)
    return result_df


@redirect_output_to_txt("output/report.txt")
@calculate_running_time
def most_active_users(df):
    result_df = df.groupBy("author_login") \
        .agg(F.count("pr_id").alias("count")) \
        .orderBy(F.desc("count")) \
        .limit(3)
    return result_df


@redirect_output_to_txt("output/report.txt")
@calculate_running_time
def active_users_in_pop_repos(df):
    pop_repos_df = popular_repos(df)
    pop_repos_list = pop_repos_df.select("repo").distinct().rdd.flatMap(lambda x: x).collect()
    users_in_pop_repos = df.filter(col("repo").isin(pop_repos_list)).groupBy([ "repo", "author_login" ]) \
        .agg(F.count("pr_id").alias("user_pr_count")) \
        .orderBy(F.desc("user_pr_count"))
    max_count_df = users_in_pop_repos.groupBy("repo").agg(F.max("user_pr_count").alias("max_count"))
    users_in_pop_repos = users_in_pop_repos.withColumnRenamed("repo", "repo_alias")

    partitioned_max_count_df = max_count_df.repartition("repo")
    partitioned_users_in_pop_repos = users_in_pop_repos.repartition("repo_alias")

    result_df = partitioned_max_count_df.join(
        broadcast(partitioned_users_in_pop_repos),
        (partitioned_max_count_df[ "repo" ] == partitioned_users_in_pop_repos[ "repo_alias" ]) &
        (partitioned_max_count_df[ "max_count" ] == partitioned_users_in_pop_repos[ "user_pr_count" ])
    ).limit(50)

    result_df = result_df.select("repo", "author_login", "user_pr_count")


    result_df = result_df.select("repo", "author_login", "user_pr_count")

    result_df.show()
    result_df = max_count_df.join(users_in_pop_repos,
                                  (max_count_df["repo"] == users_in_pop_repos["repo_alias"]) &
                                  (max_count_df[ "max_count" ] == users_in_pop_repos[ "user_pr_count" ])).limit(50).select("repo", "author_login", "user_pr_count")
    return result_df


@redirect_output_to_txt("output/report.txt")
@calculate_running_time
def show_result(table_name):
    print(f"TABLE {table_name} in DATABASE")
    df = spark.read.jdbc(url=mysql_jdbc_url, table="your_table_name", properties=mysql_properties)
    df.show()
    # cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
    # tables = cursor.fetchall()
    # print(pd.DataFrame(tables))



if __name__ == "__main__":
    spark = SparkSession.builder \
        .config("spark.driver.host", "localhost") \
        .appName("mysession") \
        .getOrCreate()
    # conn = sqlite3.connect(DB_FILE)
    # database_url = "jdbc:sqlite:dwh_db.db"
    # connection_properties = {
    #     "driver": "org.sqlite.JDBC",
    #     "url": database_url
    # }
    #
    # cursor = conn.cursor()

    df = get_data("data/ghtorrent-2019-05-20.csv")
    users_in_pop_repos_df = active_users_in_pop_repos(df)
    pop = popular_repos(df)
    write_to_db(pop, 'top_repos')
    show_result('top_repos')
    users = most_active_users(df)
    write_to_db(users, 'top_users')
    show_result('top_users')

    # write_to_db(users_in_pop_repos_df, 'top_users_in_top_repos', conn)
    # show_result('top_users_in_top_repos')
