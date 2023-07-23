from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F
import time
import sys
from datetime import datetime
from pyspark.conf import SparkConf

spark_home = os.environ.get("SPARK_HOME")
driver_jar = "mysql-connector-j-8.1.0.jar"
driver_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), driver_jar)
spark_conf = SparkConf()
spark_conf.set("spark.jars", driver_path)

mysql_jdbc_url = "jdbc:mysql://mysqldb:3306/githubpr?useSSL=true&trustServerCertificate=true"

mysql_properties = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "root",
    "password": "1234"
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
def busy_time(df):
    result_df = df.groupBy("commit_date") \
        .agg(F.count("pr_id").alias("count")) \
        .orderBy(F.desc("count")) \
        .limit(3)
    return result_df


@redirect_output_to_txt("output/report.txt")
@calculate_running_time
def show_result(table_name):
    print(f"TABLE {table_name} in DATABASE")
    df = spark.read.jdbc(url=mysql_jdbc_url, table=table_name, properties=mysql_properties)
    df.limit(3).show()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .appName("mysession") \
        .getOrCreate()

    df = get_data("data/ghtorrent-2019-05-20.csv")

    pop = popular_repos(df)
    write_to_db(pop, 'top_repos')
    show_result('top_repos')
    users = most_active_users(df)
    write_to_db(users, 'top_users')
    show_result('top_users')
    busy_time_df = busy_time(df)
    write_to_db(busy_time_df, 'busy_time')
    show_result('busy_day')
