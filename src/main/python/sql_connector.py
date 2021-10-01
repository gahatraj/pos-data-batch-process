from pyspark.sql import SparkSession
import pandas as pd
import mysql.connector

spark = SparkSession\
        .builder\
        .config("spark.jars", r"../jars/mysql-connector-java-8.0.26.jar")\
        .master("local")\
        .appName("sqlConnector")\
        .getOrCreate()

# Connect to MySQL DB
def write_to_dataware(data):
         data.write.format("jdbc")\
        .option("url","jdbc:mysql://localhost:3306/batchdatadb")\
        .option("driver", "com.mysql.cj.jdbc.Driver")\
        .option("dbtable", "sale_data_final")\
        .option("user", "root")\
        .option("password", "password")\
        .mode('append')\
        .save()


mydata = spark.read.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/batchdatadb") \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("dbtable", "sale_data_final") \
        .option("user", "root") \
        .option("password", "password") \
        .load()

mydata.show()