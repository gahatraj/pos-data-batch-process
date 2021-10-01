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
mydata = spark.read.format("jdbc")\
        .option("url","jdbc:mysql://localhost:3306/batchdatadb")\
        .option("driver", "com.mysql.jdbc.Driver")\
        .option("dbtable", "test_table")\
        .option("user", "root")\
        .option("password", "password")\
        .load()
mydata.show()

# conn = mysql.connector\
#         .connect(user='root', database='batchdatadb',password='password', host='127.0.0.1', port='3306')


#
# spark = SparkSession.builder.config("spark.jars", "/usr/share/java/mysql-connector-java-8.0.22.jar") \
#     .master("local").appName("PySpark_MySQL_test").getOrCreate()
#
# wine_df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/TestDB") \
#     .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "Wines") \
#     .option("user", "me").option("password", "me").load()