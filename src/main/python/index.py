import boto3
import pandas as pd
import numpy as np
import io
import configparser
from datetime import datetime, date, time, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, FloatType


# Read configurations from config file
config = configparser.ConfigParser()
config.read(r'../config/config.ini')

region_name = config.get('awsinfo', 'region_name'),
aws_access_key_id = config.get('awsinfo', 'aws_access_key_id'),
aws_secret_access_key = config.get('awsinfo', 'aws_secret_access_key')
my_bucket = config.get('s3info', 'bucket_name')


# Create Spark Session
spark = SparkSession\
    .builder\
    .master("local")\
    .appName("dataingestionandclean")\
    .getOrCreate()


# Create AWS Session
session = boto3.Session(
    region_name,
    aws_access_key_id,
    aws_secret_access_key
)


# Connect to AWS S3 resource
s3 = boto3.resource('s3')
s3_bucket_name = my_bucket


# Set Date
current_date = datetime.now()
# today_suffix = "_"+current_date.strftime("%d%m%Y")
# yesterday_suffix = "_"+(current_date - timedelta(1)).strftime("%d%m%Y")
pull_date_today = "_04062020"
pull_date_yesterday = "_05062020"


# Read data from s3 bucket 
s3_bucket = s3.Object(bucket_name= s3_bucket_name, key=f'Sales_Landing/SalesDump{pull_date_today}/SalesDump.dat')
response = s3_bucket.get()['Body'].read()


# Create Pandas Dataframe
df=[]
df.append(pd.read_csv(io.BytesIO(response), header=0, delimiter="|", low_memory=False))


# Create schema for data
landingFileSchema = StructType([
    StructField('Sale_ID', StringType(), True),
    StructField('Product_ID', StringType(), True),
    StructField('Quantity_Sold', FloatType(), True),
    StructField('Vendor_ID', StringType(), True),
    StructField('Sale_Date', StringType(), True),
    StructField('Sale_Amount', DoubleType(), True),
    StructField('Sale_Currency', StringType(), True)
])


# Create spark DataFrame with pandas dataframe
spark_df = spark.createDataFrame(df[0], schema=landingFileSchema)


# Register the DataFrame as a SQL temporary view
spark_df.createOrReplaceTempView("dailysaledata")
valid_data = spark.sql("SELECT * FROM dailysaledata WHERE Quantity_Sold != 'NaN' AND Vendor_ID != 'NaN' ")
invalid_data = spark.sql("SELECT * FROM dailysaledata WHERE Quantity_Sold = 'NaN' OR Vendor_ID = 'NaN'")


def write_csv_on_s3(s3, content):
    csv_buf = io.StringIO()
    response = content.toPandas()
    response.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3.Object('end-to-end-pipeline', 'output/valid/test.csv').put(Body=csv_buf.getvalue())

# Send CSV file to S3 folder
write_csv_on_s3(s3, valid_data)

