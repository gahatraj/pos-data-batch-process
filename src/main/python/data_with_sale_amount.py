import boto3
import pandas as pd
import numpy as np
import io
import configparser
from datetime import datetime, date, time, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, FloatType

# setup configuration
config = configparser.ConfigParser()
config.read(r'../config/config.ini')

region_name = config.get('awsinfo', 'region_name')
aws_access_key_id = config.get('awsinfo', 'aws_access_key_id')
aws_secret_access_key = config.get('awsinfo', 'aws_secret_access_key')
my_bucket = config.get('s3info', 'bucket_name')

# Create AWS session with boto3
session = boto3.Session(
    region_name,
    aws_access_key_id,
    aws_secret_access_key
)

# Connect to S3 bucket
s3 = boto3.resource('s3')
s3_bucket = my_bucket

# Start Spark Session
spark = SparkSession \
    .builder \
    .appName("dataWithSaleAmount") \
    .master("local") \
    .getOrCreate()

# Set Date
current_date = datetime.now()
# today_suffix = "_"+current_date.strftime("%d%m%Y")
# yesterday_suffix = "_"+(current_date - timedelta(1)).strftime("%d%m%Y")
pull_date_yesterday = "_04062020"
pull_date_today = "_05062020"

# Read Valid data for sale amount update
valid_sale_data = s3.Object(bucket_name= s3_bucket, key=f'output/valid/valid_{pull_date_today}.csv')
valid_sale_data_response = valid_sale_data.get()['Body'].read()
sale_data_ready_to_update = pd.read_csv(io.BytesIO(valid_sale_data_response), header=0, delimiter=",", low_memory=False)

# Schema for valid data dataframe
valid_data_schema = StructType([
    StructField('Sale_ID', StringType(), True),
    StructField('Product_ID', StringType(), True),
    StructField('Quantity_Sold', FloatType(), True),
    StructField('Vendor_ID', StringType(), True),
    StructField('Sale_Date', StringType(), True),
    StructField('Sale_Amount', DoubleType(), True),
    StructField('Sale_Currency', StringType(), True)
])
valid_data_DF = spark.createDataFrame(sale_data_ready_to_update, schema=valid_data_schema)
valid_data_DF.createOrReplaceTempView('validData')
# Read product data
product_data = s3.Object(bucket_name=s3_bucket, key = f'Products/GKProductList.dat')
product_data_response = product_data.get()['Body'].read()
product_data_final = pd.read_csv(io.BytesIO(product_data_response), header = 0, delimiter= "|", low_memory=False)

# Schema for product data dataframe
product_price = StructType([
    StructField('Product_ID',StringType(), True),
    StructField('Product_Name',StringType(), True),
    StructField('Product_Price',IntegerType(), True),
    StructField('Product_Price_Currency',StringType(), True),
    StructField('Product_updated_date',StringType(), True)
])

product_price_df = spark.createDataFrame(product_data_final, schema = product_price)
product_price_df.createOrReplaceTempView('productPrice')

data_with_price_included = spark.sql("""
    SELECT 
        VD.Sale_ID,
        VD.Product_ID,
        VD.Quantity_Sold,
        VD.Vendor_ID,
        VD.Sale_Date,
        PP.Product_Price * VD.Quantity_Sold AS Sale_Amount,
        VD.Sale_Currency,
        PP.Product_Name,
        PP.Product_Price,
        PP.Product_Price_Currency
    FROM validData as VD
    INNER JOIN productPrice as PP
    ON VD.Product_ID = PP.Product_ID
""")

# Send data updated with sale amount to enriched folder
def save_data_with_sale_amount(s3, data):
    csv_buf = io.StringIO()
    data_with_sale_amount = data.toPandas()
    data_with_sale_amount.to_csv(csv_buf, header=True, index= False)
    csv_buf.seek(0)
    s3.Object('end-to-end-pipeline', f'output/enriched/enriched_{pull_date_today}.csv').put(Body=csv_buf.getvalue())


save_data_with_sale_amount(s3,data_with_price_included)
