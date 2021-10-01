import boto3
import pandas as pd
import numpy as np
import io
import configparser
from datetime import datetime, date, time, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, FloatType
from sql_connector import write_to_dataware

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
spark = SparkSession\
    .builder\
    .master("local")\
    .appName("finalSalesData")\
    .getOrCreate()

# Set Date
current_date = datetime.now()
# today_suffix = "_"+current_date.strftime("%d%m%Y")
# yesterday_suffix = "_"+(current_date - timedelta(1)).strftime("%d%m%Y")
pull_date_yesterday = "_04062020"
pull_date_today = "_05062020"

# Get vendors data from S3
vendor_data_request = s3.Object(bucket_name=s3_bucket, key=f'Vendors/vendor_dump.dat')
vendor_data_response = vendor_data_request.get()['Body'].read()
vendor_data = pd.read_csv(io.BytesIO(vendor_data_response), header = None, delimiter="|", low_memory=False)

# Vendor data schema
vendor_data_schema = StructType([
    StructField("Vendor_ID", StringType(), True),
    StructField("Vendor_Name" ,StringType(), True),
    StructField("Vendor_Add_Street" ,StringType(), True),
    StructField("Vendor_Add_City" ,StringType(), True),
    StructField("Vendor_Add_State" ,StringType(), True),
    StructField("Vendor_Add_Country" ,StringType(), True),
    StructField("Vendor_Add_Zip" ,StringType(), True),
    StructField("Vendor_Updated_Date" ,StringType(), True),
])

vendor_data_df = spark.createDataFrame(vendor_data, schema= vendor_data_schema)
vendor_data_df.createOrReplaceTempView("vendorData")


# Get USD rates data
usd_rate_request = s3.Object(bucket_name = s3_bucket, key = f'USD_Rates/USD_Rates_Reference.dat')
usd_rate_response = usd_rate_request.get()['Body'].read()
usd_rate = pd.read_csv(io.BytesIO(usd_rate_response), header = 0, delimiter='|', low_memory=False)

# USD rate data schema
usd_rate_schema = StructType([
    StructField('Currency', StringType(), True),
    StructField('Currency_Code', StringType(), True),
    StructField('Exchange_Rate', StringType(), True),
    StructField('Currency_Updated_Date', StringType(), True)
])

usd_rate_data = spark.createDataFrame(usd_rate, schema = usd_rate_schema)
usd_rate_data.createOrReplaceTempView("usdRateData")


# get data with sale amount
data_with_sale_amount_request = s3.Object(bucket_name=s3_bucket, key = f'output/enriched/enriched_{pull_date_today}.csv')
data_with_sale_amount_response = data_with_sale_amount_request.get()['Body'].read()
data_with_sale_amount = pd.read_csv(io.BytesIO(data_with_sale_amount_response), header = 0, delimiter=",", low_memory=False)

#schema for data with sale amount
data_with_sale_amount_schema = StructType([
    StructField('Sale_ID', StringType(), True),
    StructField('Product_ID', StringType(), True),
    StructField('Quantity_Sold', FloatType(), True),
    StructField('Vendor_ID', StringType(), True),
    StructField('Sale_Date', StringType(), True),
    StructField('Sale_Amount', DoubleType(), True),
    StructField('Sale_Currency', StringType(), True),
    StructField('Product_Name',StringType(), True),
    StructField('Product_Price',IntegerType(), True)
])

data_with_sale_amount_df = spark.createDataFrame(data_with_sale_amount, schema = data_with_sale_amount_schema)
data_with_sale_amount_df.createOrReplaceTempView('dataWithSaleAmount')

# Get exchange rate in usd and calculate the sale amount
data_with_sale_amount_USD = spark.sql("""
    SELECT
    DSA.*,
    USRD.Exchange_Rate,
    ROUND((DSA.Sale_Amount / USRD.Exchange_Rate ),2) AS Sale_Amount_USD
    FROM dataWithSaleAmount AS DSA
    INNER JOIN usdRateData AS  USRD
    ON DSA.Sale_Currency = USRD.Currency_Code
""")
print("*********** Final Sales Data **************")
data_with_sale_amount_USD.show()

def save_final_sales_data(s3,data_with_sale_amount_USD):
    csv_buf = io.StringIO()
    final_data = data_with_sale_amount_USD.toPandas()
    final_data.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3.Object('end-to-end-pipeline', f'Sales_Data_Final/sales_data_final.csv').put(Body=csv_buf.getvalue())

# save data on s3 data lake
save_final_sales_data(s3, data_with_sale_amount_USD)

# save data on mysql data warehouse
write_to_dataware(data_with_sale_amount_USD)