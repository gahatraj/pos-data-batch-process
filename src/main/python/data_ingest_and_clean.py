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
    .appName("ingestAndClean")\
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
pull_date_yesterday = "_04062020"
pull_date_today = "_05062020"

# Read past data from s3 bucket
past_sale_data_bucket = s3.Object(bucket_name= s3_bucket_name, key=f'Sales_Landing/SalesDump{pull_date_yesterday}/SalesDump.dat')
past_sale_data_response = past_sale_data_bucket.get()['Body'].read()
past_sale_data = pd.read_csv(io.BytesIO(past_sale_data_response), header=0, delimiter="|", low_memory=False)


# Read current data from s3 bucket
current_sale_data_bucket = s3.Object(bucket_name= s3_bucket_name, key=f'Sales_Landing/SalesDump{pull_date_today}/SalesDump.dat')
current_sale_data_response = current_sale_data_bucket.get()['Body'].read()
current_sale_data = pd.read_csv(io.BytesIO(current_sale_data_response), header=0, delimiter="|", low_memory=False)



# Sale_data Schema
data_schema = StructType([
    StructField('Sale_ID', StringType(), True),
    StructField('Product_ID', StringType(), True),
    StructField('Quantity_Sold', FloatType(), True),
    StructField('Vendor_ID', StringType(), True),
    StructField('Sale_Date', StringType(), True),
    StructField('Sale_Amount', DoubleType(), True),
    StructField('Sale_Currency', StringType(), True)
])

# Unable to assign Timestamp type to Sale_Date, so creating temporary dataframe and updating column on the next step
temp_past_sale_df = spark.createDataFrame(past_sale_data, schema= data_schema)

# Update Sale_Date column with TimeStamp datatype
final_past_sale_df = temp_past_sale_df.withColumn('Sale_Date', col('Sale_Date').cast("timestamp"))

# Register the DataFrame as a SQL temporary view
final_past_sale_df.createOrReplaceTempView("pastSaleData")



# Unable to assign Timestamp type to Sale_Date, so creating temporary dataframe and updating column on the next step
temp_current_sale_df = spark.createDataFrame(current_sale_data, schema= data_schema)

# Update Sale_Date column with TimeStamp datatype
final_current_sale_df = temp_current_sale_df.withColumn('Sale_Date', col('Sale_Date').cast("timestamp"))

# Register the DataFrame as a SQL temporary view
final_current_sale_df.createOrReplaceTempView("currentSaleData")

# Combine past and current data and update the Quantity_Sold and Vendor_ID data
joined_data = spark.sql("""
                SELECT 
                    CSD.Sale_ID,
                    CSD.Product_ID,
                    CASE
                        WHEN CSD.Quantity_Sold = 'NaN' OR CSD.Quantity_Sold IS NULL THEN PSD.Quantity_Sold
                        ELSE CSD.Quantity_Sold
                    END AS Quantity_Sold,
                    CASE 
                        WHEN CSD.Vendor_ID = 'NaN' OR CSD.Vendor_ID IS NULL  THEN PSD.Vendor_ID
                        ELSE CSD.Vendor_ID
                    END AS Vendor_ID,
                    CSD.Sale_Date,
                    CSD.Sale_Amount,
                    CSD.Sale_Currency
                from currentSaleData AS CSD
                LEFT OUTER JOIN pastSaleData AS PSD 
                ON CSD.Sale_ID = PSD.Sale_ID
""")
joined_data.createOrReplaceTempView("joinedData")

# Released hold data that are updated
released_updated_record = spark.sql("""
                SELECT 
                    JD.Sale_ID
                FROM joinedData AS JD
                INNER JOIN pastSaleData AS PSD
                ON JD.Sale_ID = PSD.Sale_ID

""")
released_updated_record.createOrReplaceTempView("releasedUpdatedRecord")

# Held data that are still not updated
held_record = spark.sql("""
                SELECT * 
                FROM pastSaleData 
                WHERE Sale_ID NOT IN(SELECT 
                                        Sale_ID  
                                    FROM releasedUpdatedRecord
                                )
""")
held_record.createOrReplaceTempView("heldPastSaleData")

# Invalid data
invalid_data = spark.sql("""
                SELECT * 
                FROM joinedData 
                WHERE Quantity_Sold = 'NaN' OR Vendor_ID = 'NaN'
                UNION
                SELECT * 
                FROM heldPastSaleData
            """)
invalid_data.show()

# Valid data
valid_data = spark.sql("""
                SELECT * 
                FROM joinedData 
                WHERE Quantity_Sold != 'NaN' AND Vendor_ID != 'NaN' 
""")
valid_data.show()
def save_validdata_to_s3(s3, valid_data):
    csv_buf = io.StringIO()
    val_data = valid_data.toPandas()
    val_data.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3.Object('end-to-end-pipeline', f'output/valid/{pull_date_today}.csv').put(Body=csv_buf.getvalue())

def save_invaliddata_to_s3(s3, invalid_data):
    csv_buf = io.StringIO()
    inval_data = invalid_data.toPandas()
    inval_data.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3.Object('end-to-end-pipeline', f'output/invalid/{pull_date_today}.csv').put(Body=csv_buf.getvalue())


# Send CSV file to S3 folder
# save_validdata_to_s3(s3,valid_data)
# save_invaliddata_to_s3(s3,invalid_data)




