#!/usr/bin/env python
# coding: utf-8

#############################################################################
# IMPORTS
#############################################################################
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as func
import io, os, re, sys, time, logging, pandas as pd 
import boto3                                  #ECS Import
from botocore.client import Config            #ECS Import
import pytz

############################################################################
# PARSING ARGUMENTS
############################################################################
ENV=sys.argv[1]

#############################################################################
# START SPARK
#############################################################################
from pyspark.sql import SparkSession
spark = (
SparkSession.builder
.appName("SSOT_Industrial_Outlook_Stg-INCREMENTAL") \   
.enableHiveSupport() \
.getOrCreate()
)
print('Spark Version: ' + spark.version)

############################################################################
# DEFINE THE AWS S3 Client
############################################################################
session = boto3.session.Session()
s3_client = session.client(
    service_name='s3',
    aws_access_key_id=spark.conf.get("spark.hadoop.fs.s3a.access.key"),
    aws_secret_access_key=spark.conf.get("spark.hadoop.fs.s3a.secret.key"),
    endpoint_url=spark.conf.get("spark.hadoop.fs.s3a.endpoint"),
    config=Config(s3={'addressing_style': 'path'})
)
print(f"Boto3 Version: {boto3.__version__}")

############################################################################
# # SET THE S3 BUCKET & INCREMENTAL FILENAME 
############################################################################
# S3 BUCKET
bucket_name      = 'fleet-manual'
# INCREMENTAL FILE
source_file      = 'US_IndustryOutlook_SAAR.csv'             # This will be the file name and we will add date to the name for incremental file name process?
# INCREMENTAL FILE: UPDATED
today = datetime.today()
current_date = today.strftime("%Y %m %B")
destination_file = f"{current_date}_US_IndustryOutlook_SAAR.csv"

############################################################################
# # SET TO EASTERN TIMEZONE TO APPEND THE PROPER DATE
# ############################################################################
# # TIMEZONE AS: EST
# eastern_tz       = pytz.timezone('US/Eastern')
# # CURRENT EST
# current_time_et  = datetime.now(eastern_tz)
# #DATE FORMAT: YYYYMMDD
# current_date     = current_date.strftime("%Y %m %B")


############################################################################
# COPY 'MODEL_NET.dat' RENAMED: 'MODEL_NET_YYYYMMDD.txct'
############################################################################
# CREATE AN S3 CLIENT OBJECT
s3 = boto3.client('s3', aws_access_key_id=_AWS_ACCESS_KEY_ID, aws_secret_access_key=_AWS_SECRET_ACCESS_KEY, endpoint_url=_ENDPOINT_URL)   

# CHECK IF THE DESTINATION FILE ALREADY EXISTS IN THE BUCKET 
existing_files = s3.list_objects(Bucket=bucket_name, Prefix=destination_file)

# IF THE FILE EXISTS: SKIP COPY
if 'Contents' in existing_files:                                                                      # what does this contents means here? 
    print(f"File {destination_file} already exists in the bucket, skipping copy.")      
# IF THE FILE DOESN'T EXIST: COPY TO BUCKET WITH DATE APPENDED
else:
    response = s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': source_file}, Key=destination_file)
    print(f"File {destination_file} copied to bucket {bucket_name}.")

# REMOVE THE SOURCE FILE FROM THE BUCKET 
#s3.delete_object(Bucket=bucket_name, Key=source_file)

############################################################################
# DEFINE THE SCHEMA FOR THE INPUT MODEL_NET FILES
############################################################################
df_schema = StructType([
    StructField("Ntl_Cd", StringType()),         # if we want to use char(2) then StructField("Attribute", StringType(), metadata={"charType": "char(2)"})
    StructField("Veh_Type_Grp_Cd",  StringType()),
    StructField("Eff_Mnth_Dt", StringType()),
    StructField("Indy_Outlk_Qty", StringType()),
    StructField("Indy_Seasnly_Adj_Annl_Rate_Nbr", StringType()),
    StructField("Last_Upd_Usr_ID", StringType()),
    StructField("Last_Upd_Dt",  StringType())
])

############################################################################
# LOAD INCREMENTAL 'MODEL_NET_YYYYMMDD.txt' INTO DATAFRAME
############################################################################
try:
    # READ THE CSV FILES FROM S3, APPLY THE SCHEMA, & SET THE DELIMITER
    df = (
        spark.read.format('csv')
        .option("sep", "|")  #what is this separator delimiter for?
        .schema(df_schema)
        .option("header", "false")
        .load(f"s3a://{bucket_name}/{folder_name}/{model_net_folder}/*_US_IndustryOutlook_SAAR.csv")     ## 
    )
    # CAST MONTH COLUMNS TO: IntergerType
    for month in ["Veh_Type_Grp_Cd", "Indy_Outlk_Qty"]:
       df = df.withColumn(month, col(month).cast(IntegerType()))

    # EXTRACT THE DATE FROM THE INPUT FILE NAME, CAST IT TO: 'DateType,' & ADD IT AS A NEW COLUMN: 'Rpt_Dt'
    df = (
        df.withColumn("Rpt_Dt", to_date(regexp_extract(input_file_name(), r"(*.)_US_IndustryOutlook_SAAR.csv\.txt", 1), "yyyyMMdd"))
        .withColumn("Sales_Through_Dt", date_sub("Rpt_Dt", 1))
    )

# IF AN ERROR OCCURS WHILE LOADING DATA FROM S3, PRINT THE ERROR & EXIT THE SCRIPT
except Exception as e:
    print(f"Error loading data from S3: {e}")
    sys.exit(1)


#############################################################################
# WRITE THE DATAFRAME TO THE TABLE
#############################################################################
# CHECK IF THERE ARE ALREADY RECORDS WITH THE RPT_DT BEING APPENDED
matching_rpt_dt = spark.read.parquet(TABLE_PATH).select('Rpt_Dt').where(col('Rpt_Dt').isin(df.select('Rpt_Dt').distinct().rdd.flatMap(lambda x: x).collect()))

# TABLE NAME & PATH IN HDFS
TABLE_NAME = "dev_dl_edge_base_dcfpa_171749_base_dcfpa_avista.fleet_oos"
TABLE_PATH = f"/sync/{ENV.lower()}_42124_edw_hi_b/DL/EDGE_BASE/DCFPA-171749/DCFPA-AVISTA/FLEET_OOS/Data"


if matching_rpt_dt.count() > 0:
    print("There is at least one matching rpt_dt in the table, skipping write.")
else:
    print(f"Writing to {TABLE_NAME} started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    df.write.format("parquet").mode("append").save(TABLE_PATH)
    write_success = True
    print(f"Writing to {TABLE_NAME} completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


#############################################################################
# ARCHIVE THE MODEL NET FILE
#############################################################################
s3 = boto3.client('s3', aws_access_key_id=_AWS_ACCESS_KEY_ID, aws_secret_access_key=_AWS_SECRET_ACCESS_KEY, endpoint_url=_ENDPOINT_URL)

# archive the source file if there are no existing files with the same name in the archive bucket
archive_bucket_name = 'fleet-manual-archive'
archive_key_prefix  = 'FLEET_OOS_DATA_ARCHIVE/FLEET_OOS_HISTORICAL_MODEL_NET/'

existing_files = s3.list_objects(Bucket=archive_bucket_name, Prefix=archive_key_prefix)
existing_keys = [obj['Key'].split('/')[-1] for obj in existing_files.get('Contents', [])]

if destination_file.split('/')[-1] not in existing_keys:
    archive_key = archive_key_prefix + destination_file
    response = s3.copy_object(Bucket=archive_bucket_name, CopySource={'Bucket': bucket_name, 'Key': destination_file}, Key=archive_key)
    print(f"File {destination_file} copied to archive bucket.")
else:
    print(f"File {destination_file} already exists in the archive bucket, skipping copy.")

try:
    s3.head_object(Bucket=bucket_name, Key=destination_file)
    s3.delete_object(Bucket=bucket_name, Key=destination_file)
    print(f"File {destination_file} already exists in the bucket, deleted the existing file.")
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        print(f"File {destination_file} does not exist in the bucket, copying file.")
    else:
        raise e
		
#############################################################################
# ERROR HANDLING
#############################################################################
spark.stop()
sys.exit(0)