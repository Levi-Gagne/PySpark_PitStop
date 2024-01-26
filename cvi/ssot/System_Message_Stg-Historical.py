#!/usr/bin/env python
# coding: utf-8


# IMPORT REQUIRED PACKAGES
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as func
import io, os, re, sys, time, logging
from pyspark.sql.window import Window
import botocore
import boto3                         #ECS Import
from botocore.client import Config   #ECS Import

# PARSING ARGUMENTS
ENV=sys.argv[1]

# START-SPARK
from pyspark.sql import SparkSession
spark = (
SparkSession.builder
.appName("System_Message-HIS") \
.enableHiveSupport() \
.getOrCreate()
)
print('Spark Version: ' + spark.version)

# DEFINE THE AWS S3 CLIENT
session = boto3.session.Session()
s3_client = session.client(
    service_name='s3',
    aws_access_key_id=spark.conf.get("spark.hadoop.fs.s3a.access.key"),
    aws_secret_access_key=spark.conf.get("spark.hadoop.fs.s3a.secret.key"),
    endpoint_url=spark.conf.get("spark.hadoop.fs.s3a.endpoint"),
    config=Config(s3={'addressing_style': 'path'})
)
print(f"Boto3 Version: {boto3.__version__}")

# AWS CREDENTIALS AND BUCKET DETAILS
S3_ARCHIVE      = "fleet-manual-archive"
SSOT_ARCHIVE    = "SSOT_ARCHIVE"
SYS_MSG_ARCHIVE = "SSOT_System_Message_ARCHIVE"

# TABLE NAME FOR WRITING
TABLE_NAME = "dl_edge_base_dcfpa_171749_base_dcfpa_avista.system_message_stg"
TABLE_PATH = f"/sync/{ENV.lower()}_42124_edw_hi_b/DL/EDGE_BASE/DCFPA-171749/DCFPA-AVISTA/SYSTEM_MESSAGE_STG/Data" # HDFS path to write the Parquet files to

# INITIALIZE SPARK SESSION
spark = SparkSession.builder.appName("System_Message-HIS").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# DEFINE DATA SCHEMA
schema = StructType([
    StructField("NTL_CD", StringType()),
    StructField("SYS_CD", StringType()),
    StructField("SYS_DESC", StringType()),
    StructField("SYS_MSG_SA_ID", StringType()),
    StructField("MSG_TXT", StringType()),
    StructField("EFCTV_DT", StringType()),
    StructField("EXPRTN_DT", StringType()),
    StructField("MESSAGE_TYPE", StringType()),
    StructField("LAST_UPDT_USR_ID", StringType()),
    StructField("LAST_UPDT_DT", StringType())
])

# FILE LOCATION
file_location = f"s3a://{S3_ARCHIVE}/{SSOT_ARCHIVE}/{SYS_MSG_ARCHIVE}/SSOT_System_Message.csv"


# LOAD DATA INTO SPARK DATAFRAME
df = spark.read.format('csv') \
    .schema(schema) \
    .option("header", "true") \
    .load(file_location)

# CAST COLUMNS TO APPROPRIATE TYPES
df = df.withColumn("SYS_MSG_SA_ID", col("SYS_MSG_SA_ID").cast(IntegerType()))
df = df.withColumn("EFCTV_DT", to_date(col("EFCTV_DT"), "yyyy-MM-dd"))
df = df.withColumn("EXPRTN_DT", to_date(col("EXPRTN_DT"), "yyyy-MM-dd"))
df = df.withColumn("LAST_UPDT_DT", to_date(col("LAST_UPDT_DT"), "yyyy-MM-dd"))

# WRITE PROCESSED DATAFRAME TO SPECIFIED HDFS PATH
print(f"Writing to {TABLE_NAME} started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
df.write.format("parquet").mode("overwrite").save(TABLE_PATH)
print(f"Writing to {TABLE_NAME} completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ERROR HANDLING
#############################################################################
spark.stop()
sys.exit(0)