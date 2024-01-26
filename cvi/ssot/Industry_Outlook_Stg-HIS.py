#!/usr/bin/env python
# -*- coding: utf-8 -*-

#############################################################################
# IMPORTS
#############################################################################
from pyspark.sql import SparkSession
import sys
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.functions import to_date, col, substring, struct, concat, date_format, lit
from pyspark.sql.types import *
from pyspark.sql import functions as func
import time, re, pandas as pd, io, logging
import os
import boto3  # ECS Import
from botocore.client import Config  # ECS Import

############################################################################
# PARSING ARGUMENTS
############################################################################
ENV = sys.argv[1]

#############################################################################
# START SPARK
#############################################################################
spark = (
    SparkSession.builder
    .appName("rzs338") \
    .enableHiveSupport() \
    .getOrCreate()
)

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
print('Spark Version: ' + spark.version)
print('Pandas Version: ' + pd.__version__)
print(f"Boto3 Version: {boto3.__version__}")

############################################################################
# START APP-BUILDER
############################################################################
spark = SparkSession.builder.appName("GLB_INDUSTRIAL_OUTLOOK_STG_HIST").getOrCreate()

################################################
# LOAD FACT_TRACK DATA TO DATA FRAME           #
################################################

def write_to_table(df, table_name, table_path):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Writing to {table_name} started")
    try:
        df.write.mode("overwrite").parquet(table_path)
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Writing to {table_name} completed")
    except Exception as e:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Writing to {table_name} failed")
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {e}")

    df.unpersist()


bucket_name = 'fleet-manual-archive'
folder_name = 'SSOT_ARCHIVE'
archive_name = 'SSOT_Industry_Outlook_ARCHIVE'


schema = StructType([
    StructField("Ntl_Cd", StringType()),
    StructField("Veh_Type_Grp_Cd", IntegerType()),
    StructField("Eff_Mnth_Dt", DateType()),
    StructField("Indy_Outlk_Qty", StringType()),
    StructField("Indy_Seasnly_Adj_Annl_Rate_Nbr", StringType()),
    StructField("Last_Upd_Usr_ID", StringType()),
    StructField("Last_Upd_Dt", DateType())
])


df = spark.read.format('csv') \
    .option("header", "true") \
    .schema(schema) \
    .load("s3a://{}/{}/{}/*.csv".format(bucket_name, folder_name, archive_name))

print("Number of rows in 'df':", df.count())

df.show(5)
df.printSchema()
df2 = df.distinct()
df2.na.drop()
df2.count()
# DEFINING THE TARGET TABLE AND HDFS PATH
if ENV == "DEV":
    tableEnv = '{ENV}_'.format(ENV=ENV).lower()
elif ENV == "TST" or ENV == "CRT" or ENV == "PRD":
    tableEnv = ""
else:
    raise Exception('Environment Variable NOT passed properly')

TABLE_NAME: str = tableEnv + "dl_edge_base_dcfpa_171749_base_dcfpa_avista.gbl_industrial_outlook_stg"
TABLE_PATH: str = f"/sync/{ENV.lower()}_42124_edw_hi_b/DL/EDGE_BASE/DCFPA-171749/DCFPA-AVISTA/GBL_INDUSTRIAL_OUTLOOK_STG/Data"

# WRITE THE DATAFRAME, 'df_union' TO THE FLEET_OOS TABLE IN HDFS
write_to_table(df, TABLE_NAME, TABLE_PATH)

# ERROR HANDLING
spark.stop()
sys.exit(0)
