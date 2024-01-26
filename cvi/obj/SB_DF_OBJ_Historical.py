#!/usr/bin/env python
# coding: utf-8

# creating the SparkSession
# from tkinter import FALSE
from pyspark.sql import SparkSession, Row

spark = (
     SparkSession.builder
    .enableHiveSupport()
    .getOrCreate()) 

#importing the Pyspark functions
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as sf

from datetime import datetime, date, timedelta
import pandas as pd
import subprocess
import io
import re
import logging
from dda_utils.functions import *
from dda_utils.hive import *
from pyspark import SparkContext, HiveContext
from pyspark.sql.types import ShortType,IntegerType,StringType,DecimalType,DoubleType,DateType,TimestampType
from pyspark.sql.functions import current_timestamp

# For Error handling
import sys

#ECS Import
import boto3
from botocore.client import Config
from gm_pyspark_etl import s3 as gm_s3

ENV=sys.argv[1]

# Check Envinronment
if ENV == "DEV":
    tableEnv=ENV+"_"
elif ENV == "TST" or ENV == "CRT" or ENV == "PRD":
    tableEnv=""
else:
    raise Exception('Environment Variable NOT passed properly')

# Get Table Path
# DEFINING THE TARGET TABLE AND HDFS PATH
if ENV.lower() == "prod":
    TABLE_NAME = "dl_edge_base_dcfpa_171749_base_dcfpa_avista.obj_sb_df_stg"
else:
    TABLE_NAME = "dev_dl_edge_base_dcfpa_171749_base_dcfpa_avista.obj_sb_df_stg"

TABLE_PATH = "/sync/{ENV.lower()}_42124_edw_dl_b/DL/EDGE_BASE/DCFPA-171749/DCFPA-AVISTA/OBJ_SB_DF_STG/Data"
TABLE_NAME2 = '{ENV}dl_edge_base_dcfpa_171749_base_dcfpa_avista.obj_sb_df_stg'.format(ENV=tableEnv.lower())
#TABLE_PATH2 =  '/sync/{ENV}42124_edw_dl_b/DL/EDGE_BASE/DCFPA-171749/DCFPA-AVISTA/OBJ_SB_DF_STG/Data'.format(ENV=ENV.lower()+"_")
TABLE_PATH2 =  '/sync/{ENV}42124_edw_hi_b/DL/EDGE_BASE/DCFPA-171749/DCFPA-AVISTA/OBJ_SB_DF_STG/Data'.format(ENV=ENV.lower()+"_")
print('Spark Version: ' + spark.version)
print ('sys version: ' + sys.version)
print('Current Env: ' + ENV)

# Setup ECS client to read Excel files
boto3.set_stream_logger('botocore', logging.ERROR)
session = boto3.session.Session()
s3_client = session.client(
	service_name = 's3',
	aws_access_key_id = spark.conf.get("spark.hadoop.fs.s3a.access.key"),
	aws_secret_access_key = spark.conf.get("spark.hadoop.fs.s3a.secret.key"),
	endpoint_url = spark.conf.get("spark.hadoop.fs.s3a.endpoint"),
	config=Config(s3={'addressing_style': 'path'})
)
found = False
####################################### Converts Excel --> Pandas Dataframe --> Spark Dataframe #######################################
def excel_to_sdf(bucket,searchPattern,num_of_columns):
    found = False
    resp = s3_client.list_objects_v2(Bucket=bucket)
    contents = resp['Contents']
    if not contents:
        print("No Contents in {}. Exiting...".format(bucket))
        logging.info("No Contents in {}. Exiting...".format(bucket))
        exit(1)
    sdf1=None
    sdf2=None
    file_list = list()
    for file_object in resp['Contents']:
        filename=file_object.get('Key')
        if searchPattern in filename:
            file_list.append(filename)
            obj = s3_client.get_object(Bucket=bucket, Key=filename)
            # Read Excel to Pandas Dataframe
            pdf_excel = pd.read_excel(io.BytesIO(obj['Body'].read()),keep_default_na=False,engine='openpyxl' ,sheet_name=['Small Business','Dealer Fleet'])
            #pdf_excel = pd.read_excel(filepath)
            pdf_1 = pdf_excel.get('Small Business')
            pdf_2 = pdf_excel.get('Dealer Fleet')

            pdf_1 = pdf_1[pdf_1.columns[0:num_of_columns]]
            pdf_1 = pdf_1.applymap(str)

            pdf_2 = pdf_2[pdf_2.columns[0:num_of_columns]]
            pdf_2 = pdf_2.applymap(str)

            #get 1st sheet
            if sdf1 is None:
                sdf1 = spark.createDataFrame(pdf_1)
                sdf1 = sdf1.select(*sdf1.columns)      
            else:      
                sdf1_new = spark.createDataFrame(pdf_1)
                sdf1_new = sdf1_new.select(*sdf1_new.columns)
                sdf1 = sdf1.union(sdf1_new)

            #get 2nd sheet
            if sdf2 is None:
                sdf2 = spark.createDataFrame(pdf_2)
                sdf2 = sdf2.select(*sdf2.columns)      
            else:      
                sdf2_new = spark.createDataFrame(pdf_2)
                sdf2_new = sdf2_new.select(*sdf2_new.columns)
                sdf2 = sdf2.union(sdf2_new)
            found = True
    if found is False:
        print("File not found. Exiting...")
        spark.sparkContext.stop()
        spark.stop() 
        sys.exit()

    sdf1 = sdf1.select(*sdf1.columns)
    sdf2 = sdf2.select(*sdf2.columns)

    return sdf1, sdf2, file_list

#######################################################################################################################

#get dataframe from excel files
month = datetime.now()
def last_day(d):
    next_month = d.replace(day=28) + timedelta(days = 4)
    return next_month - timedelta(days=next_month.day)
FILE_DATE = last_day(month)
FILE_DATE = FILE_DATE.strftime('%Y%m%d')
EXCEL_NAME = 'SB and DF Objective Details ' + FILE_DATE + '.xlsx'
sb_obj_df, df_obj_df, file_list = excel_to_sdf('fleet-manual',EXCEL_NAME,16)
print(sb_obj_df.count())
print(df_obj_df.count())

# sb_obj_df.na.drop(subset=["BAC"])
# df_obj_df.na.drop(subset=["BAC"])
# sb_obj_df = sb_obj_df.where(sb_obj_df["bac"]!='NULL')
# df_obj_df = df_obj_df.where(df_obj_df["bac"]!='NULL')
sb_unadj_obj_df = sb_obj_df 

df_unadj_obj_df = df_obj_df

#Rename SB columns
sb_obj_df = sb_obj_df.withColumnRenamed("SB\nAverage\nRecent\nSales", 'Average_Recent_Sales')
sb_obj_df = sb_obj_df.withColumnRenamed("SB\nAverage\nSeasonal Sales", 'Average_Seasonal_Sales' )
sb_obj_df = sb_obj_df.withColumnRenamed("Share of\nSales", 'Share_of_Sales')
sb_obj_df = sb_obj_df.withColumnRenamed("SB \nUnadjusted Objective", 'SB_Unadjusted_Objective')
sb_obj_df = sb_obj_df.withColumnRenamed("SB\nObjective", 'Objective' )
sb_obj_df = sb_obj_df.withColumnRenamed("Share of\nNational Objective", 'Share_of_National_Objective')
sb_obj_df = sb_obj_df.withColumnRenamed("Zero\nDealer\nAdjusted", 'Zero_Dealer_Adjusted')
sb_obj_df = sb_obj_df.withColumnRenamed("MOR\nAdjusted", 'MOR_Adjusted')
sb_obj_df = sb_obj_df.withColumnRenamed("BE\nDealer", 'BE_Dealer')

sb_unadj_obj_df = sb_unadj_obj_df.withColumnRenamed("SB\nAverage\nRecent\nSales", 'Average_Recent_Sales')
sb_unadj_obj_df = sb_unadj_obj_df.withColumnRenamed("SB\nAverage\nSeasonal Sales", 'Average_Seasonal_Sales' )
sb_unadj_obj_df = sb_unadj_obj_df.withColumnRenamed("Share of\nSales", 'Share_of_Sales')
sb_unadj_obj_df = sb_unadj_obj_df.withColumnRenamed("SB \nUnadjusted Objective", 'SB_Unadjusted_Objective')
sb_unadj_obj_df = sb_unadj_obj_df.withColumnRenamed("SB\nObjective", 'Objective' )
sb_unadj_obj_df = sb_unadj_obj_df.withColumnRenamed("Share of\nNational Objective", 'Share_of_National_Objective')
sb_unadj_obj_df = sb_unadj_obj_df.withColumnRenamed("Zero\nDealer\nAdjusted", 'Zero_Dealer_Adjusted')
sb_unadj_obj_df = sb_unadj_obj_df.withColumnRenamed("MOR\nAdjusted", 'MOR_Adjusted')
sb_unadj_obj_df = sb_unadj_obj_df.withColumnRenamed("BE\nDealer", 'BE_Dealer')

#Rename DF columns
df_obj_df = df_obj_df.withColumnRenamed("DF\nAverage\nRecent\nSales", 'Average_Recent_Sales')
df_obj_df = df_obj_df.withColumnRenamed("DF\nAverage\nSeasonal Sales", 'Average_Seasonal_Sales' )
df_obj_df = df_obj_df.withColumnRenamed("Share of\nSales", 'Share_of_Sales')
df_obj_df = df_obj_df.withColumnRenamed("DF\nUnadjusted Objective", 'DF_Unadjusted_Objective')
df_obj_df = df_obj_df.withColumnRenamed("DF\nObjective", 'Objective' )
df_obj_df = df_obj_df.withColumnRenamed("Share of\nNational Objective", 'Share_of_National_Objective')
df_obj_df = df_obj_df.withColumnRenamed("Zero\nDealer\nAdjusted", 'Zero_Dealer_Adjusted')
df_obj_df = df_obj_df.withColumnRenamed("MOR\nAdjusted", 'MOR_Adjusted')
df_obj_df = df_obj_df.withColumnRenamed("BE\nDealer", 'BE_Dealer')
df_obj_df = df_obj_df.withColumn("ISAS", sf.when(df_obj_df["Model"] == "Trax", "TXX").otherwise(df_obj_df.ISAS))
df_obj_df = df_obj_df.withColumn("ISAS", sf.when(df_obj_df["Model"] == "Chevy Van Cutaway", "EXV").otherwise(df_obj_df.ISAS))
df_obj_df = df_obj_df.withColumn("ISAS", sf.when(df_obj_df["Model"] == "Savana Cutaway", "SAP").otherwise(df_obj_df.ISAS))

df_unadj_obj_df = df_unadj_obj_df.withColumnRenamed("DF\nAverage\nRecent\nSales", 'Average_Recent_Sales')
df_unadj_obj_df = df_unadj_obj_df.withColumnRenamed("DF\nAverage\nSeasonal Sales", 'Average_Seasonal_Sales' )
df_unadj_obj_df = df_unadj_obj_df.withColumnRenamed("Share of\nSales", 'Share_of_Sales')
df_unadj_obj_df = df_unadj_obj_df.withColumnRenamed("DF\nUnadjusted Objective", 'DF_Unadjusted_Objective')
df_unadj_obj_df = df_unadj_obj_df.withColumnRenamed("DF\nObjective", 'Objective' )
df_unadj_obj_df = df_unadj_obj_df.withColumnRenamed("Share of\nNational Objective", 'Share_of_National_Objective')
df_unadj_obj_df = df_unadj_obj_df.withColumnRenamed("Zero\nDealer\nAdjusted", 'Zero_Dealer_Adjusted')
df_unadj_obj_df = df_unadj_obj_df.withColumnRenamed("MOR\nAdjusted", 'MOR_Adjusted')
df_unadj_obj_df = df_unadj_obj_df.withColumnRenamed("BE\nDealer", 'BE_Dealer')
df_unadj_obj_df = df_unadj_obj_df.withColumn("ISAS", sf.when(df_unadj_obj_df["Model"] == "Trax", "TXX").otherwise(df_unadj_obj_df.ISAS))
df_unadj_obj_df = df_unadj_obj_df.withColumn("ISAS", sf.when(df_unadj_obj_df["Model"] == "Chevy Van Cutaway", "EXV").otherwise(df_unadj_obj_df.ISAS))
df_unadj_obj_df = df_unadj_obj_df.withColumn("ISAS", sf.when(df_unadj_obj_df["Model"] == "Savana Cutaway", "SAP").otherwise(df_unadj_obj_df.ISAS))

sb_obj_df = sb_obj_df.withColumn("process_dt", sf.lit('2023-06-01').cast(DateType()))
sb_obj_df = sb_obj_df.withColumn("KPI_Type_Code", sf.lit('711').cast(IntegerType()))
sb_obj_df = sb_obj_df.drop('SB_Unadjusted_Objective')
print(sb_obj_df.count())

sb_unadj_obj_df = sb_unadj_obj_df.withColumn("process_dt", sf.lit('2023-06-01').cast(DateType()))
sb_unadj_obj_df = sb_unadj_obj_df.withColumn("KPI_Type_Code", sf.lit('716').cast(IntegerType()))
sb_unadj_obj_df = sb_unadj_obj_df.drop('Objective')
sb_unadj_obj_df = sb_unadj_obj_df.withColumnRenamed('SB_Unadjusted_Objective', 'Objective')
print(sb_unadj_obj_df.count())

df_obj_df = df_obj_df.withColumn("process_dt", sf.lit('2023-06-01').cast(DateType()))
df_obj_df = df_obj_df.withColumn("KPI_Type_Code", sf.lit('714').cast(IntegerType()))
df_obj_df = df_obj_df.drop('DF_Unadjusted_Objective')
print(df_obj_df.count())

df_unadj_obj_df = df_unadj_obj_df.withColumn("process_dt", sf.lit('2023-06-01').cast(DateType()))
df_unadj_obj_df = df_unadj_obj_df.withColumn("KPI_Type_Code", sf.lit('715').cast(IntegerType()))
df_unadj_obj_df = df_unadj_obj_df.drop('Objective')
df_unadj_obj_df = df_unadj_obj_df.withColumnRenamed('DF_Unadjusted_Objective', 'Objective')
print(df_unadj_obj_df.count())

sb_df_obj_df = sb_obj_df.union(df_obj_df)
sb_df_obj_df = sb_df_obj_df.union(sb_unadj_obj_df)
sb_df_obj_df = sb_df_obj_df.union(df_unadj_obj_df)
sb_df_obj_df = sb_df_obj_df.withColumn("BAC", sf.col("BAC").cast(IntegerType()))
sb_df_obj_df = sb_df_obj_df.withColumn("Average_Recent_Sales", sf.col("Average_Recent_Sales").cast(DecimalType(8,2)))
sb_df_obj_df = sb_df_obj_df.withColumn("Average_Seasonal_Sales", sf.col("Average_Seasonal_Sales").cast(DecimalType(8,2)))
sb_df_obj_df = sb_df_obj_df.withColumn("Objective", sf.col("Objective").cast(IntegerType()))
# sb_df_obj_df = sb_df_obj_df.withColumn("src_sys_uniq_prim_key_val", sf.concat(sf.lit('BAC'), sf.lit('~'),
#                                                                        sf.lit('model'), sf.lit('~'),
#                                                                        sf.lit('process_dt'), sf.lit('~'),
#                                                                        sf.lit('submodel_nm'), sf.lit('~'),
#                                                                        sf.lit('region_short_nm'), sf.lit('~'),
#                                                                        sf.lit('src_sys_id')))

# sb_df_obj_df = sb_df_obj_df.withColumn("src_sys_uniq_prim_key_col_val", sf.concat(sf.col('rpt_dt'), sf.lit('~'),
#                                                                        sf.col('model_nm'), sf.lit('~'),
#                                                                        sf.col('biz_type'), sf.lit('~'),
#                                                                        sf.col('submodel_nm'), sf.lit('~'),
#                                                                        sf.col('region_short_nm'), sf.lit('~'),
#                                                                        sf.col('src_sys_id')))
                                                                       
CURR_TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
sb_df_obj_df = sb_df_obj_df.withColumn("meas_cnt", sf.lit(1))
sb_df_obj_df = sb_df_obj_df.withColumn("dw_anom_flg", sf.lit('U'))
sb_df_obj_df = sb_df_obj_df.withColumn("dw_mod_ts", sf.lit(CURR_TS).cast(TimestampType()))
sb_df_obj_df = sb_df_obj_df.withColumn("src_sys_iud_cd", sf.lit('I'))
sb_df_obj_df = sb_df_obj_df.withColumn("dw_job_id", sf.lit("SF_DF_OBJ_STAGING_HISTORICAL"))
sb_df_obj_df = sb_df_obj_df.na.drop(subset=["BAC"])


#Define Target Table Schema
sb_df_obj_schema = StructType(
    [
        StructField("BAC", LongType(), True),
        StructField("Dealer", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Model", StringType(), True),
        StructField("ISAS", StringType(), True),
        StructField("Average_Recent_Sales", DecimalType(8,2), True),
        StructField("Average_Seasonal_Sales", DecimalType(8,2), True),
        StructField("Share_Of_Sales", StringType(), True),
        StructField("Objective", IntegerType(), True),
        StructField("Share_Of_National_Objective", StringType(), True),
        StructField("Zero_Dealer_Adjusted", StringType(), True),
        StructField("MOR_Adjusted", StringType(), True),
        StructField("BE_Dealer", StringType(), True),
        StructField("Process_Date", DateType(), True),
        StructField("KPI_Type_Code", IntegerType(), True),
        StructField("meas_cnt", IntegerType(), False),
        StructField("dw_anom_flg", StringType(), False),
        StructField("dw_mod_ts", TimestampType(), False),
        StructField("src_sys_iud_cd", StringType(), False),
        StructField("dw_job_id", StringType(), False)
    ]
)
sb_df_obj_df = spark.createDataFrame(sb_df_obj_df.rdd, sb_df_obj_schema)

print('Writing to CS_DF_OBJ_STG table started ', str(datetime.now()))
BASE_TABLE_NAME = "dl_edge_base_dcfpa_171749_base_dcfpa_avista.obj_sb_df_stg"
BASE_TABLE_PATH = get_hive_table_path(BASE_TABLE_NAME)
write_to_hive_table(sb_df_obj_df, BASE_TABLE_NAME, BASE_TABLE_PATH, "append")

#print('Writing to CS_DF_OBJ_STG table started ', str(datetime.now()))
#sb_df_obj_df.write.mode("append").format("parquet").saveAsTable(TABLE_NAME2, path=TABLE_PATH2)
print('Writing to CS_DF_OBJ_STG table completed ', str(datetime.now()))
# # # Archive files
CURR_TS = datetime.now().strftime("%Y%m%d%H%M%S")
#gm_s3.archive_files(s3_client, 'business-elite','business-elite-archive', 'Bus_Elite_BAC', postfix='_'+CURR_TS)

print('COMPLETE - SUCCESSFUL')
spark.sparkContext.stop()
spark.stop() 