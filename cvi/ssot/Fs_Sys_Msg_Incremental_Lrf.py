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
import boto3  # ECS Import
from botocore.client import Config  # ECS Import
import pytz

############################################################################
# PARSING ARGUMENTS
############################################################################

ENV = sys.argv[1]
if ENV == "DEV" or ENV == "TST":
    RPT_DT = '2023-05-25'
else:
    RPT_DT = sys.argv[2]

matched = re.match("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]", RPT_DT)
is_match = bool(matched)
print(is_match)

if is_match:
    rpt_dt = datetime.strptime(RPT_DT, '%Y-%m-%d').date()
else:
    rpt_dt = datetime.today().date()
print(rpt_dt)


#############################################################################
# START SPARK
#############################################################################
spark = (
    SparkSession.builder
    .enableHiveSupport()
    .getOrCreate()
)
print(f"Spark Version: {spark.version}")

#############################################################################
# DEFINING FUNCTIONS & CONSTANTS
#############################################################################

if ENV == "DEV":
    tableEnv = '{ENV}_'.format(ENV=ENV).lower()
elif ENV == "TST" or ENV == "CRT" or ENV == "PRD":
    tableEnv = ""
else:
    raise Exception('Environment Variable NOT passed properly')

# TABLE NAME & PATH IN HDFS
LRF_TABLE_NAME = tableEnv + 'sales_mktg360_processing.Fs_Sys_Msg_Lrf'
LRF_TABLE_PATH = f'/sync/{ENV.lower()}_42124_edw_b/EDW/SALES_MKTG360/RZ/FS_SYS_MSG_LRF/Processing'
STG_TABLE_NAME = tableEnv + 'dl_edge_base_dcfpa_171749_base_dcfpa_avista.gbl_sys_msg_info_stg'
EDA_TABLE_NAME = "marketing360_public.Fs_Sys_Msg"


def write_to_table(lrf_df, lrf_table_name, lrf_table_path):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Writing to {lrf_table_name} started")
    try:
        lrf_df.write.mode("overwrite").parquet(lrf_table_path)
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Writing to {lrf_table_name} completed")
    except Exception as e:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Writing to {lrf_table_name} failed")
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {e}")

    lrf_df.unpersist()


#############################################################################
# CREATE LRF DATAFRAME
#############################################################################

eda_df = spark.sql(f"""
                SELECT *
                FROM {eda_table}
                WHERE eff_dt >= {rpt_dt}
                """).cache()

lrf_df = spark.sql(f"""
                SELECT *
                FROM {stg_table}
                WHERE eff_dt >= {rpt_dt}
                  AND last_upd_dt IN
                    (SELECT MAX(last_upd_dt)
                     FROM {stg_table}
                     WHERE eff_dt >= {rpt_dt}
                """).cache()

#############################################################################
# ADDING PLUMBING COLUMNS
#############################################################################

lrf_df = lrf_df.withColumn("Src_Sys_Id", lit("171749"))
lrf_df = lrf_df.withColumn("Src_Sys_Uniq_Prim_Key_Val", lit("NTL_CD|SYS_CD|SYS_DESC|SYS_MSG_IDENTITY|SRC_SYS_ID"))
lrf_df = lrf_df.withColumn("Src_Sys_Uniq_Prim_Key_Col_Val",
                           concat(col("NTL_CD"), lit("|"), col("SYS_CD"), lit("|"), col("SYS_DESC"), lit("|"),
                                  col("SYS_MSG_IDENTITY"), lit("|"), col("SRC_SYS_ID")))
lrf_df = lrf_df.withColumn("Meas_Cnt", lit(1))
lrf_df = lrf_df.withColumn("Dw_Anom_Flg", lit("N"))
lrf_df = lrf_df.withColumn("Dw_Mod_Ts", current_timestamp())
lrf_df = lrf_df.withColumn("Dw_Job_Id", lit("Fs_Sys_Msg"))


#############################################################################
# PERFORMING IUD TRANSFORMATIONS
#############################################################################

i_df = lrf_df.join(eda_df, on=["NTL_CD", "SYS_CD", "SYS_DESC", "SYS_MSG_IDENTITY", "SRC_SYS_ID"],
                   how="left_anti").withColumn("SRC_SYS_IUD_CD",
                                               lit("I"))
u_df = lrf_df.join(eda_df, on=["NTL_CD", "SYS_CD", "SYS_DESC", "SYS_MSG_IDENTITY", "SRC_SYS_ID"],
                   how="left_semi").withColumn("SRC_SYS_IUD_CD",
                                               lit("U"))
d_df = eda_df.join(lrf_df, on=["NTL_CD", "SYS_CD", "SYS_DESC", "SYS_MSG_IDENTITY", "SRC_SYS_ID"],
                   how="left_anti").withColumn("SRC_SYS_IUD_CD",
                                               lit("D"))
lrf_df = i_df.unionByName(u_df).unionByName(d_df)


#############################################################################
# REORDER COLUMN POSITION
#############################################################################
lrf_df.select(col('ntl_cd'),
              col('sys_cd'),
              col('sys_desc'),
              col('sys_msg_identity'),
              col('Src_Sys_Id'),
              col('msg_txt'),
              col('eff_dt'),
              col('exp_dt'),
              col('msg_type'),
              col('last_upd_usr_identity'),
              col('last_upd_dt'),
              col('Src_Sys_Uniq_Prim_Key_Val'),
              col('Src_Sys_Uniq_Prim_Key_Col_Val'),
              col('Meas_Cnt'),
              col('Dw_Anom_Flg'),
              col('Dw_Mod_Ts'),
              col('Dw_Job_Id'),
              col('Src_Sys_Iud_Cd'))
lrf_df.show(5)

###############################################################################
# WRITE TO LRF TABLE
###############################################################################
write_to_table(union_lrf_df, LRF_TABLE_NAME, LRF_TABLE_PATH)

spark.stop()
sys.exit(0)
