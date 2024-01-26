#############################################################################
# IMPORTS
#############################################################################
from datetime import date, datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import re
import sys


#############################################################################
# PARSING ARGUMENTS
#############################################################################
ENV = sys.argv[1]
#RPT_DT = sys.argv[2]


#############################################################################
# INSTANTIATING SPARK SESSION
#############################################################################
spark = (
    SparkSession.builder
    .enableHiveSupport()
    .getOrCreate()
    )
print(f"Spark Version: {spark.version}")

today = date.today()
today_year = today.strftime("%Y")
today_month = today.strftime("%m")
rpt_dt =  today.replace(day=1)
#############################################################################
# DEFINING FUNCTIONS
#############################################################################
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
# DEFINING CONSTANTS & VARIABLES
#############################################################################
EDA_TABLE_NAME = "marketing360_public.FS_DLR_FRCST"
LRF_TABLE_NAME = "sales_mktg_processing.CDS_DLR_FRCST_LRF"
#LRF_TABLE_NAME = "sales_mktg.cds_dlr_frcst"
LRF_TABLE_PATH = f"/sync/{ENV.lower()}_42124_edw_b/EDW/SALES_MKTG/RZ/CDS_DLR_FRCST_LRF/Processing"
#LRF_TABLE_PATH = f"/sync/{ENV.lower()}_42124_edw_b/EDW/SALES_MKTG/RZ/CDS_DLR_FRCST/Data"
# matched = re.match("[0-9][0-9][0-9][0-9]-[0][1]-[0-9][0-9]", RPT_DT)
# is_match = bool(matched)

# if is_match:
#     rpt_dt = datetime.strptime(RPT_DT, '%Y-%m-%d').date() - timedelta(days=1)
# else:
#     rpt_dt = datetime.today().date() - timedelta(days=1)


#############################################################################
# CREATING INITIAL DATAFRAME
#############################################################################
eda_df = spark.sql(f"""
                    SELECT *
                    FROM {EDA_TABLE_NAME}
                    WHERE frcst_rpt_dt  = '{rpt_dt}'
                    """).cache()

lrf_df = spark.sql(f"""
                    With kpi_info as (
select distinct kpi_type_cd,
metric_short_desc,
metric_long_desc
from 
marketing360_public.fs_vsna_kpi_types),
mdl_info as (
select distinct isas_brand_cd,
sell_src,
fcst_modl_cd,
fcst_modl_desc
FROM 
dev_marketing360_public.fs_vsna_dim_dsr_isas_dim)

select process_date as frcst_rpt_dt,
'US' as ctry_cd,
bac as biz_assoc_cd,
mdl_info.sell_src as sell_src_cd,
mdl_info.fcst_modl_cd as frcst_model_cd,
stg_tbl.dw_mod_ts as data_upd_dt, 
kpi_type_code as kpi_type_cd,
case
    when zero_dealer_adjusted = 'X' then 'Y'
    else 'N'
end as zero_adj_flg,
objective as frcst_cnt,
mdl_info.fcst_modl_desc as frcst_model_desc,
kpi_info.metric_short_desc as kpi_metric_short_desc, 
kpi_info.metric_long_desc as kpi_metric_long_desc,
isas as isas_brand_cd
from dl_edge_base_dcfpa_171749_base_dcfpa_avista.obj_sb_df_stg stg_tbl
left join 
mdl_info
on stg_tbl.isas = mdl_info.isas_brand_cd
left join 
kpi_info
on stg_tbl.kpi_type_code = kpi_info.kpi_type_cd
                    """).cache()

lrf_df = lrf_df.withColumnRenamed("Ctry_Cd", "Iso_Ctry_Cd")
#############################################################################
# ADDING PLUMBING COLUMNS
#############################################################################
lrf_df = lrf_df.withColumn("Src_Sys_Id", lit("335")) # DELETE IF SRC_SYS_ID IS BEING PULLED IN FROM SQL
lrf_df = lrf_df.withColumn("Src_Sys_Uniq_Prim_Key_Val", concat(col('Biz_Assoc_Cd'), lit('|'),
                                                                        col('Iso_Ctry_Cd'), lit('|'),                                                                        
                                                                        col('Data_Upd_Dt'), lit('|'),
                                                                        col('Frcst_Model_Cd'), lit('|'),
                                                                        col('Frcst_Rpt_Dt'), lit('|'),
                                                                        col('Kpi_Type_Cd'), lit('|'),
                                                                        col('Sell_Src_Cd'), lit('|'),
                                                                        col('Src_Sys_Id'), lit('|')
                                                                        ))

lrf_df = lrf_df.withColumn("Src_Sys_Uniq_Prim_Key_Col_Val", concat(col('Biz_Assoc_Cd'), lit('|'),
                                                                        lit('Iso_Ctry_Cd'), lit('|'),                                                                        
                                                                        lit('Data_Upd_Dt'), lit('|'),
                                                                        lit('Frcst_Model_Cd'), lit('|'),
                                                                        lit('Frcst_Rpt_Dt'), lit('|'),
                                                                        lit('Kpi_Type_Cd'), lit('|'),
                                                                        lit('Sell_Src_Cd'), lit('|'),
                                                                        lit('Src_Sys_Id'), lit('|')
                                                                        ))
lrf_df = lrf_df.withColumn("Meas_Cnt", lit(1))
lrf_df = lrf_df.withColumn("Dw_Anom_Flg", lit("N"))
CURR_TS = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
lrf_df = lrf_df.withColumn("Dw_Mod_Ts",  lit(CURR_TS).cast(TimestampType()))
lrf_df = lrf_df.withColumn("Dw_Job_Id", lit("42124-edw-prd-sm-dcfpa-flt-dlr-frcst-lrf"))

#############################################################################
# PERFORMING IUD TRANSFORMATIONS
#############################################################################
i_df = lrf_df.join(eda_df, on=["FRCST_RPT_DT", "ISO_CTRY_CD", "BIZ_ASSOC_CD", "SELL_SRC_CD", "FRCST_MODEL_CD", "DATA_UPD_DT", "KPI_TYPE_CD", "SRC_SYS_ID"], how="left_anti").withColumn("SRC_SYS_IUD_CD", lit("I"))
u_df = lrf_df.join(eda_df, on=["FRCST_RPT_DT", "ISO_CTRY_CD", "BIZ_ASSOC_CD", "SELL_SRC_CD", "FRCST_MODEL_CD", "DATA_UPD_DT", "KPI_TYPE_CD", "SRC_SYS_ID"], how="left_semi").withColumn("SRC_SYS_IUD_CD", lit("U"))
d_df = eda_df.join(lrf_df, on=["FRCST_RPT_DT", "ISO_CTRY_CD", "BIZ_ASSOC_CD", "SELL_SRC_CD", "FRCST_MODEL_CD", "DATA_UPD_DT", "KPI_TYPE_CD", "SRC_SYS_ID"], how="left_anti").withColumn("SRC_SYS_IUD_CD", lit("D"))

lrf_df = i_df.unionByName(u_df).unionByName(d_df)


#############################################################################
# CASTING DATA TYPES & APPLYING TABLE SCHEMA
#############################################################################
lrf_df = lrf_df.select(
    col("FRCST_RPT_DT").cast(DateType()),
    col("ISO_CTRY_CD").cast(StringType()),
    col("BIZ_ASSOC_CD").cast(LongType()),
    col("SELL_SRC_CD").cast(IntegerType()),
    col("FRCST_MODEL_CD").cast(IntegerType()),
    col("DATA_UPD_DT").cast(TimestampType()),
    col("KPI_TYPE_CD").cast(IntegerType()),
    col("SRC_SYS_ID").cast(StringType()),
    col("ZERO_ADJ_FLG").cast(StringType()),
    col("FRCST_CNT").cast(IntegerType()),
    col("FRCST_MODEL_DESC").cast(StringType()),
    col("ISAS_BRAND_CD").cast(StringType()),
    col("KPI_METRIC_SHORT_DESC").cast(StringType()),
    col("KPI_METRIC_LONG_DESC").cast(StringType()),
    col("SRC_SYS_UNIQ_PRIM_KEY_VAL").cast(StringType()),
    col("SRC_SYS_UNIQ_PRIM_KEY_COL_VAL").cast(StringType()),
    col("MEAS_CNT").cast(IntegerType()),
    col("DW_ANOM_FLG").cast(StringType()),
    col("DW_MOD_TS").cast(TimestampType()),
    col("DW_JOB_ID").cast(StringType()),
    col("SRC_SYS_IUD_CD").cast(StringType())
)


#############################################################################
# WRITING TO LRF TABLE
#############################################################################
write_to_table(lrf_df, LRF_TABLE_NAME, LRF_TABLE_PATH)

spark.stop()
sys.exit(0)
