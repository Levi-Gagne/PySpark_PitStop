#!/usr/bin/env python
# coding: utf-8


SCRIPT_NAME = "CDS_RET_CRTSY_TRP_PGM"

TIDAL_NAME  = "42124-edw-prd-sm-dcfpa-dsr-ret-crtsy-trp-pgm-lrf"


"""
CDS_RET_CRTSY_TRP_PGM_LRF-HIS.py
Auth: Levi Gagne
Date: 2023-10-01

1. Import Libraries:
    - datetime: Date and time manipulation
    - sys: System-specific parameters
    - PySpark: Spark functionalities

2. Initialize Spark:
    - SparkSession with custom configurations for performance

3. Environment Variables:
    - Parse command-line argument for environment setting

4. Source Tables:
    - Define 'RetC_TABLE', 'M360_TABLE', and 'V360_TABLE'

5. Target Table: 'sales_mktg.cds_ret_crtsy_trp_pgm'
    a. Variables: 'TARGET_TABLE_NAME' & 'TARGET_TABLE_PATH'
    b. Schema: 'TARGET_TABLE_SCHEMA'

6. SQL Query:
    - SQL for data extraction from source tables
    a. Variables: 'CT_HISTORICAL_Query'

7. Data Transformation:
    - Execute SQL, load into DataFrame
    a. Operations: Type casting, column renaming

8. Data Validation:
    - Exception handling for query execution
    a. Variables: 'Exception as e'

9. Write to Target:
    - Write DataFrame to target table in parquet
    a. Options: Overwrite mode

10. Logging:
    - Capture runtime events, errors
    a. Variables: 'print(f"An unexpected error occurred: {e}")'

11. Cleanup:
    - Release resources, terminate Spark session
    a. Variables: 'spark.stop()'

12. Exit:
    - End of script, return exit code
    a. Variables: 'sys.exit(2)'
"""


# ============================================================================
# Imports
# ============================================================================
# PYTHON STANDARD LIBRARY
from datetime import datetime, timedelta
import sys
# THIRD-PARTY IMPORTS
from pyspark.sql import SparkSession
from pyspark.sql import functions as F



# ============================================================================
# Initialize Spark
# ============================================================================
spark = (SparkSession.builder
         .appName(f"{SCRIPT_NAME}")
         # Memory configurations
         .config("spark.driver.memory", "16g")
         .config("spark.executor.memory", "16g")
         .config("spark.executor.memoryOverhead", "4g")  # Adjust as needed
         # Serialization
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .config("spark.kryoserializer.buffer.max", "512m")
         # Shuffle and parallelism configurations
         .config("spark.sql.shuffle.partitions", "300")  # Adjust based on your data size and cluster capacity
         .config("spark.default.parallelism", "300")
         # Dynamic allocation settings
         .config("spark.dynamicAllocation.enabled", "true")
         .config("spark.dynamicAllocation.initialExecutors", "2")
         .config("spark.dynamicAllocation.minExecutors", "1")
         .config("spark.dynamicAllocation.maxExecutors", "20")
         # Time zone setting
         .config("spark.sql.session.timeZone", "EST")
         # Enable Hive support if needed
         .enableHiveSupport()
         .getOrCreate())
spark.sparkContext.setLogLevel('ERROR')

print(f"Spark Session: {spark.version}")
print(f"Python Version: {sys.version}")


# ============================================================================
# Initilize Variables
# ============================================================================	
# Directly using the first argument as the environment
ENV = sys.argv[1].lower()

#   -------------------      Source-Tables      -------------------  #
RetC_TABLE = "dl_edge_base_dcfpa_171749_base_dcfpa_avista.ret_crtsy_trp_pgm_stg"
M360_TABLE = "marketing360_public.fs_vsna_dim_selling_day_report_lkup"
V360_TABLE = "vehicle360_avista.fs_daily_veh_deliv"

# Assuming you've read your data into DataFrames
df_RetC = spark.read.table(RetC_TABLE)
df_M360 = spark.read.table(M360_TABLE)
df_V360 = spark.read.table(V360_TABLE)

# Create temporary tables
df_RetC.createOrReplaceTempView("temp_RetC")
df_M360.createOrReplaceTempView("temp_M360")
df_V360.createOrReplaceTempView("temp_V360")
#   -------------------      Source-Tables      -------------------  #


#   -------------------      Target-Table       -------------------  #
TARGET_TABLE_NAME = "sales_mktg.cds_ret_crtsy_trp_pgm"
TARGET_TABLE_PATH = f"/sync/{ENV}_42124_edw_b/EDW/SALES_MKTG/RZ/CDS_RET_CRTSY_TRP_PGM/Processing"

# Target-Target-Schema
TARGET_TABLE_SCHEMA = [
    "Veh_Id_Nbr",
    "File_Dt",
    "Out_Svc_Dt",
    "Src_Sys_Id",
    "In_Svc_Dt",
    "Biz_Assoc_Id",
    "Day_In_Svc_Cnt",
    "Out_Svc_Mil_Cnt",
    "Out_Svc_Type_Cd",
    "Sell_Src_Desc",
    "Sell_Src_Cd",
    "Veh_Prod_Rpt_Model_Cd",
    "Model_Yr",
    "Loan_Great_Than_60_Day_Cd",
    "Src_Sys_Uniq_Prim_Key_Val",
    "Src_Sys_Uniq_Prim_Key_Col_Val",
    "Meas_Cnt",
    "Dw_Anom_Flg",
    "Dw_Mod_Ts",
    "Dw_Job_Id",
    "Src_Sys_Iud_Cd",
]

# Target-Table-Plumbing-Columns
SRC_SYS_ID     = F.lit("171749")
MEAS_CNT       = F.lit(1)
DW_ANOM_FLG    = F.lit("N")
CURRENT_TS     = F.current_timestamp()
DW_JOB_ID      = F.lit(f"{TIDAL_NAME}")
SRC_SYS_IUD_CD = F.lit("I")
SRC_SYS_UNIQ_PRIM_KEY_VAL     = F.concat(F.col("Veh_Id_Nbr"), F.lit("|"), F.col("File_Dt"), F.lit("|"), F.col("Out_Svc_Dt"), F.lit("|"), SRC_SYS_ID)
SRC_SYS_UNIQ_PRIM_KEY_COL_VAL = F.lit("Veh_Id_Nbr|File_Dt|Out_Svc_Dt|Src_Sys_Id")
#   -------------------      Target-Table       -------------------  #



CT_HISTORICAL_SQL = """
             ------  COURTESY TRANSPORTATION CDS  ------

           -----       HISTORICAL SQL     -----

        With rtl_sales_thru as (
        select salesthroughdate as sell_dt, --Sales thru Date
        date_sub(to_date(salesthroughdate),1) as prev_dt,
        currentmonthcumselldays, -- Current month cummulative selling days count
        currentmonthmthstart, currentmonthmthend,--Current Months
        priormthcurrentday, priormthstart, priormthend, --Prior Months
        prioryearmthstart, prioryearmthend, prioryearstart, prioryearcurrentday, --Prior Years
        currentyearstart --Current Year
        from temp_M360
        where salesthroughdate = date_sub(to_date(current_date),1))
        , --this query grabs the rtl_sales_thru date ranges and selling day count

          Stg_Out_Serv as (
          SELECT vin, outdate, todaydate, daysinservice, outmile, outtype, loangreaterthan60,

          row_number() over 
        (partition by vin || outdate || daysinservice || outmile
        order by todaydate desc) row_num -- identify duplicate entries and choose most recent file date as row 1

          FROM temp_RetC
          ), -- pull all data from staging table and identify duplicates


        In_Serv_Dtl as (
        Select
        DISTINCT f.veh_id_nbr,
        f.orig_proc_dt,
         f.biz_assoc_id,
        f.model_yr,
        case f.sell_src_cd
        when 11 then 'Buick'
        when 12 then 'Cadillac'
        when 13 then 'Chevrolet'
        when 48 then 'GMC'
        end as selling_source,
        f.sell_src_cd,
        f.veh_prod_rpt_model_cd,
        f.deliv_type_ctgy,
        row_number() over 
        (partition by f.veh_id_nbr || f.biz_assoc_id
        order by f.orig_proc_dt asc) row_num -- identify all process dates for CTP event/vin

        from
        temp_V360 f, rtl_sales_thru


        where
        f.ntl_cd = 'US' --reporting country
           and f.kpi_type_cd in (25,95)
        and f.sell_src_cd in (11,12,13,48) --Buick, Cadillac, Chevrolet, GMC
        and f.orig_proc_dt <= rtl_sales_thru.sell_dt
        and datediff(to_date(rtl_sales_thru.prioryearstart), f.orig_proc_dt) < 365 -- no records earlier than Jan 2021
          and f.veh_deliv_cnt = 1

        ), -- pull all CTP records (+1) back to January 2021

        Dlr_Seq as (
        Select
        veh_id_nbr,
        orig_proc_dt,
        biz_assoc_id,
        model_yr,
        selling_source,
        sell_src_cd,
        veh_prod_rpt_model_cd,
        row_number() over 
        (partition by veh_id_nbr
        order by orig_proc_dt asc) in_dlr_num,  -- first dealer with CTP event for vin
        row_number() over 
        (partition by veh_id_nbr
        order by orig_proc_dt desc) out_dlr_num -- most recent dealer with CTP event for vin


        from In_Serv_Dtl

        where row_num = 1 -- First process date for Dlr/CTP event

        ),  --Orders Dlr/VIN events

        In_Serv_Dlr_Dtl as (

        Select

        'In_Dlr' as category,
        Stg_Out_Serv.vin,
        incpt.orig_proc_dt,
        Stg_Out_Serv.outdate,
        Stg_Out_Serv.daysinservice,
        Stg_Out_Serv.todaydate,
        incpt.biz_assoc_id,
        incpt.selling_source, 
        incpt.sell_src_cd,
        incpt.veh_prod_rpt_model_cd,
        incpt.model_yr,
        Stg_Out_Serv.outmile,
        Stg_Out_Serv.outtype,
        Stg_Out_Serv.loangreaterthan60,
        row_number() over 
        (partition by vin
        order by todaydate asc) dlr_seq_num -- -- number the sequence of events per vin by file date

        from
           Stg_Out_Serv

        left join
        Dlr_Seq incpt
        on incpt.veh_id_nbr = Stg_Out_Serv.vin

        where Stg_Out_Serv.todaydate <= date_sub(to_date(current_date),1)
        and Stg_Out_Serv.row_num = 1-- eliminate duplicate ontrac record and choose most recent record
        and incpt.in_dlr_num = 1-- select first dealer/bac with CTP event for vin
        and Stg_Out_Serv.outdate > incpt.orig_proc_dt   -- outdate must occur after process date

        group by

        'In_Dlr',
        Stg_Out_Serv.vin,
        incpt.orig_proc_dt,
        Stg_Out_Serv.outdate,
        Stg_Out_Serv.daysinservice,
        Stg_Out_Serv.todaydate,
        incpt.biz_assoc_id,
        incpt.selling_source, 
        incpt.sell_src_cd,
        incpt.veh_prod_rpt_model_cd,
        incpt.model_yr,
        Stg_Out_Serv.outmile,
        Stg_Out_Serv.outtype,
        Stg_Out_Serv.loangreaterthan60

        ),--Record for first dealer/first OnTrac record

        Out_Serv_Dlr_Dtl as (
        Select
        'Out_Dlr' as category,
        Stg_Out_Serv.vin,
        outcpt.orig_proc_dt,
        Stg_Out_Serv.outdate,
        Stg_Out_Serv.daysinservice,
        Stg_Out_Serv.todaydate,
        outcpt.biz_assoc_id,
        outcpt.selling_source, 
        outcpt.sell_src_cd,
        outcpt.veh_prod_rpt_model_cd,
        outcpt.model_yr,
        Stg_Out_Serv.outmile,
        Stg_Out_Serv.outtype,
        Stg_Out_Serv.loangreaterthan60,
        row_number() over 
        (partition by vin
        order by todaydate desc) dlr_seq_num -- number the sequence of events per vin by file date

        from
           Stg_Out_Serv

        left join
        Dlr_Seq outcpt
        on outcpt.veh_id_nbr = Stg_Out_Serv.vin

        where Stg_Out_Serv.todaydate <= date_sub(to_date(current_date),1)
        and Stg_Out_Serv.row_num = 1-- eliminate duplicate ontrac record and choose most recent record
        and outcpt.out_dlr_num = 1-- select last dealer/bac with CTP event for vin
        and Stg_Out_Serv.outdate > outcpt.orig_proc_dt   -- outdate must occur after process date

        group by
        'Out_Dlr',
        Stg_Out_Serv.vin,
        outcpt.orig_proc_dt,
        Stg_Out_Serv.outdate,
        Stg_Out_Serv.daysinservice,
        Stg_Out_Serv.todaydate,
        outcpt.biz_assoc_id,
        outcpt.selling_source, 
        outcpt.sell_src_cd,
        outcpt.veh_prod_rpt_model_cd,
        outcpt.model_yr,
        Stg_Out_Serv.outmile,
        Stg_Out_Serv.outtype,
        Stg_Out_Serv.loangreaterthan60
        ), -- chooses most recent Dlr/CT record

        Comb_CT_Dlr as (

        select
        category,
        vin,
        orig_proc_dt,
        outdate,
        daysinservice,
        todaydate,
        biz_assoc_id,
        selling_source, 
        sell_src_cd,
        veh_prod_rpt_model_cd,
        model_yr,
        outmile,
        outtype,
        loangreaterthan60,
        dlr_seq_num

        From In_Serv_Dlr_Dtl 

        where dlr_seq_num = 1   -- choose first record for sequence of retired out event

        group by
        category,
        vin,
        orig_proc_dt,
        outdate,
        daysinservice,
        todaydate,
        biz_assoc_id,
        selling_source, 
        sell_src_cd,
        veh_prod_rpt_model_cd,
        model_yr,
        outmile,
        outtype,
        loangreaterthan60,
        dlr_seq_num

        UNION

        select
        category,
        vin,
        orig_proc_dt,
        outdate,
        daysinservice,
        todaydate,
        biz_assoc_id,
        selling_source, 
        sell_src_cd,
        veh_prod_rpt_model_cd,
        model_yr,
        outmile,
        outtype,
        loangreaterthan60,
        dlr_seq_num

        From Out_Serv_Dlr_Dtl  

        where 
        vin || biz_assoc_id || todaydate not in (select vin || biz_assoc_id || todaydate FROM In_Serv_Dlr_Dtl where dlr_seq_num = 1) 
        and vin || outdate || todaydate not in (select vin || outdate || todaydate FROM In_Serv_Dlr_Dtl where dlr_seq_num = 1)
        -- chooses all other retired out event records excluding the first record to identify any changes in bac or outdate

        group by
        category,
        vin,
        orig_proc_dt,
        outdate,
        daysinservice,
        todaydate,
        biz_assoc_id,
        selling_source, 
        sell_src_cd,
        veh_prod_rpt_model_cd,
        model_yr,
        outmile,
        outtype,
        loangreaterthan60,
        dlr_seq_num

          ) -- give full list of retired out records with corresponding bac and vehicle model info

         select
        vin as veh_id_nbr,
        todaydate as file_dt,
        orig_proc_dt as in_svc_dt,
        outdate as out_svc_dt,
        biz_assoc_id as biz_assoc_id,
        daysinservice as day_in_svc_cnt,
        outmile as out_svc_mil_cnt,
        outtype as out_svc_type_cd,
        selling_source as sell_src_desc, 
        sell_src_cd as sell_src_cd,
        veh_prod_rpt_model_cd as veh_prod_rpt_model_cd,
        model_yr as model_yr,
        loangreaterthan60 as loan_great_than_60_day_cd

        From Comb_CT_Dlr

        group by
        vin,
        todaydate,
        orig_proc_dt,
        outdate,
        biz_assoc_id,
        daysinservice,
        outmile,
        outtype,
        selling_source, 
        sell_src_cd,
        veh_prod_rpt_model_cd,
        model_yr,
        loangreaterthan60
        ---- final column names -----

          -----       HISTORICAL SQL     -----

         ------  COURTESY TRANSPORTATION CDS  ------


"""


CT_INCREMENTAL_SQL = """

     ------  COURTESY TRANSPORTATION CDS  ------

       -----       INCREMENTAL SQL     -----

    With rtl_sales_thru as (
    select salesthroughdate as sell_dt, --Sales thru Date
    date_sub(to_date(salesthroughdate),1) as prev_dt,
    currentmonthcumselldays, -- Current month cummulative selling days count
    currentmonthmthstart, currentmonthmthend,--Current Months
    priormthcurrentday, priormthstart, priormthend, --Prior Months
    prioryearmthstart, prioryearmthend, prioryearstart, prioryearcurrentday, --Prior Years
    currentyearstart --Current Year
    from temp_M360 
    where salesthroughdate = date_sub(to_date(current_date),1))
    , --this query grabs the rtl_sales_thru date ranges and selling day count

      Inc_Stg_Out_Serv as (
      SELECT vin, outdate, todaydate, daysinservice, outmile, outtype, loangreaterthan60,

      row_number() over 
    (partition by vin || outdate || daysinservice || outmile
    order by todaydate asc) row_num -- identify duplicate entries and choose most recent file date as row 1

      FROM temp_RetC

      ),-- pull all data from staging table and identify duplicates

    In_Serv_Dtl as (
    Select
    DISTINCT f.veh_id_nbr,
    f.orig_proc_dt,
     f.biz_assoc_id,
    f.model_yr,
    case f.sell_src_cd
    when 11 then 'Buick'
    when 12 then 'Cadillac'
    when 13 then 'Chevrolet'
    when 48 then 'GMC'
    end as selling_source,
    f.sell_src_cd,
    f.veh_prod_rpt_model_cd,
    f.deliv_type_ctgy,
    row_number() over 
    (partition by f.veh_id_nbr || f.biz_assoc_id
    order by f.orig_proc_dt asc) row_num-- identify all process dates for CTP event/vin

    from
    temp_V360 f, rtl_sales_thru

    where
    f.ntl_cd = 'US' --reporting country
       and f.kpi_type_cd in (25,95)
    and f.sell_src_cd in (11,12,13,48) --Buick, Cadillac, Chevrolet, GMC
    and f.orig_proc_dt <= rtl_sales_thru.sell_dt
    and datediff(to_date(rtl_sales_thru.prioryearstart), f.orig_proc_dt) < 365  -- no records earlier than Jan 2021
      and f.veh_deliv_cnt = 1

    ),  -- pull all CTP records (+1) back to January 2021

    Inc_Dlr_Seq as (
    Select
    veh_id_nbr,
    orig_proc_dt,
    biz_assoc_id,
    model_yr,
    selling_source,
    sell_src_cd,
    veh_prod_rpt_model_cd,
    row_number() over 
    (partition by veh_id_nbr
    order by orig_proc_dt desc) out_dlr_num -- most recent dealer with CTP event for vin 

    from In_Serv_Dtl

    where row_num = 1   -- First process date for Dlr/CTP event 

    ),  --Orders Dlr/VIN events


    Inc_Out_Serv_Dlr_Dtl as (
    Select
    isos.vin,
    outcpt.orig_proc_dt,
    isos.outdate,
    isos.daysinservice,
    isos.todaydate,
    outcpt.biz_assoc_id,
    outcpt.selling_source, 
    outcpt.sell_src_cd,
    outcpt.veh_prod_rpt_model_cd,
    outcpt.model_yr,
    isos.outmile,
    isos.outtype,
    isos.loangreaterthan60,
    row_number() over 
    (partition by vin
    order by todaydate desc) dlr_seq_num-- number the sequence of events per vin by file date

    from
       Inc_Stg_Out_Serv isos

    left join
    Inc_Dlr_Seq outcpt
    on outcpt.veh_id_nbr = isos.vin

    where isos.todaydate = to_date(current_date)
    and isos.row_num = 1-- eliminate duplicate ontrac record and choose most recent record
    and outcpt.out_dlr_num = 1  -- select last dealer/bac with CTP event for vin
    and isos.outdate > outcpt.orig_proc_dt   -- outdate must occur after process date


    )   -- chooses most recent Dlr/CT record

    select
    vin as veh_id_nbr,
    todaydate as file_dt,
    orig_proc_dt as in_svc_dt,
    outdate as out_svc_dt,
    biz_assoc_id as biz_assoc_id,
    daysinservice as day_in_svc_cnt,
    outmile as out_svc_mil_cnt,
    outtype as out_svc_type_cd,
    selling_source as sell_src_desc, 
    sell_src_cd as sell_src_cd,
    veh_prod_rpt_model_cd as veh_prod_rpt_model_cd,
    model_yr as model_yr,
    loangreaterthan60 as loan_great_than_60_day_cd

    From Inc_Out_Serv_Dlr_Dtl  

    group by

    vin,
    todaydate,
    orig_proc_dt,
    outdate,
    biz_assoc_id,
    daysinservice,
    outmile,
    outtype,
    selling_source, 
    sell_src_cd,
    veh_prod_rpt_model_cd,
    model_yr,
    loangreaterthan60
    ---- final column names -----

      -----       INCREMENTAL SQL     -----

     ------  COURTESY TRANSPORTATION CDS  ------

"""


# Apply Transformations
try:
    df_historical = spark.sql(CT_HISTORICAL_SQL)
    df_incremental = spark.sql(CT_INCREMENTAL_SQL)
    df_combined = df_historical.union(df_incremental)

    transformed_df = (df_combined
                      # Transform Source Data
                      .withColumn("Model_yr", F.col("Model_Yr").cast("int"))
                      # Plumbing Columns
                      .withColumn("Src_Sys_Id", SRC_SYS_ID)
                      .withColumn("Src_Sys_Uniq_Prim_Key_Val", SRC_SYS_UNIQ_PRIM_KEY_VAL)
                      .withColumn("Src_Sys_Uniq_Prim_Key_Col_Val", SRC_SYS_UNIQ_PRIM_KEY_COL_VAL)
                      .withColumn("Meas_Cnt", MEAS_CNT)
                      .withColumn("Dw_Anom_Flg", DW_ANOM_FLG)
                      .withColumn("Dw_Mod_Ts", CURRENT_TS)
                      .withColumn("Dw_Job_Id", DW_JOB_ID)
                      .withColumn("Src_Sys_Iud_Cd", SRC_SYS_IUD_CD)
                      .select(*TARGET_TABLE_SCHEMA)
                      )

    COUNT_df = transformed_df.count()
    print(f"The final dataframe has '{COUNT_df}' rows")
    transformed_df.printSchema()
    transformed_df.show(5)

    # Write to Target Table
    print(f"Starting Write To {TARGET_TABLE_NAME}")
    transformed_df.write.format('parquet').mode('overwrite').saveAsTable(TARGET_TABLE_NAME, path=TARGET_TABLE_PATH)
    print(f"Finished Writing To {TARGET_TABLE_NAME}")

except Exception as e:
    print(f"An error occurred: {e}")
    sys.exit(1)  # Exit with a non-zero code to indicate failure

finally:
    # This block will execute whether the script succeeds or fails
    spark.stop()
    sys.exit(0)  # Exit with zero to indicate success