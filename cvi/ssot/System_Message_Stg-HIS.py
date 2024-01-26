#!/usr/bin/env python
# coding: utf-8

"""
This script performs an ETL (Extract, Transform, Load) process on a dataset using PySpark. The process involves reading a file from an S3 bucket, loading it into Spark DataFrame, and then saving the DataFrame to an HDFS table in Parquet format. The script makes use of Python's boto3 library to interact with AWS S3 and PySpark to handle distributed data processing.

Here is a detailed overview of the steps:

1. Command line arguments are parsed to get the environment (e.g., "DEV", "PROD") where the script is being executed. This argument is used to decide the HDFS path to write the output.

2. The schema for the input data is defined explicitly. This is important because it ensures that data is read in the correct format. The input data consists of various fields including codes, descriptions, messages, and date information.

3. Constants are defined for the S3 bucket, folder, and file names along with the date format in the data. The data file "SSOT_System_Message_ARCHIVE" resides in the "SSOT_ARCHIVE" folder of the "fleet-manual-archive" S3 bucket. The date fields in this file are expected to be in the "M/d/yyyy" format.

4. A SparkSession, the entry point to any functionality in Spark, is created with Hive support enabled.

5. An AWS S3 client is created using the boto3 library. This client interacts with the S3 service using the AWS access key, secret key, and endpoint specified in the Spark session's configuration.

6. The "SSOT_System_Message_ARCHIVE" file is read from the "fleet-manual-archive/SSOT_ARCHIVE" S3 bucket using the predefined schema and date format. This file contains system messages and their associated information.

7. The data is processed if necessary. In this script, no transformation is applied on the data. However, in a typical ETL process, this could involve operations like cleaning, aggregating, or joining the data.

8. The data is written to an HDFS table in Parquet format. The specific HDFS path depends on the environment argument provided to the script. The table is named "dl_edge_base_dcfpa_171749_base_dcfpa_avista.system_message_stg".

9. If the data writing operation is successful, a success message is logged. Otherwise, a failure message is logged.

The script uses Python's logging module to log informational, warning, and error messages. It's useful for monitoring the script's execution and debugging potential issues. Errors encountered during the execution are logged and cause the script to terminate.
"""

# --- Importing required packages ---
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3 
from botocore.client import Config 
import logging
import sys

# --- Parsing arguments ---
ENV=sys.argv[1]


# --- Class for running ETL ---
class SparkETL:

    # Define schema of data to be read
    df_schema = StructType([
        StructField("NTL_CD", StringType()),
        StructField("SYS_CD", StringType()),
        StructField("SYS_DESC", StringType()),
        StructField("SYS_MSG_SA_ID", IntegerType()),
        StructField("MSG_TXT", StringType()),
        StructField("EFCTV_DT", DateType()),
        StructField("EXPRTN_DT", DateType()),
        StructField("MESSAGE_TYPE", StringType()),
        StructField("LAST_UPDT_USR_ID", StringType()),
        StructField("LAST_UPDT_DT", DateType())
    ])

    # Define constants for S3 paths and date format
    BUCKET_NAME = "fleet-manual-archive"
    SSOT_FOLDER = "SSOT_ARCHIVE"
    SYSM_FOLDER = "SSOT_System_Message_ARCHIVE"
    SYSMSG_FILE = "SSOT_System_Message.csv"
    DATE_FORMAT = "M/d/yyyy"
    ENV = sys.argv[1]
    TABLE_NAME = f"dl_edge_base_dcfpa_171749_base_dcfpa_avista.system_message_stg"
    TABLE_PATH = f"/sync/{ENV.lower()}_42124_edw_hi_b/DL/EDGE_BASE/DCFPA-171749/DCFPA-AVISTA/SYSTEM_MESSAGE_STG/Data"

    # --- Class for managing SparkSession ---
    class SparkSessionManager:
        def __init__(self):
            self.spark = None

        # --- Setting up the SparkSession ---
        def __enter__(self):
            try:
                self.spark = SparkSession.builder \
                    .appName("SZ92Q9") \
                    .enableHiveSupport() \
                    .getOrCreate()
                self.spark.sparkContext.setLogLevel('ERROR')
                logging.info('Spark Version: ' + self.spark.version)
                return self.spark
            except Exception as e:
                logging.error("Error while initiating Spark: ", exc_info=True)
                raise

        # --- Cleaning up the SparkSession ---
        def __exit__(self, exc_type, exc_value, exc_traceback):
            self.spark.stop()
            if exc_type:
                raise

    # --- Creating S3 client ---
    def get_s3_client(self):
        try:
            session = boto3.session.Session()
            s3_client = session.client(
                service_name='s3',
                aws_access_key_id=self.spark.conf.get("spark.hadoop.fs.s3a.access.key"),
                aws_secret_access_key=self.spark.conf.get("spark.hadoop.fs.s3a.secret.key"),
                endpoint_url=self.spark.conf.get("spark.hadoop.fs.s3a.endpoint"),
                config=Config(s3={'addressing_style': 'path'})
            )
            logging.info(f"Boto3 Version: {boto3.__version__}")
            return s3_client
        except Exception as e:
            logging.error("Error while creating S3 Client: ", exc_info=True)
            raise

    # --- Reading data from S3 ---
    def read_data(self, spark):
        try:
            df = spark.read.format('csv') \
                .schema(self.df_schema) \
                .option("header", "true") \
                .option("dateFormat", self.DATE_FORMAT) \
                .load(f"s3a://{self.BUCKET_NAME}/{self.SSOT_FOLDER}/{self.SYSM_FOLDER}/{self.SYSMSG_FILE}")
            logging.info("Data successfully loaded from S3")
            return df
        except Exception as e:
            logging.error("Error while loading data from S3: ", exc_info=True)
            raise

    # --- Running the ETL process ---
    def run(self):
        try:
            logging.info(f"Starting ETL process at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            with self.SparkSessionManager() as spark:
                self.spark = spark
                s3_client = self.get_s3_client()
                df = self.read_data(spark)
                df.show(5)
                df.printSchema()

                # Write data to HDFS table
                logging.info(f"Writing to {self.TABLE_NAME} started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                df.write.format("parquet").mode("overwrite").save(self.TABLE_PATH)
                logging.info(f"Writing to {self.TABLE_NAME} completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

                # Checking if the data writing was successful
                if self.spark.read.parquet(self.TABLE_PATH).count() == df.count():
                    logging.info(f"Dataframe written to HDFS table {self.TABLE_NAME} successfully!")
                else:
                    logging.error(f"Failed to write dataframe to HDFS table {self.TABLE_NAME}!")
        except Exception as e:
            logging.error(f"An error occurred during the ETL process: {str(e)}", exc_info=True)

if __name__ == "__main__":
    # --- Logger initialization ---
    try:
        etl = SparkETL()
        etl.run()
        logging.info("Script executed successfully")
    except Exception as e:
        logging.error("Error occurred: ", exc_info=True)
        sys.exit(1)