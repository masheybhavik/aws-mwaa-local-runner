from airflow import DAG
import logging
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3_list import S3ListOperator
import boto3
import io
# import s3fs
import csv, re
from io import StringIO
# import pyarrow
# from fastparquet import ParquetFile
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

def _pandas():
    s3_hook = S3Hook()
    logging.info("Running the functions for Pandas")
    # s3 = boto3.client('s3', 
    #                     aws_access_key_id="AKIAYEOD2SZDLNVGU2UQ",
    #                     aws_secret_access_key="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")
    s3 = S3Hook(aws_conn_id='aws_conn_id').get_conn()
    logging.info("S3 conn is available %s", s3)
    obj = s3.get_object(Bucket='datahub-custom-analytics-tables', Key='dim_current_loss_run.csv')
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    # filepath = "s3://datahub-custom-analytics-tables/dim_current_loss_run.csv"
    # df = pd.read_csv(filepath, sep=',', skiprows=1, header=None)
    # logging.info("DF is available %s", df)

    # Fill in your SFlake details here 
    # engine = create_engine(URL(
    #     account = 'qqfoshb-embroker',
    #     user = 'bshaw',
    #     password = 'H.AGhycRFtbKG._RCU4e',
    #     database = 'EL_MATILLION_RAW',
    #     schema = 'RAW_CSV',
    #     warehouse = 'DEV_EDW_XS_WH',
    #     role='EDW_DB_OWNER_ROLE',
    # ))
    engine = SnowflakeHook(snowflake_conn_id='snowflake_conn_id').get_sqlalchemy_engine()

    connection = engine.connect()
    
    df.to_sql('my_table', schema='RAW_CSV', con=engine, index=False) #make sure index is False, Snowflake doesnt accept indexes
    
    connection.close()
    engine.dispose()

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "start_date": datetime(2022, 6, 21),
    "email": ["bhavik@mashey.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("pandas_poc_3", default_args=default_args, schedule_interval=None)

list_keys = S3ListOperator(
    task_id="s3_list_operator",
    bucket="datahub-custom-analytics-tables",
    aws_conn_id='aws_conn_id',
    dag=dag
)

pandas = PythonOperator(
    task_id="pandas",
    provide_context= True, 
    python_callable=_pandas,
    op_kwargs=None,
    # op_kwargs=(key1='value1', key2='value2'),
    dag=dag,
)

list_keys >> pandas