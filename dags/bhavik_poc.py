from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta


snowflake_query = [
    """use role "DEV_EDW_DB_OWNER_ROLE";""",
    """use warehouse "DEV_EDW_XS_WH";""",
    """select * from "SNOWFLAKE_SAMPLE_DATA"."WEATHER"."WEATHER_14_TOTAL" limit 100;""",
]

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "start_date": datetime(2022, 6, 15),
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

dag = DAG("snowflake_test", default_args=default_args, schedule_interval=None)


snowflake_select = SnowflakeOperator(
    task_id="snowflake_select",
    sql=snowflake_query,
    snowflake_conn_id="snowflake_conn_id",
    dag=dag
)

"""
copy_into_table = S3ToSnowflakeOperator(
        task_id='copy_into_table',
        s3_keys=[S3_FILE_PATH],
        table=SNOWFLAKE_SAMPLE_TABLE,
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format="(type = 'CSV',field_delimiter = ';')",
    )

"""
