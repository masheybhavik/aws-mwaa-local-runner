from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

sql_master_csv_import = [ """ TRUNCATE TABLE "EL_MATILLION_RAW"."DW_UTIL".MASTER_CSV_LOAD ;
""",
"""
    COPY INTO "EL_MATILLION_RAW"."DW_UTIL".MASTER_CSV_LOAD 
    FROM  @"EL_MATILLION_RAW"."DW_UTIL".S3_INTEGRATION/master_metadata.csv
    ON_ERROR='ABORT_STATEMENT' ;
"""
]

sql_gen = """
    select  "file_owner" as file_owner,
        'CREATE OR REPLACE TABLE "EL_MATILLION_RAW"."RAW_CSV"."'|| "target_table_name" || '" (' || "columns" || ');' as create_table, 
       'COPY INTO "EL_MATILLION_RAW"."RAW_CSV"."'|| "target_table_name" || '" FROM @"EL_MATILLION_RAW"."DW_UTIL".S3_INTEGRATION/' || coalesce("csv_file_prefix",'')|| "csv_file_name" || ' ON_ERROR=''ABORT_STATEMENT'' ;' copy_table
    from "EL_MATILLION_RAW"."DW_UTIL".MASTER_CSV_LOAD
    where "active_flag";
"""

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

dag = DAG("snowflake_test_2", default_args=default_args, schedule_interval=None)


master_csv_import = SnowflakeOperator(
    task_id="master_csv_import",
    sql= sql_master_csv_import, # "create_table.sql",
    warehouse="DEV_EDW_XS_WH",
    database="EL_MATILLION_RAW",
    schema="RAW_CSV",
    role="EDW_DB_OWNER_ROLE",
    snowflake_conn_id="snowflake_conn_id",
    dag=dag
)

tmp_create_table = SnowflakeOperator(
    task_id="tmp_create_table",
    sql= "tmp_create_table.sql",
    warehouse="DEV_EDW_XS_WH",
    database="EL_MATILLION_RAW",
    schema="RAW_CSV",
    role="EDW_DB_OWNER_ROLE",
    snowflake_conn_id="snowflake_conn_id",
    dag=dag
)
tmp_load_table = SnowflakeOperator(
    task_id="tmp_load_table",
    sql= "tmp_load_table.sql",
    warehouse="DEV_EDW_XS_WH",
    database="EL_MATILLION_RAW",
    schema="RAW_CSV",
    role="EDW_DB_OWNER_ROLE",
    snowflake_conn_id="snowflake_conn_id",
    dag=dag
)
"""
sql_gen = SnowflakeOperator(
    task_id="sql_gen",
    sql= sql_sql_gen, 
    warehouse="DEV_EDW_XS_WH",
    database="EL_MATILLION_RAW",
    schema="RAW_CSV",
    role="EDW_DB_OWNER_ROLE",
    snowflake_conn_id="snowflake_conn_id",
    dag=dag
)
"""
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
master_csv_import >> tmp_create_table >> tmp_load_table