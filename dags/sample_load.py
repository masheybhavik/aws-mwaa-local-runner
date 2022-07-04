from airflow import DAG
import logging
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

sql_master_csv_import = [ """ TRUNCATE TABLE "EL_MATILLION_RAW"."DW_UTIL".MASTER_CSV_LOAD ;
""",
"""
    COPY INTO "EL_MATILLION_RAW"."DW_UTIL".MASTER_CSV_LOAD 
    FROM  @"EL_MATILLION_RAW"."DW_UTIL".S3_INTEGRATION/master_metadata.csv
    ON_ERROR='ABORT_STATEMENT' ;
"""
]

def _sql_gen():
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn_id", role = 'EDW_DB_OWNER_ROLE')
    sql_gen = """
        select  "file_owner" as file_owner, "target_table_name" as table_name,
            'CREATE OR REPLACE TABLE "EL_MATILLION_RAW"."RAW_CSV"."'|| "target_table_name" || '" (' || "columns" || ');' as create_table, 
        'COPY INTO "EL_MATILLION_RAW"."RAW_CSV"."'|| "target_table_name" || '" FROM @"EL_MATILLION_RAW"."DW_UTIL".S3_INTEGRATION/' || coalesce("csv_file_prefix",'')|| "csv_file_name" || ' ON_ERROR=''ABORT_STATEMENT'' ;' copy_table
        from "EL_MATILLION_RAW"."DW_UTIL".MASTER_CSV_LOAD
        where "active_flag";
    """
    # engine = dwh_hook.get_sqlalchemy_engine()
    # connection = engine.connect()
    result = dwh_hook.get_records(sql_gen)
    logging.info("Number of rows in `abcd_db.public.test3`  - %s", result[0][0])
    return result

def _load_csv(**kwargs):
    task_instance = kwargs['task_instance']
    result = task_instance.xcom_pull(task_ids='sql_gen')
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn_id", role = 'EDW_DB_OWNER_ROLE')

    for rows in result:
        csv_owner = rows[0]
        table_name = rows[1]
        sql_create_table = rows[2]
        sql_load_data = rows[3]
        logging.info("Creating table %s for loading csv data uploaded by %s", table_name,csv_owner)

        result1 = dwh_hook.get_records(sql_create_table)
        logging.info("%s", result1)
        result2 = dwh_hook.get_records(sql_load_data)
        logging.info("%s", result2)

        """
        create_table = SnowflakeOperator(
            task_id="create_table_{table_name}".format(table_name=table_name),
            sql= sql_create_table,
            warehouse="DEV_EDW_XS_WH",
            database="EL_MATILLION_RAW",
            schema="RAW_CSV",
            role="EDW_DB_OWNER_ROLE",
            snowflake_conn_id="snowflake_conn_id",
            dag=dag
        )
        create_table.set_downstream(load_csv)

        load_data = SnowflakeOperator(
            task_id="load_data_{table_name}".format(table_name=table_name),
            sql= sql_load_data,
            warehouse="DEV_EDW_XS_WH",
            database="EL_MATILLION_RAW",
            schema="RAW_CSV",
            role="EDW_DB_OWNER_ROLE",
            snowflake_conn_id="snowflake_conn_id",
            dag=dag
        )
        load_data.set_downstream(create_table)
        """

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

dag = DAG("sample_load", default_args=default_args, schedule_interval=None)

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

sql_gen = PythonOperator(
    task_id="sql_gen",
    provide_context= True, 
    python_callable=_sql_gen,
    op_kwargs=None,
    # op_kwargs=(key1='value1', key2='value2'),
    dag=dag,
)

load_csv = PythonOperator(
    task_id="load_csv",
    provide_context= True, 
    python_callable=_load_csv,
    dag=dag,
)

master_csv_import >> sql_gen >> load_csv

# master_csv_import.set_downstream(sql_gen)
# sql_gen.set_downstream(load_csv)