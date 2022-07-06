import json
import pandas as pd
import io
import logging
import boto3
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3_list import S3ListOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

account_id = boto3.client('sts').get_caller_identity().get('Account')
if account_id=="821579699083":
    path="prd-datahub-custom-analytics-tables"
elif account_id=="559293306438":
    path="datahub-custom-analytics-tables"
local_tz = pendulum.timezone("America/Chicago")

# path = "datahub-custom-analytics-tables"

DAG_ID = os.path.basename(__file__).replace(".py", "")
os.environ['AWS_DEFAULT_REGION'] = 'us-west-1'
def task_fail_slack_alert(context):
    SLACK_CONN_ID = 'slack'
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Pitchbook Job Failed.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            *Error*:{exception}
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
            exception=context.get('exception')

        )
    failed_alert = SlackWebhookOperator(
        task_id='slack',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        dag=dag)
    return failed_alert.execute(context=context)


def _process_obtained_data(ti):
    list_of_csvs = ti.xcom_pull(task_ids='list_csvs')
    Variable.set(key='list_of_csvs',
                 value=list_of_csvs, serialize_json=True)
    logging.info("List of csv in varoable: %s", list_of_csvs)


def _load_csv(csv_file, table_name):
    logging.info("CSV File name: %s", csv_file)
    s3 = S3Hook(aws_conn_id='aws_default').get_conn()
    obj = s3.get_object(Bucket=path, Key=csv_file)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    df = df.apply(lambda col: pd.to_datetime(col, errors='ignore') 
              if col.dtypes == object 
              else col, 
              axis=0)

    engine = SnowflakeHook(snowflake_conn_id='snowflake_conn_id').get_sqlalchemy_engine()
    connection = engine.connect()
    
    #make sure index is False, Snowflake doesnt accept indexes
    df.to_sql(f"stg_{table_name}", schema='RAW_CSV', con=engine, index=False) 
    
    connection.close()
    engine.dispose()



default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "start_date": datetime(2022, 7, 3),
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

with DAG('load_csv_to_snowflake', schedule_interval='@daily',default_args=default_args,
            description='Load data from CSV files to snowflake dynamically.') as dag:

    list_csvs = S3ListOperator(
        task_id="list_csvs",
        bucket=path,
        aws_conn_id='aws_default',
        on_failure_callback=task_fail_slack_alert
    )

    preparation_task = PythonOperator(
        task_id='preparation_task',
        python_callable=_process_obtained_data,
        provide_context=True,
        on_failure_callback=task_fail_slack_alert
    )

    all_done = DummyOperator(
        task_id='All_jobs_completed'
    )

    # Top-level code within DAG block
    iterable_list = Variable.get('list_of_csvs', 
                                default_var=['dim_current_loss_run.csv'], 
                                deserialize_json=True
    )

    with TaskGroup('dynamic_tasks_group',
                    prefix_group_id=False,
                    ) as dynamic_tasks_group:
        if iterable_list:
            for index, csv_file in enumerate(iterable_list):
                table_name = csv_file.replace(".csv", "")

                drop_table = SnowflakeOperator(
                    task_id=f'drop_table_{table_name}',
                    sql=f'drop table if exists "EL_MATILLION_RAW"."RAW_CSV".stg_{table_name}',
                    snowflake_conn_id="snowflake_conn_id",
                    on_failure_callback=task_fail_slack_alert
                )
                load_csv = PythonOperator(
                    task_id=f'load_csv_{table_name}',
                    python_callable=_load_csv,
                    op_kwargs={'csv_file': csv_file, 'table_name': table_name},
                    on_failure_callback=task_fail_slack_alert
                )
                process_csv = SnowflakeOperator(
                    task_id=f'process_csv_{table_name}',
                    sql=f'create or replace table "EL_MATILLION_RAW"."RAW_CSV".{table_name} clone "EL_MATILLION_RAW"."RAW_CSV".stg_{table_name}',
                    snowflake_conn_id="snowflake_conn_id",
                    on_failure_callback=task_fail_slack_alert
                )
                drop_table_after_processing = SnowflakeOperator(
                    task_id=f'drop_table_after_processing_{table_name}',
                    sql=f'drop table if exists "EL_MATILLION_RAW"."RAW_CSV".stg_{table_name}',
                    snowflake_conn_id="snowflake_conn_id",
                    on_failure_callback=task_fail_slack_alert
                )

                drop_table >> load_csv >> process_csv >> drop_table_after_processing

# DAG level dependencies
list_csvs >> preparation_task >> dynamic_tasks_group >> all_done