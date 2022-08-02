from airflow.models import Variable
from airflow import DAG
import logging
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator


test = Variable.get("snowflake__database")

def _test(test):
    logging.info("database name: %s", test)


default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "start_date": datetime(2022, 6, 21),
    "email": ["bhavik@mashey.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG("bhavik_test", default_args=default_args, schedule_interval=None)
 
test = PythonOperator(
    task_id=f"test_{test}",
    provide_context= True, 
    python_callable=_test,
    op_kwargs={'test':test},
    dag=dag,
)
