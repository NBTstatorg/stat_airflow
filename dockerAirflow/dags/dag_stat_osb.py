from airflow import DAG
from datetime import datetime, timedelta
import pytz
from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path
from library.emailRobot import email_robot
from library.OSB_parse import start_parse
import json


loc_tzon = pytz.timezone('Asia/Dushanbe')
start_date = datetime(2025, 6, 14, 10, 0, 0 ,0, tzinfo = loc_tzon)


default_args = {
    'owner': 'airflow',
    'start_date': start_date,
}

# dag = DAG(
#     'xcom_pb',
#     default_args=default_args,
#     # schedule_interval='0 0 1-15 * 1',
#     schedule_interval='* */10 * * *',
#     max_active_runs=1
# )

def track_runs(**kwargs):
    start_parse()



with DAG(
    dag_id= 'osb_parser_5m',
    default_args = default_args,
    schedule_interval='*/5 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['Парсинг']

) as dag:
    track_task = PythonOperator(
        task_id='track_runs',
        python_callable=track_runs,
        provide_context=True,
        # dag=dag   
)

# track_task
