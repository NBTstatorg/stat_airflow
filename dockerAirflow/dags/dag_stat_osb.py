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
start_date = datetime(2025, 6, 9, 10, 0, 0 ,0, tzinfo = loc_tzon)


default_args = {
    'owner': 'airflow',
    'start_date': start_date,
}

# dag = DAG(
#     'xcom_pb',
#     default_args=default_args,
#     # schedule_interval='0 0 1-15 * 1',
#     schedule_interval='* */15 * * *',
#     max_active_runs=1
# )

def track_runs(**kwargs):
    ti = kwargs['ti']
    run_count = ti.xcom_pull(key='run_count', task_ids='track_runs', include_prior_dates=True) or 0
    print (f' fun_count @@@@@ {run_count}')
    run_count = int(run_count)
    start_parse()

    if run_count >= 100:
        raise ValueError("Превышен максимальное количество запусков DAG.")

    ti.xcom_push(key='run_count', value=run_count + 1)


with DAG(
    dag_id= 'osb_parser',
    default_args = default_args,
    schedule_interval='*/15 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['Парсинг ОСБ']

) as dag:
    track_task = PythonOperator(
        task_id='track_runs',
        python_callable=track_runs,
        provide_context=True,
        # dag=dag   
)

# track_task
