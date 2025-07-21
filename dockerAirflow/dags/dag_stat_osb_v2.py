from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException, AirflowFailException
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from library.OSB_parse import start_parse
from library.control_scripts import Masterdata
from pathlib import Path
import logging
import json
import pytz

logging.basicConfig(level=logging.INFO)

loc_tzon = pytz.timezone('Asia/Dushanbe')
start_date = datetime(2019, 1, 1, 0, 0, 0 ,0, tzinfo = loc_tzon)


default_args = {
    'owner': 'firdavs',
    'start_date': start_date
}

with DAG(
    dag_id= 'fork_osb',
    default_args = default_args,
    schedule_interval='0 0 1 * *',
    catchup=True,
    max_active_runs=2,
    tags=['Парсинг']

) as dag:
        
    @task
    def fork_from_schedule(**kwrds):
        path = Path(__file__).parent.parent
        logging.info ('email config parent folder  %s', path)
        master_data = Masterdata (json.load (\
            open(path / ("config/email_conf.json"))))

        from_date = kwrds['data_interval_start'] + timedelta(days=1) 
        to_date =   kwrds['data_interval_end']

        logging.info('getting schedule for period: %s - %s' \
                      , from_date, to_date)

        schedules = master_data\
            .get_shcedule_by_period(report_type_id=6, \
                                    period_type=4,\
                                    from_date= from_date,\
                                    to_date=to_date)        
        schedules_ids = tuple([x[0] for x in schedules if x[5]!=0]) 
        return schedules_ids

    @task
    def trigger_task(schedules_ids):
        if not schedules_ids:
            logging.info("NO SCHEDULE LEFT OR EXIST")
            raise AirflowSkipException 
        else:
            return(schedules_ids)

    @task
    def get_osb(schedules_id):
        logging.info("getting schedule with id: %s", schedules_id)
        if not start_parse(schedules_id):
            logging.info("start_parse returned value False")    
            raise Exception("get_osb mapped task terminated.")


#==============================RUNNING TASKS=================================#
    
    get_osb.expand(
                   schedules_id=
                        trigger_task(
                                      fork_from_schedule()
                                    )
                  )

