from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.sensors import S3KeySensor, TimeDeltaSensor, ExternalTaskSensor, SqlSensor
from builtins import range
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import os, re

import data_config
import s3_utils
import airflow_utils
import local_config


# create an independent DAG to prevent send email when clean task stat
args = {
    'owner': 'libin.yang',
    'start_date': datetime(2017, 10, 15),
    'retries': 1,
    'email': data_config.AIRFLOW_USERS
}

dag = DAG(
    dag_id='daily_report_email',
    default_args=args,
    schedule_interval=timedelta(days=1),
    dagrun_timeout=timedelta(hours=airflow_utils.dag_timeout))


process_ua_daily_summary_report_template = '''
    cd %s/roi_channel && python ua_daily_summary.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % local_config.WORKSPACE
process_ua_daily_summary_report = BashOperator(
    task_id='ua_daily_summary',
    bash_command=process_ua_daily_summary_report_template,
    dag=dag
)


# wait roi data for daily summary report
now = ExternalTaskSensor(
    task_id='wait_roi_data_ready',
    timeout=18 * 3600,
    external_dag_id='roi',
    external_task_id='roi_data_ready',
    execution_delta=timedelta(0),
    dag=dag)
now.set_downstream(process_ua_daily_summary_report)
