from datetime import datetime, timedelta

import os

from airflow import AirflowException
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.sensors import S3KeySensor, TimeDeltaSensor, ExternalTaskSensor, SqlSensor

import data_config
import local_config
import os_utils

args = {
    'owner': 'hongxiang.cai',
    'start_date': datetime(2018, 11, 26, 5),
    'retries': 0,
    'email': data_config.AIRFLOW_USERS
}

dag = DAG(
    dag_id='hourly_task_monitor_v1',
    default_args=args,
    schedule_interval='@hourly',
    dagrun_timeout=timedelta(hours=1)
)

hourly_task_monitor_template = '''
    cd %s/toolbox && python task_monitor.py
''' % local_config.WORKSPACE
hourly_task_monitor = BashOperator(
    task_id='hourly_task_monitor',
    bash_command=hourly_task_monitor_template,
    priority_weight=10,
    dag=dag,
)


if __name__ == "__main__":
    dag.cli()
