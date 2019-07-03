from builtins import range
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import S3KeySensor, TimeDeltaSensor, ExternalTaskSensor
from airflow.models import DAG
from datetime import datetime, timedelta
import os

import local_config
import data_config
import airflow_utils

args = {
    'owner': 'kevin.yang',
    'start_date': datetime(2017, 1, 20),
    'retries':1,
    'email': data_config.AIRFLOW_USERS
}

dag = DAG(
    dag_id='activate_source_summary',
    default_args=args,
    schedule_interval = timedelta(days=7),
    dagrun_timeout=timedelta(hours=47))

activate_source_summary_list = []

activate_source_summary_template = '''cd %s/activate_source''' % local_config.WORKSPACE + ''' && python activate_source_summary.py -f {{macros.ds_format(macros.ds_add(ds, 6), "%Y-%m-%d", "%Y%m%d")}} -t {{macros.ds_format(macros.ds_add(ds, 6), "%Y-%m-%d", "%Y%m%d")}}'''
for product_section in data_config.PATH_DICT.keys():
    activate_source_summary = BashOperator(
        task_id='activate_source_summary_%s' % product_section,
        bash_command=activate_source_summary_template + ' -p %s' % product_section,
        dag=dag
    )
    activate_source_summary_list.append(activate_source_summary)

activate_source_data_ready = DummyOperator(task_id='activate_source_data_ready', dag=dag)
#check last 7 days activate_source success
for d in range(7):
    now = ExternalTaskSensor(
        task_id='activate_source_%d' % d,
        timeout=airflow_utils.wait_activate_source_timeout*3600,
        external_dag_id = 'activate_source',
        external_task_id = 'activate_source',
        execution_delta = timedelta(days=(0-d)),
        dag=dag)
    now.set_downstream(activate_source_data_ready)
activate_source_data_ready.set_downstream(activate_source_summary_list)

activate_source_summary_success = DummyOperator(task_id='activate_source_summary', dag=dag)
activate_source_summary_success.set_upstream(activate_source_summary_list)

activate_channel_template = '''cd %s/activate_summary''' % local_config.WORKSPACE + ''' && python activate_channel.py --date {{macros.ds_format(macros.ds_add(ds, 6), "%Y-%m-%d", "%Y%m%d")}}'''
activate_channel = BashOperator(
    dag = dag,
    task_id='activate_channel',
    bash_command = activate_channel_template
    )

#activate summary will be ready at Sat
wait_activate_summary = TimeDeltaSensor(
    dag=dag,
    task_id='wait_activate_summary',
    delta = timedelta(hours=24)
)

for reg in ['us', 'eu', 'ap']:
    now = airflow_utils.FileSensor(
        dag = dag,
        task_id='wait_activate_summary_%s' % reg,
        timeout = 10*3600,
        file_path = os.path.join(data_config.DW_ACTIVATE_SUMMARY, 'parquet/%s/'%reg + '{{macros.ds_format(macros.ds_add(ds, 6), "%Y-%m-%d", "%Y%m%d")}}/_SUCCESS')
    )
    wait_activate_summary.set_downstream(now)
    now.set_downstream(activate_channel)

if __name__ == "__main__":
    dag.cli()
