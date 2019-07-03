from builtins import range
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.sensors import S3KeySensor, TimeDeltaSensor, ExternalTaskSensor, SqlSensor
from airflow.models import DAG
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import os, logging

import data_config
import airflow_utils
import local_config

RC_SPLIT_DATE = '20170223'
logging.basicConfig(level=logging.INFO)

args = {
    'owner': 'kevin.yang',
    'start_date': datetime(2017, 2, 21),
    'retries':1,
    'email': data_config.AIRFLOW_USERS
}

dag = DAG(
    dag_id='usage_channel',
    default_args=args,
    schedule_interval = timedelta(days=1),
    dagrun_timeout=timedelta(hours=airflow_utils.dag_timeout))

def usage_channel_func(**kwargs):
    cmd = 'cd %s/usage_channel && python ./usage_channel.py %s %s'%(local_config.WORKSPACE, kwargs['ds_nodash'], (airflow_utils.get_last_friday_schedule(kwargs['execution_date']) + timedelta(days=6)).strftime('%Y%m%d'))
    ret = os.system(cmd)
    if ret != 0:
        raise AirflowException('cmd: %s' % cmd)

usage_tu_list = []
usage_channel = PythonOperator(
    dag = dag,
    task_id = 'usage_channel',
    python_callable = usage_channel_func,
    depends_on_past = True,
    provide_context = True
)
usage_tu_list.append(usage_channel)

unit_usage_channel_template = 'cd %s/usage_channel && python ./unit_usage_channel.py {{ds_nodash}}' % local_config.WORKSPACE
unit_usage_channel = BashOperator(
    task_id='unit_usage_channel',
    bash_command = unit_usage_channel_template,
    dag=dag
)
usage_tu_list.append(unit_usage_channel)


## usage data
for reg, priority in zip(['eu', 'ap', 'us'], ['1', '2', '0']):
    usage = airflow_utils.FlagsSensor(
        dag = dag,
        task_id='wait_usage_high_flag_%s' % reg,
        timeout=airflow_utils.wait_usage_flag_timeout * 3600,
        region=reg,
        pool='wait_usage_pool',
        data_type='usage_high'
    )
    usage.set_downstream(usage_tu_list)
    
    skin_plugin = airflow_utils.FlagsSensor(
        dag = dag,
        task_id='wait_skin_plugin_flag_%s' % reg,
        timeout=airflow_utils.wait_skin_plugin_flag_timeout * 3600,
        region= reg,
        pool='wait_usage_pool',
        data_type='skin_plugin'
    )    
    skin_plugin.set_downstream(usage_tu_list)
    
now = ExternalTaskSensor(
    dag=dag,
    task_id='wait_usage_matrix',
    timeout=10*3600,
    external_dag_id = 'usage_matrix',
    pool='wait_usage_pool',
    external_task_id = 'usage_matrix_rainbow_success',
)
now.set_downstream(usage_tu_list)

activate_source_data_ready = DummyOperator(task_id='activate_source_data_ready', dag=dag)
now = airflow_utils.FuncExternalTaskSensor(
    dag=dag,
    task_id='wait_activate_source_summary',
    timeout=airflow_utils.wait_activate_source_timeout*3600,
    external_dag_id = 'activate_source_summary',
    external_task_id = 'activate_source_summary',
    func = airflow_utils.get_last_friday_schedule
)
now.set_downstream(activate_source_data_ready)

#wait last 7 days of activate source
for d in range(7):
    now = ExternalTaskSensor(
        dag=dag,
        task_id='wait_activate_source_%d' % (d),
        timeout=airflow_utils.wait_activate_source_timeout*3600,
        external_dag_id = 'activate_source',
        external_task_id = 'activate_source',
        execution_delta = timedelta(days=d)
    )
    now.set_downstream(activate_source_data_ready)
activate_source_data_ready.set_downstream(usage_tu_list)

if __name__ == "__main__":
    dag.cli()
