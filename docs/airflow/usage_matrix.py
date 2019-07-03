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
import os_utils


args = {
    'owner': 'libin.yang',
    'start_date': datetime(2017, 3, 11),
    'retries':1,
    'email': data_config.AIRFLOW_USERS
}

dag = DAG(
    dag_id='usage_matrix',
    default_args=args,
    schedule_interval = timedelta(days=1),
    dagrun_timeout=timedelta(hours=airflow_utils.dag_timeout))

read_matrix_referrer_data = DummyOperator(task_id='read_matrix_referrer_data', dag=dag)
usage_matrix_rainbow_success = DummyOperator(task_id='usage_matrix_rainbow_success', dag=dag)
read_matrix_referrer_data.set_downstream(usage_matrix_rainbow_success)
usage_matrix_all_success = DummyOperator(task_id='usage_matrix_all_success', dag=dag)
usage_matrix_rainbow_success.set_downstream(usage_matrix_all_success)


wait_usage_matrix = TimeDeltaSensor(
    dag=dag,
    task_id='wait_usage_matrix',
    delta=timedelta(hours=2.5)
)


def check_usage_matrix_hdfs_func(fpath, reg, **kwargs):
    file_path = '%s/%s/MSG_*/json/%s/%s/_SUCCESS' % (data_config.DW_USAGE, fpath, reg, kwargs['ds_nodash'])
    ret = os_utils.is_file_exist(file_path)
    print('%s exist return %s' % (file_path, ret))
    if not ret:
        raw_path = data_config.USAGE_DATA(kwargs['ds_nodash'], fpath, reg, datetime.strptime(kwargs['ds'], '%Y-%m-%d').strftime('%Y'))
        ret_du, out, err = os_utils.local_run('hadoop fs -du -s %s' % raw_path)
        raw_empty = out.split()[0] == '42' if ret_du == 0 else False
        print('raw path size %s' % out)
        if not raw_empty:
            raise AirflowException('%s not exist' % file_path)


def create_wait_task(product, reg, priority=''):
    wait_data = airflow_utils.FlagsSensor(
        dag=dag,
        task_id='wait_dw_%s_%s%s' % (reg, product, priority),
        timeout=airflow_utils.wait_usage_flag_timeout * 3600,
        region='dw_%s' % reg,
        data_type='%s%s' % (product, priority),
        pool='usage_matrix_pool'
    )
    wait_usage_matrix.set_downstream(wait_data)

    check_data = PythonOperator(
        dag=dag,
        task_id='check_data_dw_%s_%s%s' % (reg, product, priority),
        python_callable=check_usage_matrix_hdfs_func,
        provide_context=True,
        op_args=[product, reg],
    )
    wait_data.set_downstream(check_data)
    return check_data


HADOOP_PROCESS_USAGE = [
    'usage_ime_filter',
    'usage_sevenfit',
    'usage_flashlight',
    'drinkwater',
    'usage_callshow',
    'usage_music',
    'gb_hades_internal',
    'gb_hades_internal_vice',
    'usage_swiftcall',
    'usage_steps',
    'usage_period',
    'gb_server_swiftcall',
    'usage_horoscope',
    'usage_sleep',
    'usage_scanner',
    'usage_wordchef',
    'usage_gdpr',
]

def wait_task(product, reg):
    wait_data = create_wait_task(product, reg)
    wait_data.set_downstream(read_matrix_referrer_data)


RAINBOW_PATHS = [
    '_RAINBOW_TRIGGER_PV',
    '_RAINBOW_FEATURE_PV',
    '_RAINBOW_AD_SHOULD_SHOW',
    '_RAINBOW_AD_SHOWN',
    '_RAINBOW_AD_SHOULD_SHOW_UNIQUE',
    '_RAINBOW_AD_SHOWN_UNIQUE',
    '_RAINBOW_AD_LOAD_FAIL',
    '_RAINBOW_AD_CLICK',
    '_RAINBOW_AD_CLOSE',
]

def wait_task_multisteps(product, reg):
    wait_high = create_wait_task(product, reg, '_high')
    wait_low = create_wait_task(product, reg, '_low')
    wait_high.set_downstream(usage_matrix_rainbow_success)
    wait_low.set_downstream(usage_matrix_all_success)


for reg in ['eu', 'ap', 'us']:
    for product in data_config.PATH_DICT['gb'].MATRIX_PRODUCT_LIST:
        if not product.startswith('gb_hades_internal'):
            wait_task(product, reg)
        else:
            wait_task_multisteps(product, reg)

# swift_call
for reg in ['cn']:
    for product in data_config.PATH_DICT['gb'].OTHER_MATRIX_PRODUCT_LIST:
        wait_task(product, reg)

# gb_server_swiftcall
for reg in ['cn']:
    for product in data_config.PATH_DICT['gb'].SERVER_PRODUCT_LOG_LIST:
        wait_task(product, reg)
