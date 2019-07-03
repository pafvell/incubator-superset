from builtins import range
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import TimeDeltaSensor
from airflow.models import DAG
from airflow.exceptions import AirflowException

from datetime import datetime, timedelta
import os
import os.path

import data_config
import local_config
import os_utils
import airflow_utils

HOUR_FLAG_TIMEOUT = 1
args = {
    'owner': 'kevin.yang',
    'start_date': datetime(2018, 3, 28),
    'retries': 1,
    'email': data_config.AIRFLOW_USERS
}

dag = DAG(
    dag_id='usage_matrix_hour',
    default_args=args,
    schedule_interval='@hourly',
    dagrun_timeout=timedelta(hours=4),
    max_active_runs=2, 
)


wait_remote_data = TimeDeltaSensor(
    dag=dag,
    task_id='wait_remote_data',
    delta=timedelta(hours=1.5)
)

wait_data = TimeDeltaSensor(
    dag=dag,
    task_id='wait_data',
    delta=timedelta(hours=0.5)
)


parse_usage_template = '''
    cd {{ params.workspace }}/usage_matrix && python ./usage_matrix_hourly.py {{ params.product }} {{ds_nodash}} \
    {{macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S", "%H")}} {{params.region}} {{params.mode}} {{params.path_black_list_flag}} {{params.path_white_list_flag}}
'''

## demo
product_config = [
    {
        "name": "usage_ime_ios_container",
        "region": ["us"],
        "mode": "hadoop",
        "path_white_list": ['/STATISTIC/AREMOJISDK'],
        "path_black_list": None
    },
    {
        "name": "usage_ime_ios_keyboard",
        "region": ["us"],
        "mode": "hadoop",
        "path_white_list": ['/STATISTIC/AREMOJISDK'],
        "path_black_list": None
    },
    {
        "name": "usage_ime_international",
        "region": ["us"],
        "mode": "hadoop",
        "path_white_list": ['/STATISTIC/MYGIF/GIF_DATA'],
        "path_black_list": None
    }
]

for cfg in product_config:
    product = cfg['name']
    mode = '--mode %s' % cfg['mode'] if cfg['mode'] else ''

    path_white_list_flag, path_black_list_flag = '', ''
    if 'path_white_list' in cfg and cfg['path_white_list']:
        path_list = map(lambda x: x.replace('/', '_').replace('.', '_'), cfg['path_white_list'])
        path_white_list_flag = '--path_white_list %s' % ','.join(path_list)

    if 'path_black_list' in cfg and cfg['path_black_list']:
        path_list = map(lambda x: x.replace('/', '_').replace('.', '_'), cfg['path_black_list'])
        path_black_list_flag = '--path_black_list %s' % ','.join(path_list)

    for reg in cfg['region']:
        process_usage_hour = BashOperator(
            task_id='parse_hourly_%s_%s_data' % (reg, product),
            bash_command=parse_usage_template,
            dag=dag,
            params={
                'workspace': local_config.WORKSPACE,
                'product': product,
                'region': reg,
                'mode': mode,
                'path_black_list_flag': path_black_list_flag,
                'path_white_list_flag': path_white_list_flag
            }
        )
        wait_flag = airflow_utils.FlagsSensor(
            dag=dag,
            task_id='wait_rawlog_%s_%s_flag' % (reg, product),
            timeout=HOUR_FLAG_TIMEOUT * 3600,
            region='rawlog_%s' % reg,
            data_type=product,
            interval='hourly',
        )
        process_usage_hour.set_upstream(wait_flag)



# remote call
remote_server_ip = "192.168.1.47"
for reg, priority in zip(['cn'], ['3']):
    for product_type in ['usage_swiftcall', 'gb_server_swiftcall']:
        def parse_hourly_remote_func(product, **kwargs):
            file_list = [
                '%s/usage_matrix/pool.conf' % local_config.WORKSPACE,
                '%s/usage_matrix/usage_matrix_hourly.py' % local_config.WORKSPACE,
                '%s/usage_matrix/usage_matrix.spark.py' % local_config.WORKSPACE,
                '%s/utils/config.py' % local_config.WORKSPACE,
                '%s/utils/ime_utils.py' % local_config.WORKSPACE,
                '%s/utils/os_utils.py' % local_config.WORKSPACE,
                '%s/utils/s3_utils.py' % local_config.WORKSPACE,
                '%s/utils/cluster.py' % local_config.WORKSPACE,
                '%s/utils/encode_identifier_lib.py' % local_config.WORKSPACE,
                '%s/local_config.py' % local_config.WORKSPACE,
            ]

            process_usage_hour_cmd = '''
                export PATH=\"/usr/local/lib/python2.7/dist-packages:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-oracle/bin:/usr/lib/jvm/java-8-oracle/db/bin:/usr/lib/jvm/java-8-oracle/jre/bin:/usr/share/rvm/bin:/usr/local/hadoop-2.6.3/bin:/usr/local/spark-2.1.1-bin-hadoop2.6/bin\" &&
                cd .%s/usage_matrix &&
                python ./usage_matrix_hourly.py %s %s %s %s --tzdelta -8 --path_black_list _BIU_FEEDS_debug_user_stay_info --only_white_list
            ''' % (local_config.WORKSPACE, product, kwargs['ds_nodash'],
                   datetime.strptime(kwargs['ts'], "%Y-%m-%dT%H:%M:%S").strftime('%H'), reg)
            returncode, out_data, err_data = os_utils.remote_run(os_utils.DRUID_HOST, process_usage_hour_cmd, files=file_list, keep_dir=True)
            #TODO: find out why hadoop in cn will return 255, but the task actually success.
            if returncode != 0 and returncode != 255:
                raise AirflowException('cmd: %s %s %s %s' % (process_usage_hour_cmd, returncode, out_data, err_data))
        process_usage_hour = PythonOperator(
            dag=dag,
            task_id='parse_hourly_%s_%s_%s_data' % (priority, reg, product_type),
            python_callable=parse_hourly_remote_func,
            provide_context=True,
            op_args=[product_type],
        )
        process_usage_hour.set_upstream(wait_remote_data)
        # sync from remote hdfs to local hdfs
        sync_usage_hour_template = '''
            cd %s/usage_matrix &&
            python ./sync_usage_matrix_hourly.py %s {{ds_nodash}} {{macros.ds_format(ts, "%%Y-%%m-%%dT%%H:%%M:%%S", "%%H")}} %s --all_paths
        ''' % (local_config.WORKSPACE, product_type, reg)
        sync_usage_hour = BashOperator(
            task_id='sync_hourly_%s%s_%s_data' % (priority, reg, product_type),
            bash_command=sync_usage_hour_template,
            priority_weight=10,
            dag=dag,
        )
        sync_usage_hour.set_upstream(process_usage_hour)

if __name__ == "__main__":
    dag.cli()
