from builtins import range
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import S3KeySensor, TimeDeltaSensor
from airflow.models import DAG
from datetime import datetime, timedelta
import os
import os.path

import data_config
import local_config
import airflow_utils

args = {
    'owner': 'kevin.yang',
    'start_date': datetime(2017, 8, 10),
    'retries':1,
    'email': data_config.AIRFLOW_USERS
}

dag = DAG(
    dag_id='usage_hour', 
    default_args=args,
    schedule_interval = '@hourly',
    dagrun_timeout=timedelta(hours=2))

init_env_template = '''
    cd {{ params.workspace }}/usage_hour &&
    [ -f {{ params.local_ipbin_path }} ] || hadoop fs -get {{ params.hdfs_ipbin_path }} {{ params.local_ipbin_path }}
'''

local_ip_bin = local_config.WORKSPACE + "/utils/IP-COUNTRY-REGION-CITY.BIN"
init_env = BashOperator(
    task_id='init_env',
    bash_command = init_env_template,
    dag=dag,
    params={'workspace': local_config.WORKSPACE, 'hdfs_ipbin_path':data_config.IP2LOCATION_DATA_PATH, 'local_ipbin_path':local_ip_bin}
)

process_part_template = '''
    cd {{ params.workspace }}/usage_hour && bash usage_part.sh {{ params.part }} {{macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S", "%Y%m%d%H")}} process
'''

upload_part_template = '''
    cd {{ params.workspace }}/usage_hour && bash usage_part.sh {{ params.part }} {{macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S", "%Y%m%d%H")}} upload
'''

clean_template = '''
    cd {{ params.workspace }}/usage_hour && bash usage_part.sh "log0[123]" {{macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S", "%Y%m%d%H")}} {{ params.operator }}
'''

usage_hour_report_template = '''
    cd {{ params.workspace }}/performance && python usage_hour_report.py --datetime {{macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S", "%Y%m%d%H")}} --mode local
'''


usage_hour_report = BashOperator(
    task_id='usage_hour_report',
    bash_command = usage_hour_report_template,
    dag=dag,
    params={'workspace': local_config.WORKSPACE}
)


clean_hdfs = BashOperator(
    task_id='clean_hdfs_his',
    bash_command = clean_template,
    dag=dag,
    params={'workspace': local_config.WORKSPACE, 'operator': 'clean_hdfs'}
)

clean_local = BashOperator(
    task_id='clean_local',
    bash_command = clean_template,
    dag=dag,
    params={'workspace': local_config.WORKSPACE, 'operator': 'clean_local'}
)
usage_hour_report.set_downstream(clean_local)

for part in ['log0%d' % (n+1) for n in range(3)]:
    wait_part = airflow_utils.FileSensor(
        dag = dag,
        task_id='wait_%s' % (part),
        timeout=10*3600,
        file_path = 'hdfs:///data/rawlog/usage/%s/%s/%s_usage_ime_international/%s_%s_usage_ime_international_SUCCESS'\
        % ('{{macros.ds_format(ds, "%Y-%m-%d", "%Y")}}', '{{ds_nodash}}', '{{ds_nodash}}', part, '{{macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S", "%Y%m%d%H")}}')
    )
    
    process_part = BashOperator(
        task_id='process_%s' % part,
        bash_command = process_part_template,
        pool='usage_hour_pool',
        dag=dag,
        params={'workspace': local_config.WORKSPACE, 'part': part}
    )
    
    upload_part = BashOperator(
        task_id='upload_%s' % part,
        bash_command = upload_part_template,
        dag=dag,
        params={'workspace': local_config.WORKSPACE, 'part': part}
    )
    
    init_env.set_downstream(wait_part)
    wait_part.set_downstream(process_part)
    process_part.set_downstream(usage_hour_report)
    process_part.set_downstream(upload_part)
    upload_part.set_downstream(clean_local)
    
    
if __name__ == "__main__":
    dag.cli()
