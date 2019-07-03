from datetime import datetime, timedelta

import os

from airflow import AirflowException
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.sensors import S3KeySensor, TimeDeltaSensor, ExternalTaskSensor, SqlSensor

import airflow_utils
import data_config
import local_config
import os_utils

args = {
    'owner': 'hongxiang.cai',
    'start_date': datetime(2018, 10, 15),
    'retries': 1,
    'email': data_config.AIRFLOW_USERS
}

dag = DAG(
    dag_id='hourly_user_acquire_v2',
    default_args=args,
    schedule_interval='@hourly',
    dagrun_timeout=timedelta(hours=5)
)

platforms = {
    'facebook': '',
    'google': ''
    #'tapjoy': '',
    #'affiliate_network': '',
    #'snapchat': '',
    #'yahoo': ''
}

process_hourly_user_acquire_template = '''
    cd %s/user_acquire && python process_cost.py --date {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}} --hourly
''' % local_config.WORKSPACE
process_hourly_user_acquire = BashOperator(
    task_id='process_hourly_user_acquire',
    bash_command=process_hourly_user_acquire_template,
    trigger_rule=TriggerRule.ALL_DONE,
    priority_weight=90,
    dag=dag,
)

for key, value in platforms.items():
    hourly_user_acquire_template = '''cd %s/user_acquire && python daily_cost.py %s --hourly --level hourly_campaign --date {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}}''' % (local_config.WORKSPACE, key)
    hourly_user_acquire = BashOperator(
        task_id='hourly_user_acquire_%s' % key,
        bash_command=hourly_user_acquire_template,
        timeout=3600,
        execution_timeout=timedelta(hours=1),
        pool='user_acquire_h_%s_pool' % key,
        dag=dag
    )
    hourly_user_acquire.set_downstream(process_hourly_user_acquire)

    if key == 'facebook':
        hourly_user_acquire_ad_template = '''cd %s/user_acquire && python daily_cost.py %s --hourly --level hourly_ad --date {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}} --account_filter act_242664053099773 act_2256310497729898''' % (local_config.WORKSPACE, key)
        hourly_user_acquire_ad = BashOperator(
            task_id='hourly_user_acquire_ad_%s' % key,
            bash_command=hourly_user_acquire_ad_template,
            timeout=3600,
            execution_timeout=timedelta(hours=1),
            pool='user_acquire_h_%s_pool' % key,
            dag=dag
        )
        hourly_user_acquire.set_downstream(hourly_user_acquire_ad)

# account map his
os.environ["AIRFLOW_CONN_ALADDIN"] = "mysql://%s:%s@%s/aladdin" % (data_config.MYSQL_UA_INFO['user'], data_config.MYSQL_UA_INFO['password'], data_config.MYSQL_UA_INFO['host'])
account_map_his = SqlSensor(
    task_id='wait_account_map_his',
    conn_id='aladdin',
    sql="select * from ua_ad_account_his where date={{yesterday_ds_nodash}} limit 10",
    timeout=3600,
    dag=dag)

hourly_cost_report_template = '''
    cd %s/user_acquire && python hourly_cost_report.py {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}} {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}}
''' % local_config.WORKSPACE
hourly_cost_report = BashOperator(
    task_id='hourly_cost_report',
    bash_command=hourly_cost_report_template,
    dag=dag,
)
process_hourly_user_acquire.set_downstream(hourly_cost_report)
account_map_his.set_downstream(hourly_cost_report)

hourly_cost_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}} -t {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}} --data_source %s --json_path %s/user_acquire  --hourly --keep_data
''' % (local_config.WORKSPACE, 'smartinput_hourly_cost', local_config.WORKSPACE)
hourly_cost_report_import = BashOperator(
    task_id='hourly_cost_report_import',
    bash_command=hourly_cost_report_import_template,
    dag=dag
)
hourly_cost_report.set_downstream(hourly_cost_report_import)


def appsflyer_s3_to_hdfs_func(af_s3_path, af_hdfs_path, **kwargs):
    ret = os_utils.sync('%s/dt=%s/h=%s' % (af_s3_path, kwargs['ds'], datetime.strptime(kwargs['ts'], '%Y-%m-%dT%H:%M:%S').strftime('%-H')),
                        '%s/%s/%s' % (af_hdfs_path, kwargs['ds_nodash'], datetime.strptime(kwargs['ts'], '%Y-%m-%dT%H:%M:%S').strftime('%H')))
    if ret != 0:
        raise AirflowException('appsflyer_s3_to_hdfs_func error')


for event_type in ['installs']:
    bucket_name = 'af-ext-raw-data'
    bucket_key = os.path.join('cootek-gmail-com/data-locker-hourly', 't=%s' % event_type)
    af_s3_path = 's3://%s/%s' % (bucket_name, bucket_key)
    af_hdfs_path = 'hdfs:///user/gbdata/raw/appsflyer_data_locker_hourly/' + event_type
    priority_weight = 50 if event_type == 'installs' else 1
    wait_appsflyer_s3 = airflow_utils.FileSensor(
        dag=dag,
        task_id='wait_hourly_appsflyer_s3_%s' % event_type,
        timeout=5 * 3600,
        file_path=af_s3_path + '/dt={{ds}}/h={{macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S", "%-H")}}/_SUCCESS',
        priority_weight=priority_weight
    )
    appsflyer_s3_to_hdfs = PythonOperator(
        dag=dag,
        task_id='hourly_appsflyer_s3_to_hdfs_%s' % event_type,
        python_callable=appsflyer_s3_to_hdfs_func,
        provide_context=True,
        op_args=[af_s3_path, af_hdfs_path],
    )
    wait_appsflyer_s3.set_downstream(appsflyer_s3_to_hdfs)


hourly_appsflyer_push_data_template = '''
    cd %s/activate_source && python appsflyer_format.py -f {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}} -t {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}}
''' % local_config.WORKSPACE
hourly_appsflyer_push_data = BashOperator(
    task_id='hourly_appsflyer_push_data',
    bash_command=hourly_appsflyer_push_data_template,
    dag=dag,
)

wait_appsflyer_push_api = TimeDeltaSensor(
    dag=dag,
    task_id='wait_appsflyer_push_api',
    delta=timedelta(hours=0.6)
)
wait_appsflyer_push_api.set_downstream(hourly_appsflyer_push_data)

wait_activate_data_ready = TimeDeltaSensor(
    dag=dag,
    task_id='wait_activate_data_ready',
    delta=timedelta(hours=1.5)
)


hourly_activate_source_template = '''
    cd %s/activate_source && python ./emr_activate_source.py -f {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}} -t {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}} -p %s
'''
for bu in data_config.PRODUCT_SECTIONS:
    hourly_activate_source = BashOperator(
        task_id='hourly_activate_source_%s' % bu,
        bash_command=hourly_appsflyer_push_data_template % (local_config.WORKSPACE, bu),
        dag=dag,
    )
    hourly_appsflyer_push_data.set_downstream(hourly_activate_source)
    wait_activate_data_ready.set_downstream(hourly_activate_source)


if __name__ == "__main__":
    dag.cli()
