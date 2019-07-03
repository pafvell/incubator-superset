import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.sensors import S3KeySensor, TimeDeltaSensor, ExternalTaskSensor, SqlSensor
from builtins import range
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta

import data_config
import s3_utils
import airflow_utils
import local_config

args = {
    'owner': 'libin.yang',
    'start_date': datetime(2017, 10, 23),
    'retries': 1,
    'email': data_config.AIRFLOW_USERS
}

dag = DAG(
    dag_id='swiftcall_data_flow',
    default_args=args,
    schedule_interval=timedelta(days=1),
    dagrun_timeout=timedelta(hours=airflow_utils.dag_timeout),
    concurrency=6,
    max_active_runs=3,
)

wait_swiftcall_data_download = TimeDeltaSensor(
    dag=dag,
    task_id='wait_swiftcall_data_download',
    delta=timedelta(hours=0.5)
)

swiftcall_data_download_template = '''
    cd %s/swiftcall && python download_swiftcall.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % local_config.WORKSPACE
swiftcall_data_download = BashOperator(
    task_id='swiftcall_data_download',
    bash_command=swiftcall_data_download_template,
    priority_weight=30,
    dag=dag
)
swiftcall_data_download.set_upstream(wait_swiftcall_data_download)

swiftcall_call_rate_download_template = '''
    cd %s/swiftcall && python download_swiftcall.py -f {{ds_nodash}} -t {{ds_nodash}} --data_type call_rate
''' % local_config.WORKSPACE
swiftcall_call_rate_download = BashOperator(
    task_id='swiftcall_call_rate_download',
    bash_command=swiftcall_call_rate_download_template,
    priority_weight=30,
    dag=dag
)
swiftcall_call_rate_download.set_upstream(wait_swiftcall_data_download)

swiftcall_call_performance_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/swiftcall --keep_data
''' % (local_config.WORKSPACE, data_config.swift_performance_report, local_config.WORKSPACE)
swiftcall_call_performance_import = BashOperator(
    task_id='swiftcall_call_performance_import',
    bash_command=swiftcall_call_performance_import_template,
    dag=dag
)

swiftcall_call_performance_template = '''
    cd %s/swiftcall && python swiftcall_call_performance.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % local_config.WORKSPACE
swiftcall_call_performance = BashOperator(
    task_id='swiftcall_call_performance',
    bash_command=swiftcall_call_performance_template,
    dag=dag
)
swiftcall_call_performance.set_upstream(swiftcall_data_download)
swiftcall_call_performance.set_downstream(swiftcall_call_performance_import)

wait_activate_source = ExternalTaskSensor(
    dag=dag,
    task_id='wait_activate_source',
    timeout=airflow_utils.wait_activate_source_timeout * 3600,
    external_dag_id='activate_source',
    external_task_id='activate_source',
    execution_delta=timedelta(days=0),
    depends_on_past=True,
)

wait_revenue_ready = ExternalTaskSensor(
    task_id='wait_revenue_ready',
    timeout=airflow_utils.wait_revenue_timeout*3600,
    external_dag_id='revenue',
    external_task_id='process_revenue',
    execution_delta=timedelta(0),
    dag=dag
)



swiftcall_user_activity_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/swiftcall --keep_data
''' % (local_config.WORKSPACE, data_config.swift_user_activity_report, local_config.WORKSPACE)
swiftcall_user_activity_import = BashOperator(
    task_id='swiftcall_user_activity_import',
    bash_command=swiftcall_user_activity_import_template,
    dag=dag
)

swiftcall_user_activity_template = '''
    cd %s/swiftcall && python swiftcall_activity_report.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % local_config.WORKSPACE
swiftcall_user_activity = BashOperator(
    task_id='swiftcall_user_activity',
    bash_command=swiftcall_user_activity_template,
    dag=dag
)
swiftcall_user_activity.set_upstream(swiftcall_data_download)
swiftcall_user_activity.set_upstream(swiftcall_call_rate_download)
swiftcall_user_activity.set_upstream([wait_activate_source, wait_revenue_ready])
swiftcall_user_activity.set_downstream(swiftcall_user_activity_import)

wait_swiftcall_launch_active = airflow_utils.FlagsSensor(
    dag=dag,
    task_id='wait_launch_active_flag_swiftcall',
    pool='wait_launch_active_pool',
    timeout=airflow_utils.wait_active_flag_timeout * 3600,
    region='flow_cn',
    data_type='launch_active_swiftcall',
)

swiftcall_linecost_channel_template = '''
    cd %s/swiftcall && python swiftcall_linecost_channel.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % local_config.WORKSPACE
swiftcall_linecost_channel = BashOperator(
    task_id='swiftcall_linecost_channel',
    bash_command=swiftcall_linecost_channel_template,
    dag=dag
)
swiftcall_linecost_channel.set_upstream(swiftcall_data_download)
swiftcall_linecost_channel.set_upstream(swiftcall_call_rate_download)
swiftcall_linecost_channel.set_upstream(wait_activate_source)
swiftcall_linecost_channel.set_upstream(wait_swiftcall_launch_active)

swiftcall_credit_channel_template = '''
    cd %s/swiftcall && python swiftcall_credit_channel.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % local_config.WORKSPACE
swiftcall_credit_channel = BashOperator(
    task_id='swiftcall_credit_channel',
    bash_command=swiftcall_credit_channel_template,
    dag=dag
)
# swiftcall_credit_channel depends on gb_server_swiftcall which is upstream of activate source
swiftcall_credit_channel.set_upstream(wait_activate_source)

# swiftcall messagecost add channel task
swiftcall_messagecost_channel_template = '''
    cd %s/swiftcall && python swiftcall_messagecost_channel.py --dates {{ds_nodash}}
''' % local_config.WORKSPACE
swiftcall_messagecost_channel = BashOperator(
    task_id='swiftcall_messagecost_channel',
    bash_command=swiftcall_messagecost_channel_template,
    dag=dag
)
# swiftcall messagecost data check task
swiftcall_messagecost_path = data_config.USAGE_DATA(date='{{ds_nodash}}', usage_type='gaia_sms_cost', region='cn', year='{{macros.ds_format(ds,"%Y-%m-%d", "%Y")}}')
wait_swiftcall_messagecost = airflow_utils.FileSensor(
    dag=dag,
    task_id='wait_cn_gaia_sms_cost',
    timeout=10*3600,
    file_path = swiftcall_messagecost_path + '/_SUCCESS',
)

# swiftcall_messagecost_channel depends on gb_server_swiftcall which is upstream of activate source and swiftcall messagecost data
swiftcall_messagecost_channel.set_upstream(wait_activate_source)
swiftcall_messagecost_channel.set_upstream(wait_swiftcall_messagecost)

# generate swiftcall messagecost report, it depends on swiftcall messagecost channel data
swiftcall_messagecost_report_template = '''
    cd %s/swiftcall && python ./swiftcall_messagecost_report.py --dates {{ds_nodash}}
''' % local_config.WORKSPACE
swiftcall_messagecost_report = BashOperator(
    dag=dag,
    task_id='swiftcall_messagecost_report',
    bash_command=swiftcall_messagecost_report_template
)
swiftcall_messagecost_report.set_upstream(swiftcall_messagecost_channel)

# import swiftcall messagecost report, it depends on swiftcall messagecost report data
swiftcall_messagecost_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py --dates {{ds_nodash}} --data_source %s --json_path %s/swiftcall --business_units gb
''' % (local_config.WORKSPACE, 'smartinput_swiftcall_messagecost', local_config.WORKSPACE)
swiftcall_messagecost_report_import = BashOperator(
    dag=dag,
    task_id='swiftcall_messagecost_report_import',
    bash_command=swiftcall_messagecost_report_import_template
)
swiftcall_messagecost_report_import.set_upstream(swiftcall_messagecost_report)

# backfill call rate data at 2 am (UTC+8)
backfill_call_rate_template = '''
    airflow clear -t "^swiftcall_call_rate_download$" -s {{ ds_nodash }} -e {{ ds_nodash }} -d -c swiftcall_data_flow
'''
backfill_call_rate = BashOperator(
    task_id='backfill_call_rate',
    bash_command=backfill_call_rate_template,
    dag=dag,
    priority_weight=10,
)

wait_backfill_call_rate = TimeDeltaSensor(
    dag=dag,
    task_id='wait_backfill_call_rate',
    delta=timedelta(hours=18)
)
wait_backfill_call_rate.set_upstream(wait_swiftcall_data_download)
backfill_call_rate.set_upstream(wait_backfill_call_rate)
