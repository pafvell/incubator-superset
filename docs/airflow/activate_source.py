from builtins import range
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import S3KeySensor, TimeDeltaSensor, ExternalTaskSensor, SqlSensor
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException
import os

import data_config
import local_config
import airflow_utils
import os_utils

args = {
    'owner': 'yang.zhou',
    'start_date': datetime(2017, 1, 1),
    'retries':1,
    'email': data_config.AIRFLOW_USERS
}

dag = DAG(
    dag_id='activate_source', 
    default_args=args,
    schedule_interval = timedelta(days=1),
    dagrun_timeout=timedelta(hours=airflow_utils.dag_timeout))


# account map his
os.environ["AIRFLOW_CONN_ALADDIN"] = "mysql://%s:%s@%s/aladdin" % (data_config.MYSQL_UA_INFO['user'], data_config.MYSQL_UA_INFO['password'], data_config.MYSQL_UA_INFO['host'])
account_map_his = SqlSensor(
    task_id='wait_account_map_his',
    conn_id='aladdin',
    sql="select * from ua_ad_account_his where date={{ds_nodash}} limit 10",
    timeout=4 * 3600,
    dag=dag)


## activate report
activate_report_template = '''
    cd %s/activate_source && python activate_report.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE
activate_report = BashOperator(
    task_id='activate_report',
    bash_command = activate_report_template,
    dag=dag
)

def activate_upgrade_func(**kwargs):
    cmd = 'cd %s/activate_source && python ./activate_upgrade.py %s %s'%(local_config.WORKSPACE, kwargs['ds_nodash'], (airflow_utils.get_last_friday_schedule(kwargs['execution_date']) + timedelta(days=6)).strftime('%Y%m%d'))
    ret = os.system(cmd)
    if ret != 0:
        raise AirflowException('cmd: %s' % cmd)

activate_upgrade = PythonOperator(
    dag = dag,
    task_id = 'activate_upgrade',
    python_callable = activate_upgrade_func,
    provide_context = True
)

wait_activate_channel = airflow_utils.FuncExternalTaskSensor(
    dag=dag,
    task_id='wait_activate_channel',
    timeout=10*3600,
    external_dag_id = 'activate_source_summary',
    external_task_id = 'activate_channel',
    func = airflow_utils.get_last_friday_schedule
)
wait_activate_channel.set_downstream(activate_upgrade)

# activate report import
activate_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/activate_source
''' % (local_config.WORKSPACE, 'smartinput_activate_source', local_config.WORKSPACE)
activate_report_import = BashOperator(
    task_id='activate_report_import',
    bash_command=activate_report_import_template,
    dag=dag
)
activate_report_import.set_upstream(activate_report)

activate_upgrade_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/activate_source
''' % (local_config.WORKSPACE, 'smartinput_upgrade_type', local_config.WORKSPACE)
activate_upgrade_report_import = BashOperator(
    task_id='activate_upgrade_report_import',
    bash_command=activate_upgrade_report_import_template,
    dag=dag
)
activate_upgrade_report_import.set_upstream(activate_report)
activate_upgrade.set_downstream(activate_report)

activate_source_list = []
activate_source_gb_template = '''
    cd %s/activate_source && python ./emr_activate_source.py -f "{{ds_nodash}}" -t "{{ds_nodash}}" -p gb
''' % local_config.WORKSPACE
activate_source_gb = BashOperator(
    task_id='activate_source_gb',
    bash_command=activate_source_gb_template,
    dag=dag,
    depends_on_past=True,
    )
set_all_activate_source_gb_false = airflow_utils.create_set_flag_task(dag, 'all_activate_source_gb', 'False', downstream=activate_source_gb)
activate_source_list.append(airflow_utils.create_set_flag_task(dag, 'all_activate_source_gb', 'True', upstream=activate_source_gb))

activate_source_gct_template = '''
    cd %s/activate_source && python ./emr_activate_source.py -f "{{ds_nodash}}" -t "{{ds_nodash}}" -p gct
''' % local_config.WORKSPACE
activate_source_gct = BashOperator(
    task_id='activate_source_gct',
    bash_command=activate_source_gct_template,
    dag=dag,
    depends_on_past=True,
    )
set_all_activate_source_gct_false = airflow_utils.create_set_flag_task(dag, 'all_activate_source_gct', 'False', downstream=activate_source_gct)
activate_source_list.append(airflow_utils.create_set_flag_task(dag, 'all_activate_source_gct', 'True', upstream=activate_source_gct))

backfill_activate_source_template = '''
    airflow clear -t "^activate_source_gb$" -s {{ yesterday_ds }} -e {{ yesterday_ds }} -d -c activate_source &&
    airflow clear -t "^activate_source_gct$" -s {{ yesterday_ds }} -e {{ yesterday_ds }} -d -c activate_source &&
    airflow clear -t "^wait_activate_source$" -s {{ yesterday_ds }} -e {{ yesterday_ds }} -d -c user_acquire_v1
'''
backfill_activate_source = BashOperator(
    task_id='backfill_activate_source',
    bash_command=backfill_activate_source_template,
    dag=dag,
    priority_weight=10,
)

activate_source_data_ready = DummyOperator(task_id='activate_source_data_ready', dag=dag)
activate_source_data_ready.set_downstream(backfill_activate_source)
backfill_activate_source.set_downstream([set_all_activate_source_gb_false, set_all_activate_source_gct_false])

process_appsflyer_template = '''
    cd %s/activate_source && python ./process_appsflyer.py -f "{{ds_nodash}}" -t "{{ds_nodash}}"
''' % local_config.WORKSPACE
process_appsflyer = BashOperator(
    task_id='process_appsflyer',
    bash_command = process_appsflyer_template,
    dag=dag
    )
process_appsflyer.set_downstream(activate_source_data_ready)

download_appsflyer_template = '''
    cd %s/activate_source && python ./download_appsflyer.py installs_report -f "{{ds_nodash}}" -t "{{ds_nodash}}"
''' % local_config.WORKSPACE
download_appsflyer = BashOperator(
    task_id='download_appsflyer',
    bash_command = download_appsflyer_template,
    retries = 1,
    retry_delay = timedelta(seconds=20),
    dag=dag
    )

download_appsflyer_uninstall_template = '''
    cd %s/activate_source && python ./download_appsflyer.py uninstall_events_report -f "{{ds_nodash}}" -t "{{ds_nodash}}"
''' % local_config.WORKSPACE
download_appsflyer_uninstall = BashOperator(
    task_id='download_appsflyer_uninstall',
    bash_command = download_appsflyer_uninstall_template,
    retries = 1,
    retry_delay = timedelta(seconds=20),
    dag=dag
    )

download_appsflyer.set_downstream([process_appsflyer, download_appsflyer_uninstall])

download_appsflyer_website_template = '''
    cd %s/activate_source && python get_appsflyer_activate_data.py {{ds_nodash}}
''' % local_config.WORKSPACE
download_appsflyer_website = BashOperator(
    task_id='download_appsflyer_website',
    bash_command = download_appsflyer_website_template,
    retries = 1,
    retry_delay = timedelta(seconds=20),
    dag=dag
    )
download_appsflyer_website.set_downstream(download_appsflyer)

appsflyer_retention_dates = ' '.join(['{{macros.ds_format(macros.ds_add(ds, -%d), "%%Y-%%m-%%d", "%%Y%%m%%d")}}' % d for d in data_config.APPSFLYER_RETENTION_DAYS])
process_appsflyer_retention_template = '''
    cd %s/activate_source && python3 process_appsflyer_retention_data.py --dates %s
''' % (local_config.WORKSPACE, appsflyer_retention_dates)

process_appsflyer_retention = BashOperator(
    task_id='process_appsflyer_retention',
    bash_command=process_appsflyer_retention_template,
    task_concurrency=1,
    dag=dag,
    depends_on_past=True
    )
download_appsflyer_retention_template = '''
    cd %s/activate_source && python get_appsflyer_retention_data.py --dates %s --today {{yesterday_ds_nodash}}
''' % (local_config.WORKSPACE, appsflyer_retention_dates)
download_appsflyer_retention = BashOperator(
    task_id='download_appsflyer_retention',
    bash_command=download_appsflyer_retention_template,
    task_concurrency=1,
    retries=1,
    retry_delay=timedelta(seconds=30),
    dag=dag,
    depends_on_past=True
    )
process_appsflyer_retention.set_upstream(download_appsflyer_retention)

# wait 5 hours before pulling appsflyer retention data
wait_appsflyer_retention_ready = TimeDeltaSensor(
    dag=dag,
    task_id='wait_appsflyer_retention_ready',
    delta=timedelta(hours=5)
)
wait_appsflyer_retention_ready.set_downstream(download_appsflyer_retention)

wait_user_acquire_db = ExternalTaskSensor(
    task_id='wait_user_acquire_db',
    timeout=15 * 3600,
    external_dag_id='user_acquire_v1',
    external_task_id='user_acquire_db',
    execution_delta=timedelta(days=1),
    dag=dag)

# generate appsflyer retention report
appsflyer_retention_report_template = '''
    cd %s/activate_source && python appsflyer_retention_report.py --dates %s
''' % (local_config.WORKSPACE, appsflyer_retention_dates)
appsflyer_retention_report = BashOperator(
    task_id='appsflyer_retention_report',
    bash_command=appsflyer_retention_report_template,
    task_concurrency=1,
    depends_on_past=True,
    dag=dag
)
appsflyer_retention_report.set_upstream([account_map_his, process_appsflyer_retention, wait_user_acquire_db])

appsflyer_retention_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py --dates %s --data_source %s --json_path %s/activate_source
''' % (local_config.WORKSPACE, appsflyer_retention_dates, 'smartinput_appsflyer_retention', local_config.WORKSPACE)
appsflyer_retention_report_import = BashOperator(
    task_id='appsflyer_retention_report_import',
    bash_command=appsflyer_retention_report_import_template,
    task_concurrency=1,
    depends_on_past=True,
    dag=dag
)
appsflyer_retention_report_import.set_upstream(appsflyer_retention_report)

wait_appsflyer = TimeDeltaSensor(
    dag=dag,
    task_id='wait_appsflyer',
    delta = timedelta(hours=1)
)
wait_appsflyer.set_downstream([download_appsflyer, download_appsflyer_website])

appsflyer_s3_to_hdfs_done = DummyOperator(task_id='appsflyer_s3_to_hdfs_done', dag=dag)

def appsflyer_s3_to_hdfs_func(af_s3_path, af_hdfs_path, **kwargs):
    ret = os_utils.sync('s3://%s-%s/' % (af_s3_path, kwargs['ds']), '%s/%s' % (af_hdfs_path, kwargs['ds_nodash']))
    if ret != 0:
        raise AirflowException('appsflyer_s3_to_hdfs_func error')

for event_type in ['clicks', 'impressions', 'inapp', 'inappreattrReport', 'inappretargets', 'installs', 'launches', 'reattrReport', 'retargets', 'uninstalls']:
    bucket_name = 'af-ext-raw-data'
    bucket_key = os.path.join('cootek-gmail-com/data-locker', event_type)
    af_s3_path = bucket_name + '/' + bucket_key
    af_hdfs_path = 'hdfs:///user/gbdata/raw/appsflyer_data_locker/' + event_type
    priority_weight = 50 if event_type == 'installs' else 1
    wait_appsflyer_s3 = airflow_utils.FileSensor(
        dag=dag,
        task_id='wait_appsflyer_s3_%s' % event_type,
        timeout=10 * 3600,
        file_path='s3://%s-{{ds}}/_SUCCESS' % af_s3_path,
        priority_weight=priority_weight,
        pool='wait_data_locker'
    )
    appsflyer_s3_to_hdfs = PythonOperator(
        dag=dag,
        task_id='appsflyer_s3_to_hdfs_%s' % event_type,
        python_callable=appsflyer_s3_to_hdfs_func,
        provide_context=True,
        op_args=[af_s3_path, af_hdfs_path],
    )
    wait_appsflyer_s3.set_downstream(appsflyer_s3_to_hdfs)
    appsflyer_s3_to_hdfs.set_downstream(appsflyer_s3_to_hdfs_done)

wait_swiftcall = airflow_utils.FlagsSensor(
    dag=dag,
    task_id='wait_activate_flag_swiftcall',
    timeout=airflow_utils.wait_activate_flag_timeout * 3600,
    region='flow_cn',
    data_type='activate_swiftcall',
    pool='wait_activate_pool',
)
wait_swiftcall.set_downstream([activate_source_data_ready, activate_upgrade])

for reg in ['us', 'eu', 'ap']:
    now = airflow_utils.FlagsSensor(
        dag = dag,
        task_id='dw_usage_high_flag_%s' % reg,
        timeout=airflow_utils.wait_usage_flag_timeout * 3600,
        region=reg,
        data_type='usage_high',
        pool='wait_activate_pool',
    )
    now.set_downstream(activate_source_data_ready)

    for ime_usage in ['skin_plugin', 'ime_boomtext_plugin', 'ime_font_plugin', 'ime_sticker_plugin']:
        now = airflow_utils.FlagsSensor(
            dag=dag,
            task_id='wait_%s_flag_%s' % (ime_usage, reg),
            timeout=airflow_utils.wait_skin_plugin_flag_timeout * 3600,
            region=reg,
            data_type=ime_usage,
            pool='wait_activate_pool',
        )
        now.set_downstream(activate_source_data_ready)
    
    now = airflow_utils.FlagsSensor(
        dag = dag,
        task_id='dw_activate_flag_%s' % reg,
        timeout=airflow_utils.wait_activate_flag_timeout * 3600,
        region= reg,
        data_type='activate_smartinput',
        pool='wait_activate_pool',
    )
    now.set_downstream([activate_source_data_ready, activate_upgrade])

    now = airflow_utils.FlagsSensor(
        dag=dag,
        task_id='flow_activate_flag_gct_%s' % reg,
        timeout=airflow_utils.wait_activate_flag_timeout * 3600,
        region='flow_%s' % reg,
        data_type='activate',
        pool='wait_activate_pool',
    )
    now.set_downstream([activate_source_data_ready, activate_upgrade])

    now = airflow_utils.FlagsSensor(
        dag=dag,
        task_id='dw_usage_flag_biu_%s' % reg,
        timeout=airflow_utils.wait_usage_flag_timeout * 3600,
        region='flow_%s' % reg,
        data_type='usage_biu',
        pool='wait_activate_pool',
    )
    now.set_downstream(activate_source_data_ready)

for reg in ['us']:
    now = airflow_utils.FlagsSensor(
        dag = dag,
        task_id='dw_usage_flag_veeu_%s' % reg,
        timeout=airflow_utils.wait_usage_flag_timeout * 3600,
        region='flow_%s' % reg,
        data_type='usage_veeu',
        pool='wait_activate_pool',
    )
    now.set_downstream(activate_source_data_ready)

    for ime_usage in ['ime_language_plugin', 'ime_emoji_plugin', 'ime_cell_plugin']:
        now = airflow_utils.FlagsSensor(
            dag=dag,
            task_id='wait_%s_flag_%s' % (ime_usage, reg),
            timeout=airflow_utils.wait_skin_plugin_flag_timeout * 3600,
            region=reg,
            data_type=ime_usage,
            pool='wait_activate_pool',
        )
        now.set_downstream(activate_source_data_ready)
## usage matrix
usage_matrix = ExternalTaskSensor(
    dag=dag,
    task_id='wait_usage_matrix',
    timeout=10*3600,
    external_dag_id = 'usage_matrix',
    external_task_id = 'read_matrix_referrer_data',
    pool='wait_activate_pool',
)
usage_matrix.set_downstream(activate_source_data_ready)


activate_source_success = DummyOperator(task_id='activate_source', dag=dag, depends_on_past=True)
activate_source_success.set_upstream(activate_source_list)
activate_source_success.set_downstream(activate_report)

if __name__ == "__main__":
    dag.cli()
