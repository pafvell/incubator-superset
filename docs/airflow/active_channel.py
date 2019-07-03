from builtins import range
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.operators.sensors import TimeDeltaSensor, ExternalTaskSensor
from airflow.models import DAG
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
from dateutil.relativedelta import *
import os, logging

import data_config
import airflow_utils
import local_config

RETENTION_MONTH_DAY = 1
RC_SPLIT_DATE = '20170223'
logging.basicConfig(level=logging.INFO)

args = {
    'owner': 'kevin.yang',
    'start_date': datetime(2017, 1, 1),
    'retries':1,
    'email': data_config.AIRFLOW_USERS
}

dag_udfs = {
    'add_month': lambda din, months=-1: (datetime.strptime(din, '%Y%m%d') + relativedelta(months=months)).strftime('%Y%m%d'),
    'ret_months': lambda din: ' '.join([(datetime.strptime(din, '%Y%m%d') + relativedelta(months=-d)).strftime('%Y%m%d') for d in data_config.RETENTION_MONTHLY_REPORT_MONTHS]),
    'add_week': lambda din, weeks=-1: (datetime.strptime(din, '%Y%m%d') + relativedelta(days=weeks*7)).strftime('%Y%m%d'),
    'ret_weeks': lambda din: ' '.join([(datetime.strptime(din, '%Y%m%d') + relativedelta(days=-d*7)).strftime('%Y%m%d') for d in data_config.RETENTION_WEEKLY_REPORT_WEEKS])
}

dag = DAG(
    dag_id='launch_active_channel',
    default_args=args,
    schedule_interval = timedelta(days=1),
    user_defined_filters=dag_udfs,
    dagrun_timeout=timedelta(hours=airflow_utils.dag_timeout))


launch_active_channel_list = []
background_active_channel_list = []
rdau_channel_list = []
active_channel_template = '''cd %s/active_channel && python ./active_channel.py --dates {{ds_nodash}} --active_type %s -p %s'''
launch_active_channel_gb = BashOperator(
    task_id='launch_active_channel_gb',
    bash_command=active_channel_template % (local_config.WORKSPACE, 'kdau', 'gb'),
    dag=dag
)
set_all_launch_active_channel_gb_false = airflow_utils.create_set_flag_task(dag, 'all_launch_active_channel_gb', 'False', downstream=launch_active_channel_gb)
launch_active_channel_list.append(airflow_utils.create_set_flag_task(dag, 'all_launch_active_channel_gb', 'True', upstream=launch_active_channel_gb))

background_active_channel_gb = BashOperator(
    task_id='background_active_channel_gb',
    bash_command=active_channel_template % (local_config.WORKSPACE, 'tdau', 'gb'),
    dag=dag
)
set_all_active_channel_gb_false = airflow_utils.create_set_flag_task(dag, 'all_active_channel_gb', 'False', downstream=background_active_channel_gb)
background_active_channel_list.append(airflow_utils.create_set_flag_task(dag, 'all_active_channel_gb', 'True', upstream=background_active_channel_gb))

launch_active_channel_gct = BashOperator(
    task_id='launch_active_channel_gct',
    bash_command=active_channel_template % (local_config.WORKSPACE, 'kdau', 'gct'),
    dag=dag
)
set_all_launch_active_channel_gct_false = airflow_utils.create_set_flag_task(dag, 'all_launch_active_channel_gct', 'False', downstream=launch_active_channel_gct)
launch_active_channel_list.append(airflow_utils.create_set_flag_task(dag, 'all_launch_active_channel_gct', 'True', upstream=launch_active_channel_gct))

background_active_channel_gct = BashOperator(
    task_id='background_active_channel_gct',
    bash_command=active_channel_template % (local_config.WORKSPACE, 'tdau', 'gct'),
    dag=dag
)
set_all_active_channel_gct_false = airflow_utils.create_set_flag_task(dag, 'all_active_channel_gct', 'False', downstream=background_active_channel_gct)
background_active_channel_list.append(airflow_utils.create_set_flag_task(dag, 'all_active_channel_gct', 'True', upstream=background_active_channel_gct))

rdau_channel_gb = BashOperator(
    task_id='rdau_channel_gb',
    bash_command=active_channel_template % (local_config.WORKSPACE, 'rdau', 'gb'),
    dag=dag
)
set_all_rdau_channel_gb_false = airflow_utils.create_set_flag_task(dag, 'all_rdau_channel_gb', 'False', downstream=rdau_channel_gb)
rdau_channel_list.append(airflow_utils.create_set_flag_task(dag, 'all_rdau_channel_gb', 'True', upstream=rdau_channel_gb))

rdau_channel_gct = BashOperator(
    task_id='rdau_channel_gct',
    bash_command=active_channel_template % (local_config.WORKSPACE, 'rdau', 'gct'),
    dag=dag
)
set_all_rdau_channel_gct_false = airflow_utils.create_set_flag_task(dag, 'all_rdau_channel_gct', 'False', downstream=rdau_channel_gct)
rdau_channel_list.append(airflow_utils.create_set_flag_task(dag, 'all_rdau_channel_gct', 'True', upstream=rdau_channel_gct))


redis_appsflyer_user_template = '''
    cd %s/active_channel && python ./redis_appsflyer_user.py {{ds_nodash}} {{ds_nodash}}
''' % local_config.WORKSPACE
redis_appsflyer_user = BashOperator(
    task_id='redis_appsflyer_user',
    bash_command=redis_appsflyer_user_template,
    depends_on_past=True,
    dag=dag
    )
redis_appsflyer_user.set_upstream(background_active_channel_list)

wait_redis_table_ready = TimeDeltaSensor(
    dag=dag,
    task_id='wait_redis_table_ready',
    delta=timedelta(hours=1)
)

# backup redis ime_table
redis_backup_template = '''
    cd %s/utils && python redis_backup.py -d {{ds_nodash}}
''' % local_config.WORKSPACE
redis_backup = BashOperator(
    task_id='redis_backup',
    bash_command=redis_backup_template,
    dag=dag
)
redis_backup.set_upstream(wait_redis_table_ready)

# compare local and redis user active media source
redis_media_source_compare_template = '''
    cd %s/active_channel && python ./redis_media_source_compare.py {{ds_nodash}} {{ds_nodash}}
''' % local_config.WORKSPACE
redis_media_source_compare = BashOperator(
    task_id='redis_media_source_compare',
    bash_command=redis_media_source_compare_template,
    depends_on_past=True,
    dag=dag
    )
redis_media_source_compare.set_upstream([redis_backup, redis_appsflyer_user])

active_channel_overlap_template = '''
    cd %s/active_channel && python ./active_channel_overlap.py --dates {{ds_nodash}}
''' % local_config.WORKSPACE

active_channel_overlap = BashOperator(
    task_id='active_channel_overlap',
    bash_command=active_channel_overlap_template,
    dag=dag
)
active_channel_overlap.set_upstream(background_active_channel_list)

retention_source_dates = ' '.join(['{{macros.ds_format(macros.ds_add(ds, -%d), "%%Y-%%m-%%d", "%%Y%%m%%d")}}' % (d - 1) for d in data_config.RETENTION_REPORT_DAYS])
retention_source_report_template = '''
    cd %s/druid_report && python ./retention_source_report.py --dates %s
''' % (local_config.WORKSPACE, retention_source_dates)
retention_source_report = BashOperator(
    task_id='retention_source_report',
    bash_command = retention_source_report_template,
    dag=dag,
    depends_on_past=True
)

retention_source_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py --dates %s --data_source %s --json_path %s/druid_report
''' % (local_config.WORKSPACE, retention_source_dates, 'smartinput_retention_source', local_config.WORKSPACE)
retention_source_report_import = BashOperator(
    task_id='retention_source_report_import',
    bash_command=retention_source_report_import_template,
    depends_on_past=True,
    dag=dag
)
retention_source_report_import.set_upstream(retention_source_report)

daily_retention_report_template = '''
    cd %s/druid_report && python ./daily_retention_report.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE
daily_retention_report = BashOperator(
    task_id='daily_retention_report',
    bash_command = daily_retention_report_template,
    dag=dag
)

daily_retention_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/druid_report
''' % (local_config.WORKSPACE, 'smartinput_daily_retention', local_config.WORKSPACE)
daily_retention_report_import = BashOperator(
    task_id='daily_retention_report_import',
    bash_command=daily_retention_report_import_template,
    dag=dag
)
daily_retention_report_import.set_upstream(daily_retention_report)

user_retention_list = []
user_retention_template = '''
    cd %s/daily_retention && python ./user_retention.py --dates {{ds_nodash}} -p %s
'''

user_retention_gb = BashOperator(
    task_id='launch_retention_source_gb',
    bash_command=user_retention_template % (local_config.WORKSPACE, 'gb'),
    dag=dag
)
set_all_launch_retention_source_gb_false = airflow_utils.create_set_flag_task(dag, 'all_launch_retention_source_gb', 'False', downstream=user_retention_gb)
user_retention_list.append(airflow_utils.create_set_flag_task(dag, 'all_launch_retention_source_gb', 'True', upstream=user_retention_gb))

user_retention_gct = BashOperator(
    task_id='launch_retention_source_gct',
    bash_command=user_retention_template % (local_config.WORKSPACE, 'gct'),
    dag=dag
)
set_all_launch_retention_source_gct_false = airflow_utils.create_set_flag_task(dag, 'all_launch_retention_source_gct', 'False', downstream=user_retention_gct)
user_retention_list.append(airflow_utils.create_set_flag_task(dag, 'all_launch_retention_source_gct', 'True', upstream=user_retention_gct))

retention_source_success = DummyOperator(task_id='launch_retention_source', dag=dag)
retention_source_success.set_upstream(user_retention_list)

retention_source_success.set_downstream(daily_retention_report)
retention_source_success.set_downstream(retention_source_report)

tdau_gb_data_ready = DummyOperator(task_id='tdau_gb_data_ready', dag=dag)
kdau_gb_data_ready = DummyOperator(task_id='kdau_gb_data_ready', dag=dag)
rdau_gb_data_ready = DummyOperator(task_id='rdau_gb_data_ready', dag=dag)
tdau_gct_data_ready = DummyOperator(task_id='tdau_gct_data_ready', dag=dag)
kdau_gct_data_ready = DummyOperator(task_id='kdau_gct_data_ready', dag=dag)
rdau_gct_data_ready = DummyOperator(task_id='rdau_gct_data_ready', dag=dag)

tdau_gb_data_ready.set_downstream([set_all_active_channel_gb_false, set_all_launch_retention_source_gb_false])
tdau_gct_data_ready.set_downstream([set_all_active_channel_gct_false, set_all_launch_retention_source_gct_false])
kdau_gb_data_ready.set_downstream([set_all_launch_active_channel_gb_false, set_all_launch_retention_source_gb_false])
kdau_gct_data_ready.set_downstream([set_all_launch_active_channel_gct_false, set_all_launch_retention_source_gct_false])
rdau_gb_data_ready.set_downstream([set_all_rdau_channel_gb_false, set_all_launch_retention_source_gb_false])
rdau_gct_data_ready.set_downstream([set_all_rdau_channel_gct_false, set_all_launch_retention_source_gct_false])

for reg in ['us', 'eu', 'ap']:
    now = airflow_utils.FlagsSensor(
        dag = dag,
        task_id='dw_active_flag_%s' % reg,
        timeout=airflow_utils.wait_active_flag_timeout * 3600,
        region= reg,
        data_type='active_smartinput',
        pool='wait_active_pool',
    )
    now.set_downstream(tdau_gb_data_ready)

    now = airflow_utils.FlagsSensor(
        dag = dag,
        task_id='flow_active_flag_gct_%s' % reg,
        timeout=airflow_utils.wait_active_flag_timeout * 3600,
        region='flow_%s' % reg,
        data_type='active',
        pool='wait_active_pool',
    )
    now.set_downstream(tdau_gct_data_ready)

    now = airflow_utils.FlagsSensor(
        dag = dag,
        task_id='dw_launch_active_flag_%s' % reg,
        timeout=airflow_utils.wait_active_flag_timeout * 3600,
        region= reg,
        data_type='launch_active_smartinput',
        pool='wait_launch_active_pool',
    )
    now.set_downstream(kdau_gb_data_ready)

    now = airflow_utils.FlagsSensor(
        dag = dag,
        task_id='flow_launch_active_flag_gct_%s' % reg,
        timeout=airflow_utils.wait_active_flag_timeout * 3600,
        region='flow_%s' % reg,
        data_type='launch_active',
        pool='wait_active_pool',
    )
    now.set_downstream(kdau_gct_data_ready)

    now = airflow_utils.FlagsSensor(
        dag=dag,
        task_id='flow_rdau_%s' % reg,
        timeout=airflow_utils.wait_active_flag_timeout * 3600,
        region='flow_%s' % reg,
        data_type='rdau',
        pool='wait_active_pool',
    )
    now.set_downstream([rdau_gb_data_ready, rdau_gct_data_ready])

wait_swiftcall = airflow_utils.FlagsSensor(
    dag=dag,
    task_id='wait_launch_active_flag_swiftcall',
    pool='wait_launch_active_pool',
    timeout=airflow_utils.wait_active_flag_timeout * 3600,
    region='flow_cn',
    data_type='launch_active_swiftcall',
)
wait_swiftcall.set_downstream(kdau_gb_data_ready)

wait_swiftcall_bg_active = airflow_utils.FlagsSensor(
    dag=dag,
    task_id='wait_active_flag_swiftcall',
    pool='wait_active_pool',
    timeout=airflow_utils.wait_active_flag_timeout * 3600,
    region='flow_cn',
    data_type='active_swiftcall',
)
wait_swiftcall_bg_active.set_downstream(tdau_gb_data_ready)

activate_source_data_ready = DummyOperator(task_id='activate_source_data_ready', dag=dag)
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
activate_source_data_ready.set_downstream([set_all_active_channel_gb_false, set_all_active_channel_gct_false, set_all_launch_active_channel_gb_false, set_all_launch_active_channel_gct_false, set_all_rdau_channel_gb_false, set_all_rdau_channel_gct_false, set_all_launch_retention_source_gb_false, set_all_launch_retention_source_gct_false])

active_channel_success = DummyOperator(task_id='active_channel_success', dag=dag)
active_channel_success.set_upstream(background_active_channel_list + launch_active_channel_list + rdau_channel_list + [active_channel_overlap])

# monthly retention
def is_retention_month_day(**kwargs):
    return datetime.strptime(kwargs['ds_nodash'], '%Y%m%d').day == RETENTION_MONTH_DAY
cond_is_retention_month_day = ShortCircuitOperator(task_id='cond_retention_month_day', python_callable=is_retention_month_day, provide_context=True, dag=dag)
active_channel_success.set_downstream(cond_is_retention_month_day)

retention_source_month_success = DummyOperator(task_id='launch_retention_source_month', dag=dag)

user_retention_month_template = '''
    cd %s/daily_retention && python ./user_retention.py --dates {{ds_nodash|add_month}} -p %s --type 'month'
'''
for p in ['gb', 'gct']:
    user_retention_month = BashOperator(
        task_id='launch_retention_source_month_%s' % p,
        bash_command=user_retention_month_template % (local_config.WORKSPACE, p),
        dag=dag
    )
    cond_is_retention_month_day.set_downstream(user_retention_month)
    retention_source_month_success.set_upstream(user_retention_month)


monthly_retention_report_template = '''
        cd %s/druid_report && python ./retention_source_report.py --dates {{ds_nodash|ret_months}} --type 'month'
    ''' % local_config.WORKSPACE

monthly_retention_report = BashOperator(
    task_id='monthly_retention_report',
    bash_command=monthly_retention_report_template,
    dag=dag
)

retention_source_month_success.set_downstream(monthly_retention_report)

monthly_retention_report_import_template = '''
        cd %s/druid_report && python druid_report_import.py --dates {{ds_nodash|ret_months}} --data_source %s --json_path %s/druid_report
    ''' % (local_config.WORKSPACE, 'smartinput_retention_source_month', local_config.WORKSPACE)

monthly_retention_report_import = BashOperator(
    task_id='monthly_retention_report_import',
    bash_command=monthly_retention_report_import_template,
    dag=dag
)

monthly_retention_report_import.set_upstream(monthly_retention_report)

# weekly retention
def is_retention_week_day(**kwargs):
    return datetime.strptime(kwargs['ds_nodash'], '%Y%m%d').weekday() == 0
cond_is_retention_week_day = ShortCircuitOperator(task_id='cond_retention_week_day', python_callable=is_retention_week_day, provide_context=True, dag=dag)
active_channel_success.set_downstream(cond_is_retention_week_day)

retention_source_week_success = DummyOperator(task_id='launch_retention_source_week', dag=dag)

user_retention_week_template = '''
    cd %s/daily_retention && python ./user_retention.py --dates {{ds_nodash|add_week}} -p %s --type 'week'
'''
for p in ['gb', 'gct']:
    user_retention_week = BashOperator(
        task_id='launch_retention_source_week_%s' % p,
        bash_command=user_retention_week_template % (local_config.WORKSPACE, p),
        dag=dag
    )
    cond_is_retention_week_day.set_downstream(user_retention_week)
    retention_source_week_success.set_upstream(user_retention_week)


weekly_retention_report_template = '''
        cd %s/druid_report && python ./retention_source_report.py --dates {{ds_nodash|ret_weeks}} --type 'week'
    ''' % local_config.WORKSPACE

weekly_retention_report = BashOperator(
    task_id='weekly_retention_report',
    bash_command=weekly_retention_report_template,
    dag=dag
)

retention_source_week_success.set_downstream(weekly_retention_report)

weekly_retention_report_import_template = '''
        cd %s/druid_report && python druid_report_import.py --dates {{ds_nodash|ret_weeks}} --data_source %s --json_path %s/druid_report
    ''' % (local_config.WORKSPACE, 'smartinput_retention_source_week', local_config.WORKSPACE)

weekly_retention_report_import = BashOperator(
    task_id='weekly_retention_report_import',
    bash_command=weekly_retention_report_import_template,
    dag=dag
)

weekly_retention_report_import.set_upstream(weekly_retention_report)

## user_group deps
# wait for first 10 hour(00-09) data ready
wait_hourly_tdau = TimeDeltaSensor(
    dag=dag,
    task_id='wait_hourly_tdau',
    delta=timedelta(hours=12)
)

user_group_list = []
user_group_template = '''cd %s/active_channel && python ./user_group.py --dates "{{ds_nodash}}" -p %s'''
user_group_gb = BashOperator(
    task_id="user_group_gb",
    bash_command=user_group_template % (local_config.WORKSPACE, "gb"),
    dag=dag,
    depends_on_past=True,
)
user_group_gb.set_upstream(wait_hourly_tdau)
user_group_gb.set_upstream(background_active_channel_gb)
user_group_gb.set_upstream(launch_active_channel_gb)
user_group_gb.set_upstream(rdau_channel_gb)
user_group_list.append(user_group_gb)

user_group_gct = BashOperator(
    task_id="user_group_gct",
    bash_command=user_group_template % (local_config.WORKSPACE, "gct"),
    dag=dag,
    depends_on_past=True,
)
user_group_gct.set_upstream(wait_hourly_tdau)
user_group_gct.set_upstream(background_active_channel_gct)
user_group_gct.set_upstream(launch_active_channel_gct)
user_group_gct.set_upstream(rdau_channel_gct)
user_group_list.append(user_group_gct)

user_group_success = DummyOperator(task_id='user_group_success', dag=dag, depends_on_past=True)
user_group_success.set_upstream(user_group_list)

## backfill prev user_group
backfill_user_group_template = '''
    airflow clear -t "^user_group_gb$" -s {{ yesterday_ds }} -e {{ yesterday_ds }} -d -c launch_active_channel &&
    airflow clear -t "^user_group_gct$" -s {{ yesterday_ds }} -e {{ yesterday_ds }} -d -c launch_active_channel
'''
backfill_user_group = BashOperator(
    task_id='backfill_user_group',
    bash_command=backfill_user_group_template,
    dag=dag
)
backfill_user_group.set_upstream(tdau_gb_data_ready)
backfill_user_group.set_upstream(tdau_gct_data_ready)


if __name__ == "__main__":
    dag.cli()
