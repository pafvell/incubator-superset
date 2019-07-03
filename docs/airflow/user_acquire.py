from builtins import range
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.operators.sensors import S3KeySensor, TimeDeltaSensor, ExternalTaskSensor, SqlSensor
from airflow.models import DAG
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import os

import airflow_utils
import data_config
import local_config

SETTLE_DAY = 14
BACKFILL_DAYS = [SETTLE_DAY, 28]

args = {
    'owner': 'hongxiang.cai',
    'start_date': datetime(2017, 12, 3),
    'retries': 1,
    'email': data_config.AIRFLOW_USERS
}

dag = DAG(
    dag_id='user_acquire_v1',
    default_args=args,
    schedule_interval=timedelta(days=1),
    dagrun_timeout=timedelta(hours=36)
)

# user acquire data use UTC + 8
# wait 13 hours before pulling cost data\
wait_user_acquire_ready = TimeDeltaSensor(
    dag=dag,
    task_id='wait_user_acquire_ready',
    delta=timedelta(hours=13)
)

wait_activate_source = ExternalTaskSensor(
    dag=dag,
    task_id='wait_activate_source',
    timeout=10 * 3600,
    external_dag_id='activate_source',
    external_task_id='activate_source',
    execution_delta=timedelta(days=0)
)
wait_user_acquire_ready.set_downstream(wait_activate_source)

wait_appsflyer_ready = ExternalTaskSensor(
    dag=dag,
    task_id='wait_appsflyer_ready',
    timeout=6 * 3600,
    external_dag_id='activate_source',
    external_task_id='process_appsflyer',
    execution_delta=timedelta(days=0)
)

wait_account_map_ready = ExternalTaskSensor(
    dag=dag,
    task_id='wait_account_map_ready',
    timeout=4 * 3600,
    external_dag_id='roi',
    external_task_id='wait_account_map_his',
    execution_delta=timedelta(days=0)
)

process_user_acquire_template = '''
    cd %s/user_acquire && python process_cost.py --date "{{ds_nodash}}"
''' % local_config.WORKSPACE
process_user_acquire = BashOperator(
    task_id='process_user_acquire',
    bash_command=process_user_acquire_template,
    depends_on_past=True,
    dag=dag,
)

platforms = {
    'facebook': '',
    'google': '',
    'tapjoy': '',
    'affiliate_network': '',
    'snapchat': '',
    'yahoo': '',
    'applovin': '',
}

wait_user_acquire_backfill = TimeDeltaSensor(
    dag=dag,
    task_id='wait_user_acquire_backfill',
    delta=timedelta(hours=2)
)
wait_user_acquire_backfill.set_downstream(wait_user_acquire_ready)

archive_affiliate_network_config_template = '''
    cd %s/toolbox && python dump_database.py affiliate_network_config -f {{ds_nodash}} -t {{ds_nodash}} --dest %s --format csv
''' % (local_config.WORKSPACE, data_config.USER_ACQUIRE_CONFIG)

archive_affiliate_network_config = BashOperator(
    task_id='archive_affiliate_network_config',
    bash_command=archive_affiliate_network_config_template,
    depends_on_past=True,
    dag=dag,
    priority_weight=10
)

backfill_command_list = ['airflow clear -t "^user_acquire_%s$" -s {{macros.ds_format(macros.ds_add(ds, -7), "%%Y-%%m-%%d", "%%Y%%m%%d")}}'
                         ' -e {{macros.ds_format(macros.ds_add(ds, -1), "%%Y-%%m-%%d", "%%Y%%m%%d")}} -c user_acquire_v1' % key for key in ['google', 'facebook', 'snapchat']]
backfill_command_list.append('airflow clear -t "^process_user_acquire$" -s {{macros.ds_format(macros.ds_add(ds, -7), "%Y-%m-%d", "%Y%m%d")}}'
                             ' -e {{macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d")}} -d -c user_acquire_v1')
backfill_command_list += ['airflow clear -t "^user_acquire_%s$" -s {{macros.ds_format(macros.ds_add(ds, -%d), "%%Y-%%m-%%d", "%%Y%%m%%d")}}'
                         ' -e {{macros.ds_format(macros.ds_add(ds, -%d), "%%Y-%%m-%%d", "%%Y%%m%%d")}} -c user_acquire_v1' % (key, backfill_day, backfill_day) for key in ['facebook'] for backfill_day in BACKFILL_DAYS]
backfill_command_list += ['airflow clear -t "^process_user_acquire$" -s {{macros.ds_format(macros.ds_add(ds, -%d), "%%Y-%%m-%%d", "%%Y%%m%%d")}}'
                             ' -e {{macros.ds_format(macros.ds_add(ds, -%d), "%%Y-%%m-%%d", "%%Y%%m%%d")}} -d -c user_acquire_v1' % (backfill_day, backfill_day) for backfill_day in BACKFILL_DAYS]
backfill_command_list += ['airflow clear -t "^cond_is_settle_day$" -s {{macros.ds_format(macros.ds_add(ds, -27), "%Y-%m-%d", "%Y%m%d")}}'
                          ' -e {{macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d")}} -d -c user_acquire_v1']

backfill_user_acquire_template = ' && '.join(backfill_command_list)
backfill_user_acquire = BashOperator(
    task_id='backfill_user_acquire',
    bash_command=backfill_user_acquire_template,
    depends_on_past=True,
    dag=dag,
    priority_weight=20
)
backfill_user_acquire.set_upstream(wait_user_acquire_backfill)

ad_designer_creative_performance_template = 'cd %s/user_acquire && python ad_creative_performance.py -f {{ds_nodash}} -t {{ds_nodash}}' % local_config.WORKSPACE
ad_designer_creative_performance = BashOperator(
    task_id='ad_creative_performance_report',
    bash_command=ad_designer_creative_performance_template,
    depends_on_past=True,
    dag=dag
)

ad_designer_creative_performance_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/user_acquire --keep_data
''' % (local_config.WORKSPACE, 'smartinput_ad_performance', local_config.WORKSPACE)
ad_designer_creative_performance_import = BashOperator(
    task_id='ad_creative_performance_import',
    bash_command=ad_designer_creative_performance_import_template,
    dag=dag
)

backfill_ad_designer_creative_performance_template = '''airflow clear -t "^ad_creative_performance_report$" -s {{ yesterday_ds }} -e {{ yesterday_ds }} -d -c user_acquire_v1'''
backfill_ad_designer_creative_performance = BashOperator(
    task_id='backfill_ad_designer_creative_performance',
    bash_command=backfill_ad_designer_creative_performance_template,
    dag=dag,
    priority_weight=10,
)

ad_designer_creative_performance_import.set_upstream(ad_designer_creative_performance)
ad_designer_creative_performance.set_upstream(backfill_ad_designer_creative_performance)
wait_account_map_ready.set_downstream(backfill_ad_designer_creative_performance)
wait_appsflyer_ready.set_downstream(backfill_ad_designer_creative_performance)

for key, value in platforms.items():
    user_acquire_template = 'cd %s/user_acquire && python daily_cost.py --date {{ds_nodash}} %s' % (local_config.WORKSPACE, key)
    user_acquire = BashOperator(
        task_id='user_acquire_%s' % (key),
        bash_command=user_acquire_template,
        depends_on_past=True,
        pool='user_acquire_%s_pool' % (key),
        dag=dag
    )
    user_acquire.set_upstream(backfill_user_acquire)
    user_acquire.set_upstream(wait_user_acquire_ready)
    user_acquire.set_downstream(process_user_acquire)

    if key in ['affiliate_network']:
        user_acquire.set_upstream(archive_affiliate_network_config)
        user_acquire.set_upstream(wait_activate_source)

    if key in ['facebook', 'google']:
        ad_performance_template = 'cd %s/user_acquire && python daily_cost.py --date {{ds_nodash}} %s --level ad' % (local_config.WORKSPACE, key)
        ad_performance = BashOperator(
            task_id='ad_performance_%s' % (key),
            bash_command=ad_performance_template,
            depends_on_past=True,
            retries=3,
            pool='user_acquire_%s_pool' % (key),
            dag=dag
        )

        ad_performance.set_upstream(user_acquire)

        if key in ['facebook']:
            wait_download_ad_info = TimeDeltaSensor(
                dag=dag,
                task_id='wait_download_ad_info',
                delta=timedelta(hours=16)
            )

            download_ad_info_template = 'cd %s/user_acquire && python download_%s_ad_info.py {{tomorrow_ds_nodash}}' % (local_config.WORKSPACE, key)
            download_ad_info = BashOperator(
                task_id='download_%s_ad_info' % key,
                bash_command=download_ad_info_template,
                depends_on_past=True,
                pool='user_acquire_%s_pool' % key,
                dag=dag
            )

            process_ad_info_template = 'cd %s/user_acquire && python process_%s_ad_info.py {{tomorrow_ds_nodash}}' % (local_config.WORKSPACE, key)
            process_ad_info = BashOperator(
                task_id='process_%s_ad_info' % key,
                bash_command=process_ad_info_template,
                depends_on_past=True,
                pool='user_acquire_%s_pool' % key,
                dag=dag
            )

            wait_activate_source_for_ad_violation = ExternalTaskSensor(
                dag=dag,
                task_id='wait_activate_source_for_ad_violation',
                timeout=10 * 3600,
                external_dag_id='activate_source',
                external_task_id='activate_source',
                execution_delta=timedelta(days=0)
            )

            backfill_ad_violation_report_template = '''airflow clear -t "^ad_violation_report" -s {{ macros.ds_add(yesterday_ds, -1) }} -e {{ yesterday_ds }} -d -c user_acquire_v1'''
            backfill_ad_violation_report = BashOperator(
                task_id='backfill_ad_violation_report',
                bash_command=backfill_ad_violation_report_template,
                dag=dag,
                priority_weight=10,
            )

            ad_violation_report_template = 'cd %s/user_acquire && python ad_violation_report.py -f {{tomorrow_ds_nodash}} -t {{tomorrow_ds_nodash}}' % local_config.WORKSPACE
            ad_violation_report = BashOperator(
                task_id='ad_violation_report',
                bash_command=ad_violation_report_template,
                depends_on_past=True,
                retries=1,
                dag=dag
            )

            ad_violation_report_import_template = '''
                cd %s/druid_report && python druid_report_import.py -f {{tomorrow_ds_nodash}} -t {{tomorrow_ds_nodash}} --data_source %s --json_path %s/user_acquire
            ''' % (local_config.WORKSPACE, 'smartinput_ad_violation', local_config.WORKSPACE)
            ad_violation_report_import = BashOperator(
                task_id='ad_violation_report_import',
                bash_command=ad_violation_report_import_template,
                dag=dag
            )

            ad_performance.set_downstream(download_ad_info)
            wait_download_ad_info.set_downstream(download_ad_info)
            download_ad_info.set_downstream(process_ad_info)
            process_ad_info.set_downstream(backfill_ad_designer_creative_performance)

            process_ad_info.set_downstream(backfill_ad_violation_report)
            wait_activate_source_for_ad_violation.set_downstream(backfill_ad_violation_report)
            backfill_ad_violation_report.set_downstream(ad_violation_report)
            ad_violation_report.set_downstream(ad_violation_report_import)

            wait_account_map_ready.set_downstream(backfill_ad_violation_report)
        else:
            ad_performance.set_downstream(backfill_ad_designer_creative_performance)

user_acquire_db_template = '''cd %s/user_acquire && spark-submit --master=local[4] campaign2db.spark.py''' % (local_config.WORKSPACE)
user_acquire_db = BashOperator(
    task_id='user_acquire_db',
    bash_command=user_acquire_db_template,
    depends_on_past=True,
    dag=dag
)
user_acquire_db.set_upstream(process_user_acquire)

def is_settle_day(**kwargs):
    return datetime.strptime(kwargs['ds_nodash'], '%Y%m%d').day == SETTLE_DAY
cond_is_settle_day = ShortCircuitOperator(task_id='cond_is_settle_day', python_callable=is_settle_day, provide_context=True, dag=dag)
process_user_acquire.set_downstream(cond_is_settle_day)

last_month_start_date = '''{{ macros.datetime.strptime((macros.datetime.strptime(execution_date.strftime("%Y%m"), "%Y%m") - macros.timedelta(days=1)).strftime("%Y%m"), "%Y%m").strftime("%Y%m%d") }}'''
last_month_end_date = '''{{ (macros.datetime.strptime(execution_date.strftime("%Y%m"), "%Y%m") - macros.timedelta(days=1)).strftime("%Y%m%d") }}'''
partner_settlement_report_template = 'cd %s/user_acquire && python partner_settlement_report.py %s %s' % (local_config.WORKSPACE, last_month_start_date, last_month_end_date)
partner_settlement_report = BashOperator(
    task_id='partner_settlement_report',
    bash_command=partner_settlement_report_template,
    dag=dag
)
cond_is_settle_day.set_downstream(partner_settlement_report)

partner_settlement_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f %s -t %s --data_source %s --json_path %s/user_acquire --keep_data
''' % (local_config.WORKSPACE, last_month_start_date, last_month_start_date, 'smartinput_partner_settlement', local_config.WORKSPACE)
partner_settlement_report_import = BashOperator(
    task_id='partner_settlement_report_import',
    bash_command=partner_settlement_report_import_template,
    dag=dag
)
partner_settlement_report.set_downstream(partner_settlement_report_import)
