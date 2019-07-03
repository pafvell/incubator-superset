from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.sensors import S3KeySensor, TimeDeltaSensor, ExternalTaskSensor, SqlSensor
from airflow.models import DAG
from airflow.exceptions import AirflowException
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import os

import data_config
import local_config
import airflow_utils

args = {
    'owner': 'tianfeng.jiang',
    'start_date': datetime(2017, 5, 16),
    'retries':1,
    'email': data_config.AIRFLOW_USERS
}

dag = DAG(
    dag_id='dau_stat_report',
    default_args=args,
    schedule_interval = timedelta(days=1),
    dagrun_timeout=timedelta(hours=airflow_utils.dag_timeout),
    concurrency=6,
    max_active_runs=3,
)

# dau_stat_report
dau_stat_report_template = '''
    cd %s/dau_stat_report && python dau_stat_report.py --dates {{ds_nodash}}
'''%local_config.WORKSPACE
dau_stat_report = BashOperator(
    task_id='dau_stat_report',
    bash_command = dau_stat_report_template,
    dag=dag
)

dau_stat_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/dau_stat_report
''' % (local_config.WORKSPACE, 'smartinput_dau_stat', local_config.WORKSPACE)
dau_stat_report_import = BashOperator(
    task_id='dau_stat_report_import',
    bash_command=dau_stat_report_import_template,
    dag=dag
)
dau_stat_report_import.set_upstream(dau_stat_report)

# ad_user
ad_user = ExternalTaskSensor(
    task_id='wait_ad_user',
    timeout=airflow_utils.wait_ad_user_timeout*3600,
    external_dag_id = 'roi',
    external_task_id = 'ad_user',
    dag=dag)

# swiftcall_linecost_channel
swiftcall_linecost_channel = ExternalTaskSensor(
    task_id='wait_swiftcall_linecost_channel',
    timeout=10*3600,
    external_dag_id = 'swiftcall_data_flow',
    external_task_id = 'swiftcall_linecost_channel',
    dag=dag)

# wait_active_channel kdau + tdau + rdau + overlap
wait_active_channel = ExternalTaskSensor(
    task_id='wait_active_channel',
    timeout=airflow_utils.wait_active_channel_timeout*3600,
    external_dag_id = 'launch_active_channel',
    external_task_id = 'active_channel_success',
    execution_delta = timedelta(0),
    dag=dag)

# dau_stat_report dependencies
ad_user.set_downstream(dau_stat_report)
wait_active_channel.set_downstream(dau_stat_report)
swiftcall_linecost_channel.set_downstream(dau_stat_report)

# veeu join to matrix
pull_au_from_rainbow_template = '''
    cd %s/dau_stat_report && python3 veeu_join.py {{ds_nodash}}
''' % local_config.WORKSPACE
pull_au_from_rainbow = BashOperator(
    task_id='pull_au_from_rainbow',
    bash_command=pull_au_from_rainbow_template,
    dag=dag
)

join_veeu_report_template = '''
    cd %s/dau_stat_report && python veeu_join_report.py {{ds_nodash}}
''' % local_config.WORKSPACE
join_veeu_report = BashOperator(
    task_id='join_veeu_report',
    bash_command=join_veeu_report_template,
    dag=dag
)
join_veeu_report.set_upstream(pull_au_from_rainbow)

one_week_ago = '{{macros.ds_format(macros.ds_add(ds, -%d), "%%Y-%%m-%%d", "%%Y%%m%%d")}}' % 7
join_veeu_dau_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/dau_stat_report --keep_data --import_range %s {{ds_nodash}}
''' % (local_config.WORKSPACE, 'smartinput_joined_dau', local_config.WORKSPACE, one_week_ago)
join_veeu_dau_report_import = BashOperator(
    task_id='join_veeu_dau_report_import',
    bash_command=join_veeu_dau_report_import_template,
    dag=dag
)
join_veeu_dau_report_import.set_upstream(join_veeu_report)

two_month_ago = '{{macros.ds_format(macros.ds_add(ds, -%d), "%%Y-%%m-%%d", "%%Y%%m%%d")}}' % 60
join_veeu_mau_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/dau_stat_report --keep_data --import_range %s {{ds_nodash}}
''' % (local_config.WORKSPACE, 'smartinput_joined_mau', local_config.WORKSPACE, two_month_ago)
join_veeu_mau_report_import = BashOperator(
    task_id='join_veeu_mau_report_import',
    bash_command=join_veeu_mau_report_import_template,
    dag=dag
)
join_veeu_mau_report_import.set_upstream(join_veeu_report)


# pure skin report
pure_skin_report_template = '''
    cd %s/dau_stat_report && python pure_skin_report.py {{ds_nodash}} {{ds_nodash}}
''' % local_config.WORKSPACE
pure_skin_report = BashOperator(
    task_id='pure_skin_report',
    bash_command=pure_skin_report_template,
    dag=dag
)
pure_skin_report.set_upstream(wait_active_channel)

pure_skin_dau_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/dau_stat_report 
''' % (local_config.WORKSPACE, 'smartinput_pure_skin_dau', local_config.WORKSPACE)
pure_skin_dau_import = BashOperator(
    task_id='pure_skin_dau_import',
    bash_command=pure_skin_dau_import_template,
    dag=dag
)
pure_skin_dau_import.set_upstream(pure_skin_report)

first_day_this_month = '{{macros.ds_format(ds, "%Y-%m-%d", "%Y%m01")}}'
pure_skin_mau_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f %s -t %s --data_source %s --json_path %s/dau_stat_report
''' % (local_config.WORKSPACE, first_day_this_month, first_day_this_month, 'smartinput_pure_skin_mau', local_config.WORKSPACE)
pure_skin_mau_import = BashOperator(
    task_id='pure_skin_mau_import',
    bash_command=pure_skin_mau_import_template,
    dag=dag
)
pure_skin_mau_import.set_upstream(pure_skin_report)

usage_dau_compare_template = '''
    cd %s/dau_stat_report && python usage_dau_compare.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % local_config.WORKSPACE
usage_dau_compare = BashOperator(
    task_id='usage_dau_compare_report',
    bash_command=usage_dau_compare_template,
    dag=dag
)
usage_dau_compare.set_upstream(dau_stat_report)

usage_dau_compare_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/dau_stat_report
''' % (local_config.WORKSPACE, 'smartinput_usage_dau_compare', local_config.WORKSPACE)
usage_dau_compare_import = BashOperator(
    task_id='usage_dau_compare_import',
    bash_command=usage_dau_compare_import_template,
    dag=dag
)
usage_dau_compare_import.set_upstream(usage_dau_compare)

usage_matrix = ExternalTaskSensor(
    dag=dag,
    task_id='wait_usage_matrix',
    timeout=10 * 3600,
    external_dag_id='usage_matrix',
    external_task_id='usage_matrix_all_success',
)
usage_matrix.set_downstream(usage_dau_compare)

if __name__ == "__main__":
    dag.cli()
