from datetime import datetime, timedelta

import os

from airflow import AirflowException
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

import data_config
import local_config
import os_utils

args = {
    'owner': 'tianfeng.jiang',
    'start_date': datetime(2018, 5, 4),
    'retries': 1,
    'email': data_config.AIRFLOW_USERS
}

dag = DAG(
    dag_id='hourly_revenue_v2',
    default_args=args,
    schedule_interval='@hourly',
    dagrun_timeout=timedelta(hours=2)
)

platforms = {
    'spider_platforms': ['mopub'],
    'api_platforms': ['google', 'yahoo', 'adx', 'yeahmobi', 'facebook']
}

process_hourly_revenue_template = '''
    cd %s/revenue && python process_revenue.py --date {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}} --hourly
''' % local_config.WORKSPACE
process_hourly_revenue = BashOperator(
    task_id='process_hourly_revenue',
    bash_command=process_hourly_revenue_template,
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE,
    priority_weight=90
)

for spider_platform in platforms['spider_platforms']:
    get_data_template = '''
        cd %s/revenue && python %s_revenue_spider.py --date {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}}
    ''' % (local_config.WORKSPACE, spider_platform)
    get_spider_data = BashOperator(
        task_id='get_%s_data' % spider_platform,
        bash_command=get_data_template,
        dag=dag,
    )
    get_spider_data.set_downstream(process_hourly_revenue)


for api_platform in platforms['api_platforms']:
    api_revenue_template = '''
        cd %s/revenue && python daily_revenue.py --date {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}} %s %s
    ''' % (local_config.WORKSPACE, api_platform, '--hourly')
    if api_platform == 'google':
        retry_time = 0
    else:
        retry_time = 1
    api_revenue = BashOperator(
        task_id='hourly_revenue_%s' % api_platform,
        bash_command=api_revenue_template,
        dag=dag,
        retries=retry_time
    )
    api_revenue.set_downstream(process_hourly_revenue)

report_template = '''
    cd %s/revenue && python hourly_revenue_report.py {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}} {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}}
''' % local_config.WORKSPACE
report_hourly_revenue = BashOperator(
    task_id='report_hourly_revenue',
    bash_command=report_template,
    dag=dag,
)
report_hourly_revenue.set_upstream(process_hourly_revenue)


hourly_revenue_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}} -t {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}} --data_source %s --json_path %s/revenue  --hourly --keep_data
''' % (local_config.WORKSPACE, 'smartinput_ad_hourly_revenue', local_config.WORKSPACE)
hourly_revenue_import = BashOperator(
    task_id='hourly_revenue_import',
    bash_command=hourly_revenue_import_template,
    dag=dag
)
hourly_revenue_import.set_upstream(report_hourly_revenue)

hourly_revenue_check_template = '''
    cd %s/revenue && python hourly_revenue_report.py {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}} {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}} --operate_check
''' % local_config.WORKSPACE
hourly_revenue_check = BashOperator(
    task_id='hourly_revenue_date_check',
    bash_command=hourly_revenue_check_template,
    retries=0,
    dag=dag
)
hourly_revenue_check.set_upstream(report_hourly_revenue)

platform_data_size_check_success = DummyOperator(
    task_id='check_platform_data_size_success',
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag)

all_platforms = []
for value in platforms.values():
    all_platforms.extend(value)
for platform in all_platforms:
    check_platform_data_size_template = '''
        cd %s/revenue && python check_hourly_revenue_status.py --platform %s --date {{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}}
    ''' % (local_config.WORKSPACE, platform)
    hourly_revenue_check = BashOperator(
        task_id='%s_data_size_check' % platform,
        bash_command=check_platform_data_size_template,
        retries=0,
        email_on_failure=False,
        execution_timeout=timedelta(hours=1.5),
        dag=dag
    )
    hourly_revenue_check.set_downstream(platform_data_size_check_success)
    hourly_revenue_check.set_upstream(report_hourly_revenue)

hourly_revenue_path = data_config.DW_DRUID_REPORT + "/smartinput_ad_hourly_revenue/{{macros.ds_format(ts, '%Y-%m-%dT%H:%M:%S', '%Y%m%d%H')}}"
hourly_revenue_share_dir = '/user/gbdata/share/hourly_revenue/'
three_days_ago_path = '/user/gbdata/share/hourly_revenue/{{macros.ds_format(macros.ds_add(ds, -3), "%Y-%m-%d", "%Y%m%d")}}*'
copy_hourly_revenue_template = '''
    hadoop fs -rm -r -f %s &&
    hadoop fs -rm -r -f %s{{macros.ds_format(ts, '%%Y-%%m-%%dT%%H:%%M:%%S', '%%Y%%m%%d%%H')}} &&
    hadoop fs -cp -f %s %s
''' % (three_days_ago_path, hourly_revenue_share_dir, hourly_revenue_path, hourly_revenue_share_dir)
copy_hourly_revenue = BashOperator(
    task_id='copy_hourly_revenue',
    bash_command=copy_hourly_revenue_template,
    dag=dag,
)
copy_hourly_revenue.set_upstream(report_hourly_revenue)


def hourly_revenue_alarm_func(**kwargs):
    hourly_revenue_alarm_check_cmd = '''
        cd %s/revenue &&
        python hourly_revenue_alarm.py %s
    ''' % (local_config.WORKSPACE, datetime.strptime(kwargs['ts'], "%Y-%m-%dT%H:%M:%S").strftime('%Y%m%d%H'))
    returncode, out_data, err_data = os_utils.local_run(hourly_revenue_alarm_check_cmd)

    if returncode != 0:
        raise AirflowException(err_data)
    if err_data and err_data.find('abnormal data is :') > 0:
        raise AirflowException(err_data[err_data.find('abnormal data is :'):])


hourly_revenue_alarm_emails = ['xiaona.lu@cootek.cn', 'lu.gan@cootek.cn',
                               'wanwan.wang@cootek.cn', 'yiyang.huang@cootek.cn', 'lin.zou@cootek.cn',
                               'libin.yang@cootek.cn', 'tianfeng.jiang@cootek.cn', 'yang.zhou@cootek.cn']
hourly_revenue_alarm = PythonOperator(
    dag=dag,
    task_id='hourly_revenue_alarm_check',
    python_callable=hourly_revenue_alarm_func,
    provide_context=True,
    email=hourly_revenue_alarm_emails,
    retries=0,
)
hourly_revenue_alarm.set_upstream(hourly_revenue_import)

hourly_revenue_alarm_success = DummyOperator(
    task_id='hourly_revenue_alarm_success',
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag)

hourly_revenue_alarm_success.set_upstream(hourly_revenue_alarm)


if __name__ == "__main__":
    dag.cli()
