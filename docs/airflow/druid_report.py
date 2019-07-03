from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.sensors import S3KeySensor, TimeDeltaSensor, ExternalTaskSensor, SqlSensor
from airflow.models import DAG
from airflow.exceptions import AirflowException
from datetime import date, datetime, timedelta
import os

import data_config
import local_config
import airflow_utils

args = {
    'owner': 'tianfeng.jiang',
    'start_date': datetime(2017, 2, 25),
    'retries':1,
    'email': data_config.AIRFLOW_USERS
}

dag = DAG(
    dag_id='commercial_report',
    default_args=args,
    schedule_interval = timedelta(days=1),
    dagrun_timeout=timedelta(hours=airflow_utils.dag_timeout),
)

act_ad_stat_report_dates = ' '.join(['{{macros.ds_format(macros.ds_add(ds, -%d), "%%Y-%%m-%%d", "%%Y%%m%%d")}}' % (d - 1) for d in data_config.ACT_AD_STAT_REPORT_DATES])
# generate act ad stat report
act_ad_stat_report_template = '''
    cd %s/druid_report && python act_ad_stat_report.py --dates %s
''' % (local_config.WORKSPACE, act_ad_stat_report_dates)
act_ad_stat_report = BashOperator(
    task_id='act_ad_stat_report',
    bash_command = act_ad_stat_report_template,
    depends_on_past=True,
    dag=dag
)

# import act ad stat report
act_ad_stat_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py --dates %s --data_source %s --json_path %s/druid_report
''' % (local_config.WORKSPACE, act_ad_stat_report_dates, 'smartinput_act_ad_stat', local_config.WORKSPACE)
act_ad_stat_report_import = BashOperator(
    task_id='act_ad_stat_report_import',
    bash_command=act_ad_stat_report_import_template,
    depends_on_past=True,
    dag=dag
)
act_ad_stat_report.set_downstream(act_ad_stat_report_import)

wait_retention_source = ExternalTaskSensor(
    task_id='wait_retention_source',
    timeout=airflow_utils.wait_retention_timeout*3600,
    external_dag_id = 'launch_active_channel',
    external_task_id = 'launch_retention_source',
    execution_delta = timedelta(0),
    dag=dag)
wait_retention_source.set_downstream(act_ad_stat_report)

druid_report_template = '''
    cd %s/druid_report && python commercial_report.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE
druid_report = BashOperator(
    task_id='druid_report',
    bash_command = druid_report_template,
    dag=dag
)

commercial_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/druid_report
''' % (local_config.WORKSPACE, ' '.join(data_config.commercial_report_name_list), local_config.WORKSPACE)
commercial_report_import = BashOperator(
    task_id='commercial_report_import',
    bash_command=commercial_report_import_template,
    dag=dag
)
commercial_report_import.set_upstream(druid_report)

ad_stat_report_template = '''
    cd %s/druid_report && python ad_stat_report.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE
ad_stat_report = BashOperator(
    task_id='ad_stat_report',
    bash_command = ad_stat_report_template,
    dag=dag
)

ad_stat_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/druid_report
''' % (local_config.WORKSPACE, ' '.join(data_config.ad_stat_report_name_list), local_config.WORKSPACE)
ad_stat_report_import = BashOperator(
    task_id='ad_stat_report_import',
    bash_command=ad_stat_report_import_template,
    dag=dag
)
ad_stat_report_import.set_upstream(ad_stat_report)

financial_ad_stat_report_template = '''
    cd %s/druid_report && python financial_ad_stat_report.py {{ds_nodash}} {{ds_nodash}}
''' % local_config.WORKSPACE
financial_ad_stat_report = BashOperator(
    task_id='financial_ad_stat_report',
    bash_command=financial_ad_stat_report_template,
    dag=dag
)
financial_ad_stat_report.set_upstream(ad_stat_report)

financial_ad_stat_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/druid_report
''' % (local_config.WORKSPACE, 'smartinput_financial_ad_stat', local_config.WORKSPACE)
financial_ad_stat_import = BashOperator(
    task_id='financial_ad_stat_import',
    bash_command=financial_ad_stat_import_template,
    dag=dag
)
financial_ad_stat_import.set_upstream(financial_ad_stat_report)

financial_revenue2db_template = 'cd %s/druid_report && python financial_revenue_process.py -f {{ds_nodash}} -t {{ds_nodash}}'%local_config.WORKSPACE
financial_revenue2db = BashOperator(
    task_id='financial_revenue2db',
    bash_command=financial_revenue2db_template,
    dag=dag,
)
financial_revenue2db.set_upstream(ad_stat_report)

swiftcall_revenue_detail2db_template = 'cd %s/swiftcall && python swiftcall_revenue_post_data_process.py -f {{ds_nodash}} -t {{ds_nodash}}'%local_config.WORKSPACE
swiftcall_revenue_detail2db = BashOperator(
    task_id='swiftcall_revenue_detail2db',
    bash_command=swiftcall_revenue_detail2db_template,
    dag=dag,
)
swiftcall_revenue_detail2db.set_upstream(ad_stat_report)

now = ExternalTaskSensor(
    dag=dag,
    task_id='wait_swiftcall_credit',
    timeout=10 * 3600,
    external_dag_id='swiftcall_data_flow',
    external_task_id='swiftcall_credit_channel',
    execution_delta=timedelta(0)
)
now.set_downstream(swiftcall_revenue_detail2db)

# ads_channel_impression
wait_ads_impression = ExternalTaskSensor(
    task_id='wait_ads_impression',
    timeout=airflow_utils.wait_ad_user_timeout * 3600,
    external_dag_id='roi',
    external_task_id='ads_channel_impression',
    execution_delta=timedelta(0),
    dag=dag)
wait_ads_impression.set_downstream(act_ad_stat_report)

# generate ads_summary
ads_summary_template = '''
    cd %s/davinci && python ads_summary.py {{ds_nodash}} {{ds_nodash}}
''' % local_config.WORKSPACE
ads_summary = BashOperator(
    task_id='ads_summary',
    bash_command=ads_summary_template,
    dag=dag
)
ads_summary.set_upstream(wait_ads_impression)

copy_ads_summary_template = '''
    hadoop fs -rm -r -f /user/gbdata/share/ads_summary/{{ds_nodash}} &&
    hadoop fs -cp  -f /user/gbdata/data/druid_report/ads_summary/{{ds_nodash}} /user/gbdata/share/ads_summary/ &&
    hadoop fs -rm -r -f /user/gbdata/share/ads_summary_platform/{{ds_nodash}} &&
    hadoop fs -cp  -f /user/gbdata/data/druid_report/ads_summary_platform/{{ds_nodash}} /user/gbdata/share/ads_summary_platform/
'''
copy_ads_summary = BashOperator(
    task_id='copy_ads_summary',
    bash_command=copy_ads_summary_template,
    dag=dag,
    priority_weight=20
)
copy_ads_summary.set_upstream([ad_stat_report, ads_summary])

ssp_davinci_ctr_report_template = '''
    cd %s/davinci && python ssp_davinci_ctr.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % (local_config.WORKSPACE)
ssp_davinci_ctr_report = BashOperator(
    task_id='ssp_davinci_ctr',
    bash_command=ssp_davinci_ctr_report_template,
    dag=dag,
    priority_weight=20
)
ssp_davinci_ctr_report.set_upstream(ads_summary)
ssp_davinci_ctr_report.set_upstream(ad_stat_report)

ssp_davinci_ctr_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/davinci
''' % (local_config.WORKSPACE, 'smartinput_ssp_davinci_ctr', local_config.WORKSPACE)
ssp_davinci_ctr_import = BashOperator(
    task_id='ssp_davinci_ctr_import',
    bash_command=ssp_davinci_ctr_import_template,
    dag=dag
)
ssp_davinci_ctr_import.set_upstream(ssp_davinci_ctr_report)

admob_2nd_click_report_template = '''
    cd %s/usage_based_report && python admob_2nd_click.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % (local_config.WORKSPACE)
admob_2nd_click_report = BashOperator(
    task_id='admob_2nd_click_report',
    bash_command=admob_2nd_click_report_template,
    dag=dag,
    priority_weight=20
)
admob_2nd_click_report.set_upstream(ads_summary)

admob_2nd_click_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/usage_based_report
''' % (local_config.WORKSPACE, 'smartinput_admob_2nd_click', local_config.WORKSPACE)
admob_2nd_click_import = BashOperator(
    task_id='admob_2nd_click_import',
    bash_command=admob_2nd_click_import_template,
    dag=dag
)
admob_2nd_click_import.set_upstream(admob_2nd_click_report)

# usage_matrix
usage_hades_internal = ExternalTaskSensor(
    task_id='wait_usage_matrix',
    timeout=15 * 3600,
    external_dag_id='usage_matrix',
    external_task_id='usage_matrix_all_success',
    execution_delta=timedelta(0),
    dag=dag)
usage_hades_internal.set_downstream(admob_2nd_click_report)

# usage_channel
now = ExternalTaskSensor(
    task_id='wait_usage_channel',
    timeout = 15*3600,
    external_dag_id = 'usage_channel',
    external_task_id = 'usage_channel',
    execution_delta = timedelta(0),
    dag=dag)
now.set_downstream(druid_report) 

# unit_usage_channel
now = ExternalTaskSensor(
    task_id='wait_unit_usage_channel',
    timeout = 15*3600,
    external_dag_id = 'usage_channel',
    external_task_id = 'unit_usage_channel',
    execution_delta = timedelta(0),
    dag=dag)
now.set_downstream(druid_report) 

# revenue_channel
now = ExternalTaskSensor(
    task_id='wait_ad_user',
    timeout=airflow_utils.wait_ad_user_timeout*3600,
    external_dag_id = 'roi',
    external_task_id = 'ad_user',
    execution_delta = timedelta(0), 
    dag=dag)
now.set_downstream([druid_report, ad_stat_report, ads_summary])
now.set_downstream(act_ad_stat_report)
now.set_downstream(admob_2nd_click_report)

# wait_active_channel kdau + tdau
wait_active_channel = ExternalTaskSensor(
    task_id='wait_active_channel',
    timeout=airflow_utils.wait_active_channel_timeout*3600,
    external_dag_id = 'launch_active_channel',
    external_task_id = 'active_channel_success',
    execution_delta = timedelta(0),
    dag=dag)
wait_active_channel.set_downstream(druid_report)
wait_active_channel.set_downstream(ad_stat_report)

pre_ad_stat_report_template = ''' 
    cd %s/druid_report && python ad_stat_report.py {{ds_nodash}} {{ds_nodash}} --pre_process
''' % local_config.WORKSPACE
pre_ad_stat_report = BashOperator(
    task_id='pre_ad_stat_report',
    bash_command=pre_ad_stat_report_template,
    dag=dag
)

pre_ad_stat_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/druid_report
''' % (local_config.WORKSPACE, ' '.join(data_config.ad_stat_report_name_list[:2]), local_config.WORKSPACE)
pre_ad_stat_report_import = BashOperator(
    task_id='pre_ad_stat_report_import',
    bash_command=pre_ad_stat_report_import_template,
    dag=dag
)
pre_ad_stat_report_import.set_upstream(pre_ad_stat_report)
pre_ad_stat_report_import.set_downstream(ad_stat_report)

wait_revenue_ready = ExternalTaskSensor(
    task_id='wait_pre_revenue_ready',
    timeout=8*3600,
    external_dag_id='revenue',
    external_task_id='pre_process_revenue',
    execution_delta=timedelta(0),
    dag=dag)
wait_revenue_ready.set_downstream(pre_ad_stat_report)


if __name__ == "__main__":
    dag.cli()
