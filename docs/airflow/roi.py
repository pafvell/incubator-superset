from builtins import range
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.sensors import S3KeySensor, TimeDeltaSensor, ExternalTaskSensor, SqlSensor
from airflow.models import DAG
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import os

import airflow_utils
import data_config
import local_config

args = {
    'owner': 'hongxiang.cai',
    'start_date': datetime(2017, 1, 24),
    'retries':1,
    'email': data_config.AIRFLOW_USERS
}

dag = DAG(
    dag_id='roi',
    default_args=args,
    schedule_interval = timedelta(days=1),
    dagrun_timeout=timedelta(hours=airflow_utils.dag_timeout),
    max_active_runs=3,
)

ROI_CHANNEL_DELTA_DAYS = '14'
roi_channel_template = '''
    cd %s/roi_channel && python roi_channel.py -f {{yesterday_ds_nodash}} -t {{yesterday_ds_nodash}} -d %s --delay 1
'''%(local_config.WORKSPACE, ROI_CHANNEL_DELTA_DAYS)
roi_channel = BashOperator(
    task_id='roi_channel',
    bash_command = roi_channel_template,
    depends_on_past = True,
    dag=dag
)

ROI_WEEKLY_ISOWEEKDAY = '5'
WEEKLY_DELTA_DAYS = '7'
roi_weekly_template = '''
    cd %s/roi_channel && python roi_weekly.py -f {{yesterday_ds_nodash}} -t {{yesterday_ds_nodash}} -d %s --delay 1 -w %s
''' % (local_config.WORKSPACE, WEEKLY_DELTA_DAYS, ROI_WEEKLY_ISOWEEKDAY)
roi_weekly = BashOperator(
    task_id='roi_weekly',
    bash_command=roi_weekly_template,
    dag=dag
)

roi_weekly_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{yesterday_ds_nodash}} -t {{yesterday_ds_nodash}} --data_source %s --json_path %s/roi_channel -w %s 
''' % (local_config.WORKSPACE, 'smartinput_roi_weekly', local_config.WORKSPACE, ROI_WEEKLY_ISOWEEKDAY)
roi_weekly_import = BashOperator(
    task_id='roi_weekly_import',
    bash_command=roi_weekly_import_template,
    dag=dag
)
roi_weekly_import.set_upstream(roi_weekly)

druid_roi_report_template = '''
    cd %s/roi_channel && python roi_channel_report.py {{yesterday_ds_nodash}} {{yesterday_ds_nodash}}
'''%local_config.WORKSPACE
druid_roi_report = BashOperator(
    task_id='druid_roi_report',
    bash_command = druid_roi_report_template,
    dag=dag
)
druid_roi_report.set_upstream(roi_channel)

druid_roi_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{yesterday_ds_nodash}} -t {{yesterday_ds_nodash}} --data_source %s --json_path %s/roi_channel 
''' % (local_config.WORKSPACE, 'smartinput_roi_channel', local_config.WORKSPACE)
druid_roi_import = BashOperator(
    task_id='roi_channel_import',
    bash_command=druid_roi_import_template,
    dag=dag
)
druid_roi_import.set_upstream(druid_roi_report)

roi_track_template = '''
    cd %s/roi_channel && python roi_track.py -f {{yesterday_ds_nodash}} -t {{yesterday_ds_nodash}} -d 14
'''%local_config.WORKSPACE
roi_track = BashOperator(
    task_id='roi_track',
    bash_command = roi_track_template,
    dag=dag
)
roi_track.set_upstream(roi_channel)

roi_track_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{yesterday_ds_nodash}} -t {{yesterday_ds_nodash}} --data_source %s --json_path %s/roi_channel 
''' % (local_config.WORKSPACE, 'smartinput_roi_track', local_config.WORKSPACE)
roi_track_import = BashOperator(
    task_id='roi_track_import',
    bash_command=roi_track_import_template,
    dag=dag
)
roi_track_import.set_upstream(roi_track)

fast_roi_apply_template = '''
    cd %s/roi_channel && python fast_roi.py {{yesterday_ds_nodash}} apply --mode local
'''%local_config.WORKSPACE
fast_roi_apply = BashOperator(
    task_id='fast_roi_apply',
    bash_command = fast_roi_apply_template,
    dag=dag
)

fast_roi_apply_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{yesterday_ds_nodash}} -t {{yesterday_ds_nodash}} --data_source %s --json_path %s/roi_channel 
''' % (local_config.WORKSPACE, 'smartinput_fast_roi', local_config.WORKSPACE)
fast_roi_apply_import = BashOperator(
    task_id='fast_roi_apply_import',
    bash_command=fast_roi_apply_import_template,
    dag=dag
)
fast_roi_apply_import.set_upstream(fast_roi_apply)

fast_roi_build_template = '''
    cd %s/roi_channel && python fast_roi.py {{yesterday_ds_nodash}} build -d 14 -w 5
'''%local_config.WORKSPACE
fast_roi_build = BashOperator(
    task_id='fast_roi_build',
    bash_command = fast_roi_build_template,
    dag=dag
)
fast_roi_build.set_downstream(fast_roi_apply)

roi_data_ready = DummyOperator(task_id='roi_data_ready', dag=dag)
roi_data_ready.set_downstream([roi_channel, roi_weekly, fast_roi_build])
#wait pre day of base data
# user_acquire ok

# add act_variance report
act_variance_report_template = '''
    cd %s/user_acquire && python ua_act_variance.py --dates {{ds_nodash}}
'''%local_config.WORKSPACE
act_variance_report = BashOperator(
    task_id='act_variance_report',
    bash_command = act_variance_report_template,
    depends_on_past = True,
    dag=dag
)
date_mapper = {day: -6 if day == 6 else 0 for day in range(7)}
# import activate variance report
act_variance_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ macros.ds_format(macros.ds_add(ds, params[execution_date.weekday()]), "%%Y-%%m-%%d", "%%Y%%m%%d") }} -t {{ds_nodash}} --data_source %s --json_path %s/user_acquire
''' % (local_config.WORKSPACE, 'smartinput_user_acquire', local_config.WORKSPACE)
act_variance_report_import = BashOperator(
    dag=dag,
    task_id='act_variance_report_import',
    bash_command=act_variance_report_import_template,
    depends_on_past=True,
    params=date_mapper
)
act_variance_report_import.set_upstream(act_variance_report)
# backfill activate varince report
backfill_act_variance_report_template = '''
    airflow clear -t "^act_variance_report$" -s {{ yesterday_ds }} -e {{ yesterday_ds }} -d -c roi
'''
backfill_act_variance_report = BashOperator(
    task_id='backfill_act_variance_report',
    bash_command=backfill_act_variance_report_template,
    dag=dag,
    priority_weight=10,
)
backfill_act_variance_report.set_downstream(act_variance_report)

ezalter_lt_data_ready = DummyOperator(task_id='ezalter_lt_data_ready', dag=dag)
# retention_detail
now = ExternalTaskSensor(
    task_id='wait_retention_source',
    timeout=airflow_utils.wait_retention_timeout*3600,
    external_dag_id = 'launch_active_channel',
    external_task_id = 'launch_retention_source',
    execution_delta = timedelta(0), # note: previous day data
    dag=dag)
now.set_downstream([roi_data_ready, ezalter_lt_data_ready])

# generate purchase join dau report
purchase_dau_summary_template = '''
    cd %s/purchase && python ./purchase_dau_summary.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % local_config.WORKSPACE

purchase_dau_summary = BashOperator(
    dag=dag,
    task_id='purchase_dau_summary_report',
    bash_command=purchase_dau_summary_template
)

purchase_dau_summary_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/purchase
''' % (local_config.WORKSPACE, 'smartinput_purchase_dau_summary', local_config.WORKSPACE)
purchase_dau_summary_import = BashOperator(
    dag=dag,
    task_id='purchase_dau_summary_import',
    bash_command=purchase_dau_summary_import_template
)
purchase_dau_summary_import.set_upstream(purchase_dau_summary)

# wait all_active_channel
for d in range(2):
    now = ExternalTaskSensor(
        task_id='wait_active_channel_success_%d' % (d),
        timeout=airflow_utils.wait_active_channel_timeout*3600,
        external_dag_id = 'launch_active_channel',
        external_task_id = 'active_channel_success',
        execution_delta = timedelta(days=d),
        dag=dag)
    now.set_downstream(roi_data_ready)
    now.set_downstream(purchase_dau_summary)


real_roi_template = '''
    cd %s/roi_channel && python real_roi_report.py -f {{yesterday_ds_nodash}} -t {{ds_nodash}}
'''%local_config.WORKSPACE
real_roi = BashOperator(
    task_id='real_roi',
    bash_command = real_roi_template,
    task_concurrency=1,
    depends_on_past = True,
    dag=dag
)

real_roi_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{yesterday_ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/roi_channel
''' % (local_config.WORKSPACE, 'smartinput_real_roi', local_config.WORKSPACE)
real_roi_import = BashOperator(
    task_id='real_roi_import',
    bash_command=real_roi_import_template,
    task_concurrency=1,
    depends_on_past = True,
    dag=dag
)
real_roi_import.set_upstream(real_roi)

real_roi_summary_dates = ' '.join(['{{macros.ds_format(macros.ds_add(ds, -%d), "%%Y-%%m-%%d", "%%Y%%m%%d")}}' % (d - 1) for d in data_config.REAL_ROI_SUMMARY_DATES])
real_roi_summary_report_template = '''
    cd %s/roi_channel && python real_roi_summary_report.py --dates %s
''' % (local_config.WORKSPACE, real_roi_summary_dates)
real_roi_summary_report = BashOperator(
    task_id='real_roi_summary_report',
    bash_command=real_roi_summary_report_template,
    task_concurrency=1,
    depends_on_past = True,
    dag=dag
)

real_roi_summary_import_dates = ' '.join(['{{macros.ds_format(macros.ds_add(ds, -%d), "%%Y-%%m-%%d", "%%Y%%m%%d")}}' % (d - 1) for d in data_config.REAL_ROI_SUMMARY_IMPORT_DATES])
real_roi_summary_import_template = '''
    cd %s/druid_report && python druid_report_import.py --dates %s --data_source %s --json_path %s/roi_channel 
''' % (local_config.WORKSPACE, real_roi_summary_import_dates, 'smartinput_real_roi_summary', local_config.WORKSPACE)
real_roi_summary_import = BashOperator(
    task_id='real_roi_summary_import',
    bash_command=real_roi_summary_import_template,
    task_concurrency=1,
    depends_on_past = True,
    dag=dag
)
real_roi_summary_import.set_upstream(real_roi_summary_report)

roi_performance_dates = ' '.join(['{{macros.ds_format(macros.ds_add(ds, -%d), "%%Y-%%m-%%d", "%%Y%%m%%d")}}' % (d - 1) for d in data_config.ROI_PERFORMANCE_DATES])
roi_weekly_performance_template = '''
    cd %s/roi_channel && python roi_weekly_performance.py --dates %s -d %s -w %s
''' % (local_config.WORKSPACE, roi_performance_dates, WEEKLY_DELTA_DAYS, ROI_WEEKLY_ISOWEEKDAY)
roi_weekly_performance = BashOperator(
    task_id='roi_weekly_performance',
    bash_command=roi_weekly_performance_template,
    dag=dag
)
roi_weekly_performance.set_upstream(roi_weekly)
roi_weekly_performance.set_upstream(real_roi_summary_report)


roi_weekly_performance_import_template = '''
    cd %s/druid_report && python druid_report_import.py --dates %s --data_source %s --json_path %s/roi_channel -w %s 
''' % (local_config.WORKSPACE, roi_performance_dates, 'smartinput_roi_weekly_performance', local_config.WORKSPACE, ROI_WEEKLY_ISOWEEKDAY)
roi_weekly_performance_import = BashOperator(
    task_id='roi_weekly_performance_import',
    bash_command=roi_weekly_performance_import_template,
    dag=dag
)
roi_weekly_performance_import.set_upstream(roi_weekly_performance)

roi_channel_performance_template = '''
    cd %s/roi_channel && python roi_channel_performance.py --dates %s -d %s 
''' % (local_config.WORKSPACE, roi_performance_dates, ROI_CHANNEL_DELTA_DAYS)
roi_channel_performance = BashOperator(
    task_id='roi_channel_performance',
    bash_command=roi_channel_performance_template,
    dag=dag
)
roi_channel_performance.set_upstream(real_roi_summary_report)

roi_channel_performance_import_template = '''
    cd %s/druid_report && python druid_report_import.py --dates %s --data_source %s --json_path %s/roi_channel 
''' % (local_config.WORKSPACE, roi_performance_dates, 'smartinput_roi_channel_performance', local_config.WORKSPACE)
roi_channel_performance_import = BashOperator(
    task_id='roi_channel_performance_import',
    bash_command=roi_channel_performance_import_template,
    dag=dag
)
roi_channel_performance_import.set_upstream(roi_channel_performance)



financial_cost2db_template = 'cd %s/roi_channel && python financial_cost_process.py -f {{macros.ds_format(macros.ds_add(ds, -7), "%%Y-%%m-%%d", "%%Y%%m%%d")}} -t {{ds_nodash}}' % (local_config.WORKSPACE)
financial_cost2db = BashOperator(
    task_id='financial_cost2db',
    bash_command=financial_cost2db_template,
    depends_on_past=True,
    dag=dag
)
financial_cost2db.set_upstream(real_roi_summary_report)

# placement_to_tu
os.environ["AIRFLOW_CONN_IME_CONFIG"] = "mysql://%s:%s@%s/ime_config" % (data_config.MYSQL_US['user'], data_config.MYSQL_US['password'], data_config.MYSQL_US['host'])
map_his = SqlSensor(
    task_id='wait_placement_map_his',
    conn_id='ime_config',
    sql="select * from ime_advertise_tu_placement_map_his where date={{ds_nodash}} limit 10",
    timeout=4 * 3600,
    dag=dag)


process_talia_usage_template = '''
    cd %s/usage_based_report && python talia_data_process.py {{ds_nodash}} {{ds_nodash}}
''' % local_config.WORKSPACE
process_talia_usage = BashOperator(
    task_id='process_talia_usage',
    bash_command=process_talia_usage_template,
    dag=dag
)


talia_click_template = '''
    cd %s/davinci && python talia_click.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % local_config.WORKSPACE
talia_click = BashOperator(
    task_id='talia_click',
    bash_command=talia_click_template,
    dag=dag
)
talia_click.set_upstream(process_talia_usage)


wait_ezalter_lt = airflow_utils.FileSensor(
    dag=dag,
    task_id='wait_ezalter_lt',
    timeout=10*3600,
    file_path='hdfs:///data/ezalter_data/exp_identifier_info_for_lt/{{tomorrow_ds_nodash}}/*/_SUCCESS',
)
wait_ezalter_lt.set_downstream(ezalter_lt_data_ready)

ad_user_template = '''
    cd %s/davinci && python ad_user.py -f {{ds_nodash}} -t {{ds_nodash}}
'''%local_config.WORKSPACE
ad_user = BashOperator(
    task_id='ad_user',
    bash_command = ad_user_template,
    dag=dag
)
ad_user.set_downstream(roi_data_ready)
ad_user.set_downstream([real_roi, real_roi_summary_report, ezalter_lt_data_ready])
ad_user.set_upstream(map_his)
ad_user.set_upstream(talia_click)

ezalter_lt_template = '''
    cd %s/daily_retention && python ezalter_lt.py -f {{ds_nodash}} -t {{ds_nodash}}
'''%local_config.WORKSPACE
ezalter_lt = BashOperator(
    task_id='ezalter_lt',
    bash_command=ezalter_lt_template,
    dag=dag
)
set_ezalter_lt_false = airflow_utils.create_set_flag_task(dag, 'ezalter_lt', 'False', upstream=ezalter_lt_data_ready, downstream=ezalter_lt)
set_ezalter_lt_true = airflow_utils.create_set_flag_task(dag, 'ezalter_lt', 'True', upstream=ezalter_lt)

ad_user_ltv_run_dates = ' '.join(['{{macros.ds_format(macros.ds_add(ds, -%d), "%%Y-%%m-%%d", "%%Y%%m%%d")}}' % (d - 1) for d in data_config.AD_USER_LTV_DAYS])
ad_user_ltv_template = '''
    cd %s/davinci && python ad_user_ltv.py --dates %s
''' % (local_config.WORKSPACE, ad_user_ltv_run_dates)
ad_user_ltv_report = BashOperator(
    task_id='ad_user_ltv_report',
    bash_command=ad_user_ltv_template,
    dag=dag
)
ad_user_ltv_report.set_upstream(ad_user)

ad_user_ltv_import_template = '''
    cd %s/druid_report && python druid_report_import.py --dates %s --data_source %s --json_path %s/davinci
''' % (local_config.WORKSPACE, ad_user_ltv_run_dates, 'smartinput_ad_user_ltv', local_config.WORKSPACE)
ad_user_ltv_import = BashOperator(
    task_id='ad_user_ltv_import',
    bash_command=ad_user_ltv_import_template,
    dag=dag
)
ad_user_ltv_import.set_upstream(ad_user_ltv_report)

copy_ad_user_templates = []
for bu in data_config.PATH_DICT:
    ad_user_path = os.path.join(data_config.AD_USER_PARQUET.replace('*', bu, 1), '{{ds_nodash}}')
    ad_user_share_dir = '/user/gbdata/share/ad_user_%s/' % bu
    three_days_ago_path = '/user/gbdata/share/ad_user_%s' % bu + '/{{macros.ds_format(macros.ds_add(ds, -3), "%Y-%m-%d", "%Y%m%d")}}'
    copy_ad_user_templates.append('''hadoop fs -rm -r -f %s &&
        hadoop fs -rm -r -f %s{{ds_nodash}} &&
        hadoop fs -cp -f %s %s''' % (three_days_ago_path, ad_user_share_dir, ad_user_path, ad_user_share_dir))
global_copy_ad_user_template = ' && '.join(copy_ad_user_templates)
copy_ad_user = BashOperator(
    task_id='copy_ad_user',
    bash_command=global_copy_ad_user_template,
    dag=dag,
)
set_all_ad_user_gb_false = airflow_utils.create_set_flag_task(dag, 'all_ad_user_gb', 'False', downstream=copy_ad_user)
set_all_ad_user_gct_false = airflow_utils.create_set_flag_task(dag, 'all_ad_user_gct', 'False', downstream=copy_ad_user)
ad_user.set_downstream(set_all_ad_user_gb_false)
ad_user.set_downstream(set_all_ad_user_gct_false)
set_all_ad_user_gb_true = airflow_utils.create_set_flag_task(dag, 'all_ad_user_gb', 'True', upstream=copy_ad_user)
set_all_ad_user_gct_true = airflow_utils.create_set_flag_task(dag, 'all_ad_user_gct', 'True', upstream=copy_ad_user)

# wait revenue dag finish
wait_revenue_ready = ExternalTaskSensor(
    task_id='wait_revenue_ready',
    timeout=airflow_utils.wait_revenue_timeout*3600,
    external_dag_id = 'revenue',
    external_task_id = 'process_revenue',
    execution_delta = timedelta(0),
    dag=dag)
wait_revenue_ready.set_downstream(ad_user)

wait_user_acquire_db = ExternalTaskSensor(
    task_id='wait_user_acquire_db',
    timeout=15 * 3600,
    external_dag_id='user_acquire_v1',
    external_task_id='user_acquire_db',
    execution_delta=timedelta(0),
    dag=dag)
wait_user_acquire_db.set_downstream(roi_data_ready)
wait_user_acquire_db.set_downstream([real_roi, real_roi_summary_report])
wait_user_acquire_db.set_downstream(backfill_act_variance_report)

wait_swiftcall_linecost = ExternalTaskSensor(
    task_id='wait_swiftcall_linecost',
    timeout=15 * 3600,
    external_dag_id='swiftcall_data_flow',
    external_task_id='swiftcall_linecost_channel',
    execution_delta=timedelta(0),
    dag=dag)
wait_swiftcall_linecost.set_downstream([real_roi, real_roi_summary_report])

wait_swiftcall_messagecost = ExternalTaskSensor(
    task_id='wait_swiftcall_messagecost',
    timeout=15 * 3600,
    external_dag_id='swiftcall_data_flow',
    external_task_id='swiftcall_messagecost_channel',
    execution_delta=timedelta(0),
    dag=dag)
wait_swiftcall_messagecost.set_downstream([real_roi, real_roi_summary_report])


#add activate source info to click data
ads_channel_template = '''
    cd %s/davinci && python ./ads_channel.py -f {{ds_nodash}} -t {{ds_nodash}}
'''%local_config.WORKSPACE

ads_channel = BashOperator(
    dag = dag,
    task_id = 'ads_channel',
    depends_on_past=True,
    bash_command = ads_channel_template
)
ads_channel.set_downstream(ad_user)

#add activate source info to impression data
ads_channel_impression_template = '''
    cd %s/davinci && python ./ads_channel_impression.py -f {{ds_nodash}} -t {{ds_nodash}}
'''%local_config.WORKSPACE

ads_channel_impression = BashOperator(
    dag = dag,
    task_id = 'ads_channel_impression',
    bash_command = ads_channel_impression_template
)

ad_data_ready = DummyOperator(task_id='ad_data_ready', dag=dag)
ad_data_ready.set_downstream([ads_channel, ads_channel_impression])

#add activate source info to goblin data
goblin_channel_template = '''
    cd %s/davinci && python ./goblin_channel.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE

goblin_channel = BashOperator(
    dag = dag,
    task_id = 'goblin_channel',
    bash_command = goblin_channel_template
)
goblin_channel.set_downstream(ad_user)

#goblin data checking
goblin_check_template = '''
    cd %s/davinci && python ./goblin_check.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE

goblin_check = BashOperator(
    dag = dag,
    task_id = 'goblin_check',
    bash_command = goblin_check_template
)
goblin_check.set_downstream(goblin_channel)

# wait goblin income ready (utc 6:00)
goblin_income_ready = TimeDeltaSensor(
    dag=dag,
    task_id='goblin_income_ready',
    delta = timedelta(hours=7)
)
goblin_income_ready.set_downstream(goblin_check)

#process raw goblin data
goblin_template = '''
    cd %s/davinci && python ./goblin.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE

goblin = BashOperator(
    dag = dag,
    task_id = 'goblin',
    bash_command = goblin_template
)
goblin.set_downstream(goblin_check)

get_currency_template = '''
    cd %s/revenue && python get_currency_ex.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % (local_config.WORKSPACE)
get_currency = BashOperator(
    task_id='get_currency_exchange_rate',
    bash_command=get_currency_template,
    dag=dag
)

# add activate source info to gb purchase data
purchase_channel_template = '''
    cd %s/purchase && python ./purchase_channel.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % local_config.WORKSPACE

purchase_channel = BashOperator(
    dag=dag,
    task_id='purchase_channel',
    bash_command=purchase_channel_template
)
purchase_channel.set_upstream(get_currency)
purchase_channel.set_downstream([real_roi, real_roi_summary_report])
purchase_channel.set_downstream(ad_user_ltv_report)
purchase_channel.set_downstream(purchase_dau_summary)

# generate GB purchase report
purchase_report_template = '''
    cd %s/purchase && python ./purchase_report.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % local_config.WORKSPACE

purchase_report = BashOperator(
    dag=dag,
    task_id='purchase_report',
    bash_command=purchase_report_template
)
purchase_report.set_upstream(purchase_channel)

# import GB purchase report
purchase_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/purchase
''' % (local_config.WORKSPACE, 'smartinput_purchase', local_config.WORKSPACE)

purchase_report_import = BashOperator(
    dag=dag,
    task_id='purchase_report_import',
    bash_command=purchase_report_import_template
)
purchase_report_import.set_upstream(purchase_report)

#wait activate_source_summary
wait_activate_source_summary = airflow_utils.FuncExternalTaskSensor(
    dag=dag,
    task_id='wait_activate_source_summary',
    timeout=airflow_utils.wait_activate_source_timeout*3600,
    external_dag_id = 'activate_source_summary',
    external_task_id = 'activate_source_summary',
    func = airflow_utils.get_last_friday_schedule
)
wait_activate_source_summary.set_downstream(ad_data_ready)

wait_activate_source_week = DummyOperator(task_id='wait_activate_source_week', dag=dag)
#wait last 8 days of activate source
for d in range(8): # ua_act_report need last 8 days activate_source data for timezone conversion
    now = ExternalTaskSensor(
        dag=dag,
        task_id='wait_activate_source_%d' % (d),
        timeout=airflow_utils.wait_activate_source_timeout*3600,
        external_dag_id = 'activate_source',
        external_task_id = 'activate_source',
        execution_delta = timedelta(days=d)
    )
    now.set_downstream(wait_activate_source_week)
wait_activate_source_week.set_downstream(ad_data_ready)
wait_activate_source_week.set_downstream(backfill_act_variance_report)
wait_activate_source_week.set_downstream(goblin_channel)
wait_activate_source_week.set_downstream(purchase_channel)

ads_click_ready = DummyOperator(task_id='ads_click_ready', dag=dag)
ads_click_ready.set_downstream(ad_data_ready)

for reg in ['us', 'ap', 'eu', 'cn']:
    ads_click_template = '''
        cd %s/davinci && python ./ads_click.py {{ds_nodash}} --region %s
    ''' % (local_config.WORKSPACE, reg)

    ads_click = BashOperator(
        dag=dag,
        task_id='ads_click_%s' % reg,
        bash_command=ads_click_template
    )

    ads_click_fix_template = '''
        cd %s/davinci && python ./ads_click_fix.py -f {{ds_nodash}} -t {{ds_nodash}} --region %s
    ''' % (local_config.WORKSPACE, reg)

    ads_click_fix = BashOperator(
        dag=dag,
        task_id='ads_click_fix_%s' % reg,
        depends_on_past=True,
        bash_command=ads_click_fix_template
    )

    ads_click.set_downstream(ads_click_fix)
    ads_click_fix.set_downstream(ads_click_ready)

    wait_data = airflow_utils.FlagsSensor(
        dag = dag,
        task_id='wait_%s_ad_sspstat_flag' % (reg),
        timeout=10*3600,
        pool='wait_ad_pool',
        region= '%s_compress_davinci' % reg,
        data_type='ad_sspstat',
        priority_weight=-2
    )
    wait_data.set_downstream(ads_click)

    product = 'gb_purchase'
    data_path = data_config.USAGE_DATA('{{ds_nodash}}', product, reg, '{{macros.ds_format(ds, "%Y-%m-%d", "%Y")}}')
    wait_purchase_data = airflow_utils.FileSensor(
        dag=dag,
        task_id='wait_%s_%s' % (reg, product),
        timeout=10 * 3600,
        file_path=data_path + '/_SUCCESS',
        pool='wait_purchase_pool',
        priority_weight=-2
    )
    wait_purchase_data.set_downstream(purchase_channel)

for reg in ['us', 'ap', 'eu']:
    ad = 'tk'
    wait_data = airflow_utils.FlagsSensor(
        dag = dag,
        task_id='wait_%s_ad_%s_flag' % (reg, ad),
        timeout=10*3600,
        pool='wait_ad_pool',
        region= '%s_compress_davinci' % reg,
        data_type='ad_%s' % ad,
        priority_weight=-2
    )
    wait_data.set_downstream(goblin)


wait_talia_usage = TimeDeltaSensor(
    dag=dag,
    task_id='wait_talia_usage',
    delta=timedelta(hours=8)
)
wait_talia_usage.set_downstream(process_talia_usage)


#wait ad_transform
ad = 'transform'
reg = 'us'
wait_data = airflow_utils.FlagsSensor(
    dag = dag,
    task_id='wait_%s_ad_%s_flag' % (reg, ad),
    timeout=10*3600,
    pool='wait_ad_pool',
    region= '%s_compress_davinci' % reg,
    data_type='ad_%s' % ad,
    priority_weight=-2
)
wait_data.set_downstream(goblin)

# daily dump mysql ua account map db
dump_account_map_template = '''
    cd %s/toolbox && python ./dump_database.py gb_ua_account_map -f {{ds_nodash}} -t {{ds_nodash}}
''' % local_config.WORKSPACE
dump_account_map = BashOperator(
    dag=dag,
    task_id='dump_account_map',
    bash_command=dump_account_map_template
)

# account map his
os.environ["AIRFLOW_CONN_ALADDIN"] = "mysql://%s:%s@%s/aladdin" % (data_config.MYSQL_UA_INFO['user'], data_config.MYSQL_UA_INFO['password'], data_config.MYSQL_UA_INFO['host'])
account_map_his = SqlSensor(
    task_id='wait_account_map_his',
    conn_id='aladdin',
    sql="select * from ua_ad_account_his where date={{ds_nodash}} limit 10",
    timeout=4 * 3600,
    dag=dag)
account_map_his.set_downstream([real_roi, roi_data_ready, real_roi_summary_report, backfill_act_variance_report])

if __name__ == "__main__":
    dag.cli()
