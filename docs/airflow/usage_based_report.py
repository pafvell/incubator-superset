from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor, TimeDeltaSensor
from airflow.models import DAG
from datetime import datetime, timedelta

import local_config
import airflow_utils
import data_config

args = {
    'owner': 'kevin.yang',
    'start_date': datetime(2018, 3, 28),
    'retries': 1,
    'email': data_config.AIRFLOW_USERS
}

dag = DAG(
    dag_id='usage_based_report',
    default_args=args,
    schedule_interval=timedelta(days=1),
    dagrun_timeout=timedelta(hours=airflow_utils.dag_timeout))

wait_usage_list = []
wait_matrix_usage_list = []
wait_activate_source_list = []
performance_report_list = []
for action in ['UI_CAND', 'SHOW_KEYBOARD', 'SHOW_KEYBOARD_FIRST', 'HIDE_KEYBOARD', 'EMOJI', 'CURVE']:
    performance_report_template = '''
        cd %s/performance && python performance_report.py --date {{ds_nodash}} --action %s
    ''' % (local_config.WORKSPACE, action)
    performance_report = BashOperator(
        task_id='performance_report_%s' % action,
        bash_command=performance_report_template,
        dag=dag
    )
    performance_report_list.append(performance_report)

performance_report_upload_template = '''
    cd %s/performance && python performance_report_upload.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % local_config.WORKSPACE
performance_report_upload = BashOperator(
    task_id='performance_report_upload',
    bash_command=performance_report_upload_template,
    dag=dag
)

performance_report_upload.set_upstream(performance_report_list)
wait_usage_list.extend(performance_report_list)

crash_ratio_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/performance --business_units gb
''' % (local_config.WORKSPACE, data_config.crash_ratio_report_name, local_config.WORKSPACE)
crash_ratio_report_import = BashOperator(
    task_id='crash_ratio_report_import',
    bash_command=crash_ratio_report_import_template,
    dag=dag
)

crash_ratio_report_template = '''
    cd %s/performance && python crash_ratio_report.py --date {{ds_nodash}}
''' % local_config.WORKSPACE
crash_ratio_report = BashOperator(
    task_id='crash_ratio_report',
    bash_command=crash_ratio_report_template,
    dag=dag
)
wait_usage_list.append(crash_ratio_report)
crash_ratio_report.set_downstream(crash_ratio_report_import)


cdn_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/performance --business_units gb
''' % (local_config.WORKSPACE, data_config.cdn_report_name, local_config.WORKSPACE)
cdn_report_import = BashOperator(
    task_id='cdn_report_import',
    bash_command=cdn_report_import_template,
    dag=dag
)

cdn_report_template = '''
    cd %s/performance && python cdn_report.py --date {{ds_nodash}}
''' % local_config.WORKSPACE
cdn_report = BashOperator(
    task_id='cdn_report',
    bash_command=cdn_report_template,
    dag=dag
)
wait_usage_list.append(cdn_report)
cdn_report.set_downstream(cdn_report_import)


webview_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/performance --business_units gb
''' % (local_config.WORKSPACE, data_config.webview_report_name, local_config.WORKSPACE)
webview_report_import = BashOperator(
    task_id='webview_report_import',
    bash_command=webview_report_import_template,
    dag=dag
)

webview_report_template = '''
    cd %s/performance && python webview_report.py --date {{ds_nodash}}
''' % local_config.WORKSPACE
webview_report = BashOperator(
    task_id='webview_report',
    bash_command=webview_report_template,
    dag=dag
)
wait_usage_list.append(webview_report)
webview_report.set_downstream(webview_report_import)


method_lag_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/performance --business_units gb
''' % (local_config.WORKSPACE, data_config.method_lag_report_name, local_config.WORKSPACE)
method_lag_report_import = BashOperator(
    task_id='method_lag_report_import',
    bash_command=method_lag_report_import_template,
    dag=dag
)

method_lag_report_template = '''
    cd %s/performance && python method_lag_report.py {{ds_nodash}}
''' % local_config.WORKSPACE
method_lag_report = BashOperator(
    task_id='method_lag_report',
    bash_command=method_lag_report_template,
    dag=dag
)
wait_usage_list.append(method_lag_report)
wait_matrix_usage_list.append(method_lag_report)
method_lag_report.set_downstream(method_lag_report_import)

background_stat_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/performance --business_units gb
''' % (local_config.WORKSPACE, ' '.join(data_config.background_stat_report_name_list), local_config.WORKSPACE)
background_stat_report_import = BashOperator(
    task_id='background_stat_report_import',
    bash_command=background_stat_report_import_template,
    dag=dag
)

background_stat_report_template = '''
    cd %s/performance && python background_stat_report.py {{ds_nodash}} --mode hadoop
''' % local_config.WORKSPACE
background_stat_report = BashOperator(
    task_id='background_stat_report',
    bash_command=background_stat_report_template,
    dag=dag
)
wait_usage_list.append(background_stat_report)
wait_matrix_usage_list.append(background_stat_report)
background_stat_report.set_downstream(background_stat_report_import)

silent_install_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/oem_report --business_units gb
''' % (local_config.WORKSPACE, data_config.silent_install_report, local_config.WORKSPACE)
silent_install_report_import = BashOperator(
    task_id='silent_install_report_import',
    bash_command=silent_install_report_import_template,
    dag=dag
)

silent_install_report_template = '''
    cd %s/oem_report && python ./silent_install_report.py {{ds_nodash}} {{ds_nodash}}
''' % local_config.WORKSPACE
silent_install_report = BashOperator(
    task_id='silent_install_report',
    bash_command=silent_install_report_template,
    dag=dag
)
silent_install_report.set_downstream(silent_install_report_import)
wait_usage_list.append(silent_install_report)

store_pageview_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/store_report --business_units gb
''' % (local_config.WORKSPACE, data_config.store_pageview_report_name, local_config.WORKSPACE)
store_pageview_report_import = BashOperator(
    task_id='store_pageview_report_import',
    bash_command=store_pageview_report_import_template,
    dag=dag
)

store_pageview_report_template = '''
    cd %s/store_report && python ./store_pageview_report.py {{ds_nodash}} {{ds_nodash}}
''' % local_config.WORKSPACE
store_pageview_report = BashOperator(
    task_id='store_pageview_report',
    bash_command=store_pageview_report_template,
    dag=dag
)
wait_usage_list.append(store_pageview_report)
store_pageview_report.set_downstream(store_pageview_report_import)

store_tract_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/store_report
''' % (local_config.WORKSPACE, data_config.store_track_report_name, local_config.WORKSPACE)
store_tract_report_import = BashOperator(
    task_id='store_tract_report_import',
    bash_command=store_tract_report_import_template,
    dag=dag
)

store_tract_report_template = '''
    cd %s/store_report && python ./store_track_report.py {{ds_nodash}} {{ds_nodash}}
''' % local_config.WORKSPACE
store_tract_report = BashOperator(
    task_id='store_tract_report',
    bash_command=store_tract_report_template,
    dag=dag
)
store_tract_report.set_downstream(store_tract_report_import)
wait_usage_list.append(store_tract_report)

login_stat_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/usage_based_report
''' % (local_config.WORKSPACE, data_config.login_stat_report_name, local_config.WORKSPACE)
login_stat_report_import = BashOperator(
    task_id='login_stat_report_import',
    bash_command=login_stat_report_import_template,
    dag=dag
)

login_stat_report_template = '''
    cd %s/usage_based_report && python ./login_stat_report.py {{ds_nodash}} {{ds_nodash}}
''' % local_config.WORKSPACE
login_stat_report = BashOperator(
    task_id='login_stat_report',
    bash_command=login_stat_report_template,
    dag=dag
)
login_stat_report.set_downstream(login_stat_report_import)
wait_usage_list.append(login_stat_report)


referrer_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/store_report
''' % (local_config.WORKSPACE, data_config.referrer_report_name, local_config.WORKSPACE)
referrer_report_import = BashOperator(
    task_id='referrer_report_import',
    bash_command=referrer_report_import_template,
    dag=dag
)

referrer_report_template = '''
    cd %s/store_report && python ./referrer_report.py {{ds_nodash}} {{ds_nodash}}
''' % local_config.WORKSPACE
referrer_report = BashOperator(
    task_id='referrer_report',
    bash_command=referrer_report_template,
    dag=dag
)
wait_usage_list.append(referrer_report)
referrer_report.set_downstream(referrer_report_import)

page_duration_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/usage_based_report
''' % (local_config.WORKSPACE, ' '.join(data_config.page_duration_report_name_list), local_config.WORKSPACE)
page_duration_report_import = BashOperator(
    task_id='page_duration_report_import',
    bash_command=page_duration_report_import_template,
    dag=dag
)

page_duration_report_template = '''
    cd %s/usage_based_report && python page_duration_report.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE
page_duration_report = BashOperator(
    task_id='page_duration_report',
    bash_command = page_duration_report_template,
    dag=dag
)
wait_usage_list.append(page_duration_report)
wait_matrix_usage_list.append(page_duration_report)
wait_activate_source_list.append(page_duration_report)
page_duration_report.set_downstream(page_duration_report_import)

duration_funnel_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/usage_based_report
''' % (local_config.WORKSPACE, data_config.duration_funnel_report_name, local_config.WORKSPACE)
duration_funnel_report_import = BashOperator(
    task_id='duration_funnel_report_import',
    bash_command=duration_funnel_report_import_template,
    dag=dag
)

duration_funnel_report_template = '''
    cd %s/usage_based_report && python duration_funnel_report.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE
duration_funnel_report = BashOperator(
    task_id='duration_funnel_report',
    bash_command = duration_funnel_report_template,
    dag=dag
)
wait_usage_list.append(duration_funnel_report)
wait_matrix_usage_list.append(duration_funnel_report)
wait_activate_source_list.append(duration_funnel_report)
duration_funnel_report.set_downstream(duration_funnel_report_import)


# accumulate for 7 days data then backfill 7 days reports
page_funnel_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{macros.ds_format(macros.ds_add(ds, -6), "%%Y-%%m-%%d", "%%Y%%m%%d")}} -t {{ds_nodash}} --data_source %s --json_path %s/funnel
''' % (local_config.WORKSPACE, 'smartinput_page_funnel', local_config.WORKSPACE)
page_funnel_report_import = BashOperator(
    task_id='page_funnel_report_import',
    bash_command=page_funnel_report_import_template,
    dag=dag
)

page_funnel_report_template = '''
    cd %s/funnel && python page_funnel_report.py -f {{macros.ds_format(macros.ds_add(ds, -6), "%%Y-%%m-%%d", "%%Y%%m%%d")}} -t {{ds_nodash}}
''' % local_config.WORKSPACE
page_funnel_report = BashOperator(
    task_id='page_funnel_report',
    bash_command=page_funnel_report_template,
    dag=dag
)
page_funnel_report.set_downstream(page_funnel_report_import)

process_pageview_channel_template = '''
    cd %s/funnel && python process_usage_pageview.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % local_config.WORKSPACE
process_pageview_channel = BashOperator(
    task_id='process_pageview_channel',
    bash_command=process_pageview_channel_template,
    dag=dag
)
wait_usage_list.append(process_pageview_channel)
wait_matrix_usage_list.append(process_pageview_channel)
wait_activate_source_list.append(process_pageview_channel)
process_pageview_channel.set_downstream(page_funnel_report)


referrer_recognize_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/usage_based_report --business_units gb
''' % (local_config.WORKSPACE, data_config.referrer_recognize_report_name, local_config.WORKSPACE)
referrer_recognize_report_import = BashOperator(
    task_id='referrer_recognize_report_import',
    bash_command=referrer_recognize_report_import_template,
    dag=dag
)

referrer_recognize_report_template = '''
    cd %s/usage_based_report && python referrer_recognize_report.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE
referrer_recognize_report = BashOperator(
    task_id='referrer_recognize_report',
    bash_command = referrer_recognize_report_template,
    dag=dag
)
referrer_recognize_report.set_downstream(referrer_recognize_report_import)

referrer_recognize_user_template = '''
    cd %s/usage_based_report && python referrer_recognize_user.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE
referrer_recognize_user = BashOperator(
    task_id='referrer_recognize_user',
    bash_command = referrer_recognize_user_template,
    dag=dag
)
wait_usage_list.append(referrer_recognize_user)
wait_activate_source_list.append(referrer_recognize_user)
referrer_recognize_user.set_downstream(referrer_recognize_report)

hades_not_show_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/usage_based_report
''' % (local_config.WORKSPACE, data_config.hades_not_show_report_name, local_config.WORKSPACE)
hades_not_show_report_import = BashOperator(
    task_id='hades_not_show_report_import',
    bash_command=hades_not_show_report_import_template,
    dag=dag
)

hades_not_show_report_template = '''
    cd %s/usage_based_report && python hades_not_show_report.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE
hades_not_show_report = BashOperator(
    task_id='hades_not_show_report',
    bash_command = hades_not_show_report_template,
    dag=dag
)
wait_usage_list.append(hades_not_show_report)
wait_activate_source_list.append(hades_not_show_report)
hades_not_show_report.set_downstream(hades_not_show_report_import)

process_lifetime_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{yesterday_ds_nodash}} -t {{yesterday_ds_nodash}} --data_source %s --json_path %s/performance
''' % (local_config.WORKSPACE, data_config.process_lifetime_report_name, local_config.WORKSPACE)
process_lifetime_report_import = BashOperator(
    task_id='process_lifetime_report_import',
    bash_command=process_lifetime_report_import_template,
    dag=dag
)

process_lifetime_report_template = '''
    cd %s/performance && python process_lifetime_report.py {{yesterday_ds_nodash}} {{yesterday_ds_nodash}}
'''%local_config.WORKSPACE
process_lifetime_report = BashOperator(
    task_id='process_lifetime_report',
    bash_command = process_lifetime_report_template,
    dag=dag
)
wait_usage_list.append(process_lifetime_report)
wait_matrix_usage_list.append(process_lifetime_report)
process_lifetime_report.set_downstream(process_lifetime_report_import)

ar_emoji_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/usage_based_report
''' % (local_config.WORKSPACE, data_config.ar_emoji_name, local_config.WORKSPACE)
ar_emoji_report_import = BashOperator(
    task_id='ar_emoji_report_import',
    bash_command=ar_emoji_report_import_template,
    dag=dag
)

ar_emoji_report_template = '''
    cd %s/usage_based_report && python ./ar_emoji.py {{ds_nodash}} {{ds_nodash}}
''' % local_config.WORKSPACE
ar_emoji_report = BashOperator(
    task_id='ar_emoji_report',
    bash_command=ar_emoji_report_template,
    dag=dag
)
wait_usage_list.append(ar_emoji_report)
ar_emoji_report.set_downstream(ar_emoji_report_import)

## page_retention
page_retention_channel_template = '''
    cd %s/usage_based_report && python page_retention_channel.py --dates {{ds_nodash}} -p %s
'''

retention_report_dates = ' '.join(['{{macros.ds_format(macros.ds_add(ds, -%d), "%%Y-%%m-%%d", "%%Y%%m%%d")}}' % (d - 1) for d in data_config.PAGE_RETENTION_DICT["date"]])
page_retention_report_template = '''
    cd %s/usage_based_report && python page_retention_report.py --dates %s
''' % (local_config.WORKSPACE, retention_report_dates)
page_retention_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py --dates %s --data_source %s --json_path %s/usage_based_report
''' % (local_config.WORKSPACE, retention_report_dates, 'smartinput_page_retention', local_config.WORKSPACE)

page_retention_report = BashOperator(
    task_id='page_retention_report',
    bash_command = page_retention_report_template,
    dag=dag
)
page_retention_report_import = BashOperator(
    task_id='page_retention_report_import',
    bash_command=page_retention_report_import_template,
    dag=dag
)
page_retention_report.set_downstream(page_retention_report_import)

for p in data_config.PRODUCT_SECTIONS:
    page_retention_channel = BashOperator(
        task_id='page_retention_channel_%s' % p,
        bash_command=page_retention_channel_template % (local_config.WORKSPACE, p),
        dag=dag
    )
    wait_usage_list.append(page_retention_channel)
    wait_matrix_usage_list.append(page_retention_channel)
    wait_activate_source_list.append(page_retention_channel)
    page_retention_channel.set_downstream(page_retention_report)

join = DummyOperator(task_id='join', trigger_rule='all_success', dag=dag)
join.set_downstream(wait_usage_list)

# ads_cover_rate
ads_cover_report_template = '''
    cd %s/performance && python ads_cover.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % local_config.WORKSPACE
ads_cover_report = BashOperator(
    task_id='ads_cover_report',
    bash_command=ads_cover_report_template,
    dag=dag,
    priority_weight=20
)
wait_matrix_usage_list.append(ads_cover_report)

ads_cover_source = ['smartinput_ads_reach_time', 'smartinput_ads_cover_rate', 'smartinput_ads_exception_analyse']
for source in ads_cover_source:
    ads_cover_import_template = '''
        cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/performance
''' % (local_config.WORKSPACE, source, local_config.WORKSPACE)
    ads_cover_import = BashOperator(
        task_id='%s_import' % source,
        bash_command=ads_cover_import_template,
        dag=dag
    )
    ads_cover_import.set_upstream(ads_cover_report)


gdpr_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/usage_based_report
''' % (local_config.WORKSPACE, data_config.gdpr_report_name, local_config.WORKSPACE)
gdpr_report_import = BashOperator(
    task_id='gdpr_report_import',
    bash_command=gdpr_report_import_template,
    dag=dag
)

gdpr_report_template = '''
    cd %s/usage_based_report && python ./gdpr_report.py {{ds_nodash}} {{ds_nodash}}
''' % local_config.WORKSPACE
gdpr_report = BashOperator(
    task_id='gdpr_report',
    bash_command=gdpr_report_template,
    dag=dag
)
wait_matrix_usage_list.append(gdpr_report)
gdpr_report.set_downstream(gdpr_report_import)

matrix_join = ExternalTaskSensor(
    dag=dag,
    task_id='wait_usage_matrix',
    timeout=10 * 3600,
    external_dag_id='usage_matrix',
    pool='wait_usage_pool',
    external_task_id='usage_matrix_all_success',
)
matrix_join.set_downstream(wait_matrix_usage_list)

activate_source_data_ready = DummyOperator(task_id='activate_source_data_ready', dag=dag)

now = airflow_utils.FuncExternalTaskSensor(
    dag=dag,
    task_id='wait_activate_source_summary',
    timeout=airflow_utils.wait_activate_source_timeout*3600,
    external_dag_id = 'activate_source_summary',
    external_task_id = 'activate_source_summary',
    func = airflow_utils.get_last_friday_schedule
)
now.set_downstream(activate_source_data_ready)
activate_source_data_ready.set_downstream(wait_activate_source_list)

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

# account_stat_report
account_stat_report_template = '''
    cd %s/account_info && python account_stat_report.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE
account_stat_report = BashOperator(
    task_id='account_stat_report',
    bash_command = account_stat_report_template,
    dag=dag
)

account_stat_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/account_info
''' % (local_config.WORKSPACE, 'smartinput_account_stat', local_config.WORKSPACE)
account_stat_report_import = BashOperator(
    task_id='account_stat_report_import',
    bash_command=account_stat_report_import_template,
    dag=dag
)
account_stat_report_import.set_upstream(account_stat_report)

# account_channel
account_channel_template = '''
    cd %s/account_info && python account_channel.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE
account_channel = BashOperator(
    task_id='account_channel',
    bash_command = account_channel_template,
    dag=dag
)

activate_source_data_ready.set_downstream(account_channel)

# wait_active_channel kdau + tdau
wait_active_channel = ExternalTaskSensor(
    task_id='wait_active_channel',
    timeout=airflow_utils.wait_active_channel_timeout*3600,
    external_dag_id = 'launch_active_channel',
    external_task_id = 'active_channel_success',
    execution_delta = timedelta(0),
    dag=dag)

# account_stat_report dependencies
account_channel.set_downstream(account_stat_report)
wait_active_channel.set_downstream(account_stat_report)

ime_no_token_template = '''
    cd %s/ime_no_token && python ime_no_token.py {{ds_nodash}}
'''%local_config.WORKSPACE
ime_no_token = BashOperator(
    task_id='ime_no_token',
    bash_command = ime_no_token_template,
    dag=dag
)

# wait revenue dag finish
wait_revenue_ready = ExternalTaskSensor(
    task_id='wait_revenue_ready',
    timeout=airflow_utils.wait_revenue_timeout*3600,
    external_dag_id='revenue',
    external_task_id='process_revenue',
    execution_delta=timedelta(0),
    dag=dag)


ime_game_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/ime_game --keep_data
''' % (local_config.WORKSPACE, data_config.game_report_name, local_config.WORKSPACE)
ime_game_report_import = BashOperator(
    task_id='ime_game_report_import',
    bash_command=ime_game_report_import_template,
    dag=dag
)

ime_game_report_template = '''
    cd %s/ime_game && python ime_game_report.py {{ds_nodash}} {{ds_nodash}}
''' % local_config.WORKSPACE
ime_game_report = BashOperator(
    task_id='ime_game_report',
    bash_command=ime_game_report_template,
    dag=dag
)
ime_game_report.set_upstream(wait_revenue_ready)
ime_game_report.set_downstream(ime_game_report_import)

funnel_sql_report_data_ready = DummyOperator(task_id='funnel_sql_report_data_ready', dag=dag)

wait_activate_source = ExternalTaskSensor(
    dag=dag,
    task_id='wait_activate_source',
    timeout=airflow_utils.wait_activate_source_timeout * 3600,
    external_dag_id='activate_source',
    external_task_id='activate_source',
    execution_delta=timedelta(days=0),
    depends_on_past=True,
)
funnel_sql_report_data_ready.set_upstream(wait_activate_source)

funnel_sql_report_template = '''
    cd %s/usage_based_report && python funnel_sql_report.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE
funnel_sql_report = BashOperator(
    task_id='funnel_sql_report',
    bash_command=funnel_sql_report_template,
    dag=dag
)
funnel_sql_report.set_upstream(funnel_sql_report_data_ready)

funnel_sql_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/usage_based_report
''' % (local_config.WORKSPACE, 'smartinput_sql_funnel', local_config.WORKSPACE)
funnel_sql_report_import = BashOperator(
    task_id='funnel_sql_report_import',
    bash_command=funnel_sql_report_import_template,
    dag=dag
)
funnel_sql_report_import.set_upstream(funnel_sql_report)

commercial_coverage_report_data_ready = DummyOperator(task_id='commercial_coverage_report_data_ready', dag=dag)
commercial_coverage_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/usage_based_report
''' % (local_config.WORKSPACE, data_config.commercial_coverage_report_name, local_config.WORKSPACE)
commercial_coverage_report_import = BashOperator(
    task_id='commercial_coverage_report_import',
    bash_command=commercial_coverage_report_import_template,
    dag=dag
)

commercial_coverage_report_template = '''
    cd %s/usage_based_report && python ./commercial_coverage_report.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % local_config.WORKSPACE
commercial_coverage_report = BashOperator(
    task_id='commercial_coverage_report',
    bash_command=commercial_coverage_report_template,
    dag=dag
)
commercial_coverage_report_data_ready.set_upstream([wait_active_channel, wait_activate_source])
commercial_coverage_report.set_upstream(commercial_coverage_report_data_ready)
commercial_coverage_report.set_downstream(commercial_coverage_report_import)


for reg in ['us', 'eu', 'ap']:
    now = airflow_utils.FlagsSensor(
        dag=dag,
        task_id='dw_usage_flag_%s' % reg,
        timeout=airflow_utils.wait_usage_flag_timeout * 3600,
        region=reg,
        pool='wait_usage_pool',
        data_type='usage'
    )
    now.set_downstream(join)

    now = airflow_utils.FlagsSensor(
        dag = dag,
        task_id='dw_usage_high_flag_%s' % reg,
        timeout=airflow_utils.wait_usage_flag_timeout * 3600,
        region=reg,
        pool='wait_usage_pool',
        data_type='usage_high'
    )
    now.set_downstream(join)

    now = airflow_utils.FlagsSensor(
        dag=dag,
        task_id='dw_activate_flag_%s' % reg,
        timeout=airflow_utils.wait_activate_flag_timeout * 3600,
        region=reg,
        data_type='activate_smartinput',
        pool='wait_usage_pool',
    )
    now.set_downstream([process_lifetime_report, funnel_sql_report_data_ready])

    now = airflow_utils.FlagsSensor(
        dag=dag,
        task_id='wait_skin_plugin_flag_%s' % reg,
        timeout=airflow_utils.wait_skin_plugin_flag_timeout * 3600,
        region=reg,
        data_type='skin_plugin',
        pool='wait_usage_pool',
    )
    now.set_downstream(funnel_sql_report_data_ready)
    now.set_downstream(join)

    now = airflow_utils.FlagsSensor(
        dag=dag,
        task_id='dw_active_flag_%s' % reg,
        timeout=airflow_utils.wait_active_flag_timeout * 3600,
        region=reg,
        data_type='active_smartinput',
        pool='wait_usage_pool'
    )
    now.set_downstream(process_lifetime_report)

    now = airflow_utils.FlagsSensor(
        dag=dag,
        task_id='dw_usage_smartinput_ios_keyboard_flag_%s' % reg,
        timeout=airflow_utils.wait_smartinput_ios_timeout * 3600,
        region=reg,
        data_type='usage_smartinput_ios_keyboard',
        pool='wait_usage_based_deps_pool',
    )
    now.set_downstream(ar_emoji_report)

    now = airflow_utils.FlagsSensor(
        dag=dag,
        task_id='dw_usage_smartinput_ios_container_flag_%s' % reg,
        timeout=airflow_utils.wait_smartinput_ios_timeout * 3600,
        region=reg,
        data_type='usage_smartinput_ios_container',
        pool='wait_usage_based_deps_pool',
    )
    now.set_downstream(ar_emoji_report)

    for product, downstream_task in {'ime_games_statistics': ime_game_report, 'ime_no_token': ime_no_token, 'ime_record_account_info': account_channel}.items():
        data_path = data_config.USAGE_DATA('{{ds_nodash}}', product, reg, '{{macros.ds_format(ds, "%Y-%m-%d", "%Y")}}')
        wait_data = airflow_utils.FileSensor(
            dag=dag,
            task_id='wait_%s_%s' % (reg, product),
            timeout=10*3600,
            file_path=data_path + '/_SUCCESS',
            pool='wait_usage_based_deps_pool',
            priority_weight=-2
        )
        wait_data.set_downstream(downstream_task)


wait_cluster = TimeDeltaSensor(
    dag=dag,
    task_id='wait_cluster',
    delta=timedelta(hours=11)
)
wait_cluster.set_downstream([join, funnel_sql_report_data_ready, matrix_join])


if __name__ == "__main__":
    dag.cli()
