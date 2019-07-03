from builtins import range
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.operators.sensors import S3KeySensor, TimeDeltaSensor, SqlSensor
from airflow.models import DAG
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import os
import logging
import data_config
import local_config
import airflow_utils

args = {
    'owner': 'libin.yang',
    'start_date': datetime(2017, 2, 23),
    'retries':1,
    'email': data_config.AIRFLOW_USERS,
}

#manually change schedule_interval timedelta(days=1), generate dag run with 1 day interval
dag = DAG(
    dag_id='ime_data_flow_v0',
    default_args=args,
    schedule_interval = None,
    dagrun_timeout=timedelta(hours=airflow_utils.dag_timeout),
    concurrency=4,
    max_active_runs=1,
)
##########activate_source##########
activate_source_template = '''
    cd "{{params.dir}}"/activate_source && python ./emr_activate_source.py -f "{{ds_nodash}}" -t "{{ds_nodash}}"
'''
activate_source = BashOperator(
    task_id='activate_source',
    bash_command=activate_source_template,
    params={'dir':local_config.WORKSPACE},
    depends_on_past=True,
    dag=dag
    )

##########activate_source_summary##########

def generate_activate_source(**kwargs):
    logging.info('execution day is %s' % kwargs['ds_nodash'])
    adate = datetime.strptime(kwargs['ds_nodash'], '%Y%m%d')
    #generate summary for Thursday
    if adate.isoweekday() == 4:
        return 'generate_summary'
    else:
        return 'skip_summary'

activate_source_summary_template = '''cd %s/activate_source''' % local_config.WORKSPACE + ''' && if [ "$(date +"%u" -d {{ ds_nodash }})" -eq 4 ]; then python activate_source_summary.py -f {{ ds_nodash }} -t {{ ds_nodash }}; else echo "no need to generate summary for {{ ds_nodash }}"; fi'''
activate_source_summary = BashOperator(
    task_id='activate_source_summary',
    bash_command=activate_source_summary_template,
    depends_on_past=True,
    dag=dag
    )

activate_source_summary.set_upstream(activate_source)

##########activate_channel##########
activate_channel_template = '''cd %s/activate_summary''' % local_config.WORKSPACE + ''' && if [ "$(date +"%u" -d {{ ds_nodash }})" -eq 4 ]; then python activate_channel.py --date {{ ds_nodash }}; else echo "no need to generate summary for {{ ds_nodash }}"; fi'''
activate_channel = BashOperator(
    dag = dag,
    task_id='activate_channel',
    bash_command = activate_channel_template
    )

activate_channel.set_upstream(activate_source)
##########active_channel##########
active_channel_template = '''cd %s/active_channel && python ./active_channel.py -f {{ds_nodash}} -t {{ds_nodash}} --active_type %s'''
active_channel = BashOperator(
    task_id='active_channel',
    bash_command = active_channel_template % (local_config.WORKSPACE, 'kdau'),
    depends_on_past=True,
    dag=dag
)


active_channel.set_upstream(activate_source_summary)

##########background_active_channel##########
background_active_channel = BashOperator(
    task_id='background_active_channel',
    bash_command = active_channel_template % (local_config.WORKSPACE, 'tdau'),
    depends_on_past=True,
    dag=dag
)

background_active_channel.set_upstream(activate_source_summary)

##########launch_retention_source##########
retention_source_report_template = '''
    cd %s/druid_report && python ./retention_source_report.py {{ds_nodash}}
'''%local_config.WORKSPACE
retention_source_report = BashOperator(
    task_id='retention_source_report',
    bash_command = retention_source_report_template,
    depends_on_past=True,
    dag=dag
)

daily_retention_report_template = '''
    cd %s/druid_report && python ./daily_retention_report.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE
daily_retention_report = BashOperator(
    task_id='daily_retention_report',
    bash_command = daily_retention_report_template,
    depends_on_past=True,
    dag=dag
)

user_retention_template = '''
    cd %s/daily_retention && python ./user_retention.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE

user_retention = BashOperator(
    task_id='launch_retention_source',
    bash_command = user_retention_template,
    depends_on_past=True,
    dag=dag
)
user_retention.set_downstream(daily_retention_report)
user_retention.set_downstream(retention_source_report)
user_retention.set_upstream(activate_source_summary)

##########usage_channel##########
def usage_channel_func(**kwargs):
    cmd = 'cd %s/usage_channel && python ./usage_channel.py %s %s'%(local_config.WORKSPACE, kwargs['ds_nodash'], (airflow_utils.get_last_friday_schedule(kwargs['execution_date']) + timedelta(days=6)).strftime('%Y%m%d'))
    ret = os.system(cmd)
    if ret != 0:
        raise AirflowException('cmd: %s' % cmd)

usage_channel = PythonOperator(
    dag = dag,
    task_id = 'usage_channel',
    python_callable = usage_channel_func,
    depends_on_past = True,
    provide_context = True
)

usage_channel.set_upstream(activate_source_summary)
##########roi##########

real_roi_template = '''
    cd %s/roi_channel && python real_roi_report.py -f {{ds_nodash}} -t {{ds_nodash}}
'''%local_config.WORKSPACE
real_roi = BashOperator(
    task_id='real_roi',
    bash_command = real_roi_template,
    dag=dag
)

ad_user_template = '''
    cd %s/davinci && python ad_user.py -f {{ds_nodash}} -t {{ds_nodash}}
'''%local_config.WORKSPACE
ad_user = BashOperator(
    task_id='ad_user',
    bash_command = ad_user_template,
    dag=dag
)
ad_user.set_downstream(real_roi)

roi_channel_template = '''
    cd %s/roi_channel && python roi_channel.py -f {{yesterday_ds_nodash}} -t {{yesterday_ds_nodash}} -d 14 --delay 1
'''%local_config.WORKSPACE
roi_channel = BashOperator(
    task_id='roi_channel',
    bash_command = roi_channel_template,
    depends_on_past=True,
    dag=dag
)

fast_roi_apply_template = '''
    cd %s/roi_channel && python fast_roi.py {{yesterday_ds_nodash}} apply
''' % local_config.WORKSPACE
fast_roi_apply = BashOperator(
    task_id='fast_roi_apply',
    bash_command=fast_roi_apply_template,
    dag=dag
)

fast_roi_build_template = '''
    cd %s/roi_channel && python fast_roi.py {{yesterday_ds_nodash}} build -d 14 -w 5
''' % local_config.WORKSPACE
fast_roi_build = BashOperator(
    task_id='fast_roi_build',
    bash_command=fast_roi_build_template,
    depends_on_past=True,
    dag=dag
)
fast_roi_build.set_downstream(fast_roi_apply)

roi_weekly_template = '''
    cd %s/roi_channel && python roi_weekly.py -f {{yesterday_ds_nodash}} -t {{yesterday_ds_nodash}} -d 7 --delay 1 -w 6
''' % local_config.WORKSPACE
roi_weekly = BashOperator(
    task_id='roi_weekly',
    bash_command=roi_weekly_template,
    dag=dag
)

roi_data_ready = DummyOperator(task_id='roi_data_ready', dag=dag)
roi_data_ready.set_upstream([ad_user, active_channel, user_retention])
roi_data_ready.set_downstream([roi_channel, roi_weekly, fast_roi_build])

druid_roi_report_template = '''
    cd %s/roi_channel && python roi_channel_report.py {{yesterday_ds_nodash}} {{yesterday_ds_nodash}}
'''%local_config.WORKSPACE
druid_roi_report = BashOperator(
    task_id='druid_roi_report',
    bash_command = druid_roi_report_template,
    dag=dag
)
druid_roi_report.set_upstream(roi_channel)

roi_track_template = '''
    cd %s/roi_channel && python roi_track.py -f {{yesterday_ds_nodash}} -t {{yesterday_ds_nodash}} -d 14
'''%local_config.WORKSPACE
roi_track = BashOperator(
    task_id='roi_track',
    bash_command = roi_track_template,
    dag=dag
)
roi_track.set_upstream(roi_channel)

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
ads_channel.set_upstream(activate_source_summary)

#add activate source info to impression data
ads_channel_impression_template = '''
    cd %s/davinci && python ./ads_channel_impression.py -f {{ds_nodash}} -t {{ds_nodash}}
'''%local_config.WORKSPACE

ads_channel_impression = BashOperator(
    dag = dag,
    task_id = 'ads_channel_impression',
    depends_on_past=True,
    bash_command = ads_channel_impression_template
)
ads_channel_impression.set_upstream(activate_source_summary)

#add activate source info to goblin_channel data
goblin_channel_template = '''
    cd %s/davinci && python ./goblin_channel.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE

goblin_channel = BashOperator(
    dag = dag,
    task_id = 'goblin_channel',
    depends_on_past=True,
    bash_command = goblin_channel_template
)
goblin_channel.set_downstream(ad_user)
goblin_channel.set_upstream(activate_source_summary)

##########commercial_report##########
druid_report_template = '''
    cd %s/druid_report && python commercial_report.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE
druid_report = BashOperator(
    task_id='druid_report',
    bash_command = druid_report_template,
    dag=dag
)

druid_report.set_upstream(usage_channel)
druid_report.set_upstream(ad_user)

## activate report
activate_report_template = '''
    cd %s/druid_report && python activate_report.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE
activate_report = BashOperator(
    task_id='activate_report',
    bash_command = activate_report_template,
    dag=dag
)
activate_report.set_upstream(activate_source_summary)

## fdau_channel
fdau_channel_template = '''
    cd %s/dau_stat_report && python fdau_channel.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE
fdau_channel = BashOperator(
    task_id='fdau_channel',
    bash_command = fdau_channel_template,
    depends_on_past=True,
    dag=dag
)
fdau_channel.set_upstream(active_channel)

## dau stat report
dau_stat_report_template = '''
    cd %s/dau_stat_report && python dau_stat_report.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE
dau_stat_report = BashOperator(
    task_id='dau_stat_report',
    bash_command = dau_stat_report_template,
    dag=dag
)
dau_stat_report.set_upstream(ad_user)
dau_stat_report.set_upstream(fdau_channel)
dau_stat_report.set_upstream(active_channel)
dau_stat_report.set_upstream(background_active_channel)

## fdau_stat_report
fdau_stat_report_template = '''
    cd %s/dau_stat_report && python fdau_stat_report.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE
fdau_stat_report = BashOperator(
    task_id='fdau_stat_report',
    bash_command = fdau_stat_report_template,
    dag=dag
)
fdau_stat_report.set_upstream(fdau_channel)
fdau_stat_report.set_upstream(ad_user)

## abtest_ltv_report
abtest_ltv_report_template = '''
    cd %s/abtest_report && python ./abtest_ltv_report.py {{ds_nodash}} {{ds_nodash}} 14
'''%local_config.WORKSPACE
abtest_ltv_report = BashOperator(
    task_id='abtest_ltv_report',
    bash_command = abtest_ltv_report_template,
    dag=dag
)
abtest_ltv_report.set_upstream(ad_user)
abtest_ltv_report.set_upstream(user_retention)

## uc_activate report
act_variance_template = '''
    cd %s/user_acquire && python ua_act_variance.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE
act_variance = BashOperator(
    task_id='act_variance_report',
    bash_command = act_variance_template,
    depends_on_past = True,
    dag=dag
)
act_variance.set_upstream(activate_source_summary)

swiftcall_user_activity_template = '''
    cd %s/swiftcall && python swiftcall_activity_report.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % local_config.WORKSPACE
swiftcall_user_activity = BashOperator(
    task_id='swiftcall_user_activity',
    bash_command=swiftcall_user_activity_template,
    dag=dag
)
swiftcall_user_activity.set_upstream(activate_source)

if __name__ == "__main__":
    dag.cli()
