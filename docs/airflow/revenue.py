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
    'owner': 'tianfeng.jiang',
    'start_date': datetime(2017, 6, 18),
    'retries':1,
    'email': data_config.AIRFLOW_USERS
}

dag = DAG(
    dag_id='revenue',
    default_args=args,
    schedule_interval = timedelta(days=1),
    dagrun_timeout=timedelta(hours=airflow_utils.dag_timeout)
)

copy_ad_detail_template = '''
    hadoop fs -rm -r -f /user/gbdata/share/davinci/{{ds_nodash}} && hadoop fs -cp  -f /user/gbdata/data/ad_detail/smartinput/json/{{ds_nodash}} /user/gbdata/share/davinci/
'''
copy_ad_detail = BashOperator(
    task_id='copy_ad_detail',
    bash_command = copy_ad_detail_template,
    dag=dag,
    priority_weight=20
)
set_all_davinci_all_false = airflow_utils.create_set_flag_task(dag, 'all_davinci_all', 'False', downstream=copy_ad_detail)
set_all_davinci_all_true = airflow_utils.create_set_flag_task(dag, 'all_davinci_all', 'True', upstream=copy_ad_detail)

process_revenue_template = '''
    cd %s/revenue && python process_revenue.py --date "{{ds_nodash}}"
'''%local_config.WORKSPACE
process_revenue = BashOperator(
    task_id='process_revenue',
    bash_command = process_revenue_template,
    dag=dag,
    priority_weight=20
)
process_revenue.set_downstream(set_all_davinci_all_false)


def monitor_revenue_task_func(**kwargs):
    import subprocess
    import time
    # check stats of the task process_revenue
    # exit if SUCCESS
    # else clear all failed revenue task and retry in 1 hour
    check_process_revenue = 'airflow task_failed_deps revenue process_revenue %s' % (kwargs['ds'])
    success_mark_sentence = "Dagrun Running: Task instance's dagrun was not in the 'running' state but in the state 'success'."
    deps_all_met_sentence = "Task instance dependencies are all met."
    clear_revenue_task = 'airflow clear -t "^revenue_" -s %s -e %s -d -c -f revenue && airflow clear -t "^process_revenue$" -s %s -e %s -d -c revenue' % (kwargs['ds'], kwargs['ds'], kwargs['ds'], kwargs['ds'])
    # retry 10 times from 21h to 6h
    for t in range(10):
        time.sleep(3600)
        print('execute %s' % check_process_revenue)
        ret = subprocess.check_output(check_process_revenue, shell=True).decode("utf-8")
        print(ret)
        if (success_mark_sentence in ret) or (deps_all_met_sentence in ret):
            return True
        else:
            print('execute %s' % clear_revenue_task)
            print(subprocess.check_output(clear_revenue_task, shell=True))
    raise AirflowException('revenue retry failed')


monitor_revenue_task = PythonOperator(
    dag=dag,
    task_id='monitor_revenue_task',
    python_callable=monitor_revenue_task_func,
    provide_context=True
)
# monitor_revenue_task.set_upstream(wait_revenue_ready)

reuse_raw = "--reuseraw"
default_wait_time = 12
platforms = {
    'duad': {'use_raw': '', 'wait_delta': default_wait_time},
    'yahoo': {'use_raw': '', 'wait_delta': default_wait_time},
    'facebook': {'use_raw': reuse_raw, 'wait_delta': default_wait_time},
    'google': {'use_raw': '', 'wait_delta': default_wait_time},
    'yeahmobi': {'use_raw': '', 'wait_delta': default_wait_time},
    #'pubnative': {'use_raw': '', 'wait_delta': default_wait_time}, stopped on 20181126, platform do not corporate any more
    #'avocarrot': {'use_raw': '', 'wait_delta': default_wait_time}, stopped on 20181206, platform do not corporate any more
    #'inner_active': {'use_raw': '', 'wait_delta': default_wait_time},stopped on 20181206, platform do not corporate any more
    #    'smaato':'', stopped on 20180809, jstag closed
    'mopub': {'use_raw': '', 'wait_delta': default_wait_time},
    'amazon': {'use_raw': '', 'wait_delta': default_wait_time},
    'amazon_shopping': {'use_raw': reuse_raw, 'wait_delta': default_wait_time},
    #    'mobfox':reuse_raw, stopped on 20180809, jstag closed
    #    'cheetah_dashboard':'',
    #    'aol':reuse_raw, stopped on 20180809, jstag closed
    #'mobvista': {'use_raw': reuse_raw, 'wait_delta': default_wait_time}, stopped on 20181206, platform do not corporate any more
    'adx': {'use_raw': reuse_raw, 'wait_delta': default_wait_time},
    #'gmobi': {'use_raw': reuse_raw, 'wait_delta': default_wait_time}, stopped on 20181206, platform do not corporate any more
    #    'inmobi':reuse_raw, stopped on 20171014
    #    'yahoosearch':'',  # login failed temporarily removed on 20180717
    #    'mobitech':reuse_raw, stopped on 20170826
    'applovin': {'use_raw': reuse_raw, 'wait_delta': default_wait_time},
    'vungle': {'use_raw': reuse_raw, 'wait_delta': default_wait_time},
    #    'conversant':reuse_raw, stopped on 20180809, jstag closed
    #    'openx':reuse_raw,  # stopped on 20180731, switch from jstag to sniper
    'unity': {'use_raw': reuse_raw, 'wait_delta': default_wait_time},
    #    'rubicon':reuse_raw, stopped on 20180809, jstag closed
    'sniper': {'use_raw': reuse_raw, 'wait_delta': default_wait_time + 1},
    'instreamatic': {'use_raw': reuse_raw, 'wait_delta': default_wait_time},
}

revenue_task_list = []
for key, value in platforms.items():
    # revenue using pst time
    wait_revenue_ready = TimeDeltaSensor(
        dag=dag,
        task_id='wait_%s_revenue_ready' % key,
        delta=timedelta(hours=value['wait_delta']),
        pool='wait_revenue_pool',
    )
    revenue_template = '''
        cd %s/revenue && python daily_revenue.py --date "{{ds_nodash}}" %s %s
    '''% (local_config.WORKSPACE, key, value['use_raw'])
    revenue = BashOperator(
        task_id='revenue_%s' % key,
        bash_command = revenue_template,
        dag=dag
    )
    revenue_task_list.append(revenue)
    wait_revenue_ready.set_downstream(monitor_revenue_task)
    revenue.set_upstream(wait_revenue_ready)
    revenue.set_downstream(process_revenue)

wait_share_talia_ads = TimeDeltaSensor(
        dag=dag,
        task_id='wait_talia_ads_revenue_ready',
        delta=timedelta(hours=3),
        pool='wait_revenue_pool',
        priority_weight=90
)
talia_ads_revenue_template = '''
        cd %s/revenue && python pull_revenue_for_talia.py --date "{{ds_nodash}}" 
''' % local_config.WORKSPACE
talia_ads_revenue = BashOperator(
        task_id='revenue_talia_ads',
        bash_command=talia_ads_revenue_template,
        dag=dag
    )
talia_ads_revenue.set_upstream(wait_share_talia_ads)


backfill_flurry_template = '''
    airflow clear -t "^revenue_mopub$" -s {{ yesterday_ds }} -e {{ yesterday_ds }} -d -c revenue &&
    airflow clear -t "^revenue_yahoo$" -s {{ yesterday_ds }} -e {{ yesterday_ds }} -d -c revenue &&
    airflow clear -t "^revenue_sniper$" -s {{ yesterday_ds }} -e {{ yesterday_ds }} -d -c revenue &&
    airflow clear -t "^revenue_google$" -s {{ yesterday_ds }} -e {{ yesterday_ds }} -d -c revenue &&
    airflow clear -t "^revenue_facebook$" -s {{ yesterday_ds }} -e {{ yesterday_ds }} -d -c revenue &&
    airflow clear -t "^revenue_amazon_shopping" -s {{ yesterday_ds }} -e {{ yesterday_ds }} -d -c revenue &&
    airflow clear -t "^wait_revenue_ready$" -s {{ yesterday_ds }} -e {{ yesterday_ds }} -d -c roi &&
    airflow clear -t "^wait_revenue_ready$" -s {{ yesterday_ds }} -e {{ yesterday_ds }} -d -c usage_based_report &&
    airflow clear -t "^wait_revenue_ready$" -s {{ yesterday_ds }} -e {{ yesterday_ds }} -d -c swiftcall_data_flow &&
    airflow clear -t "^wait_ad_user$" -s {{ yesterday_ds }} -e {{ yesterday_ds }} -d -c commercial_report &&
    airflow clear -t "^wait_ad_user$" -s {{ yesterday_ds }} -e {{ yesterday_ds }} -d -c dau_stat_report &&
    airflow clear -t "^wait_ad_user$" -s {{ yesterday_ds }} -e {{ yesterday_ds }} -d -c abtest_ltv_report
'''
backfill_flurry = BashOperator(
    task_id='backfill_flurry',
    bash_command=backfill_flurry_template,
    dag=dag,
    priority_weight=10,
)

pre_process_revenue_template = '''
    cd %s/revenue && python process_revenue.py --date "{{ds_nodash}}"
'''%local_config.WORKSPACE
pre_process_revenue = BashOperator(
    task_id='pre_process_revenue',
    bash_command=pre_process_revenue_template,
    dag=dag,
    priority_weight=20
)
pre_process_revenue.set_downstream(revenue_task_list)

pre_revenue_platforms={
    'google': {'use_raw': '', 'wait_delta': 2},
}
for key, value in pre_revenue_platforms.items():
    # revenue using pst time
    wait_revenue_ready = TimeDeltaSensor(
        dag=dag,
        task_id='wait_%s_pre_revenue_ready' % key,
        delta=timedelta(hours=value['wait_delta']),
        pool='wait_revenue_pool',
        priority_weight=100
    )
    revenue_template = '''
        cd %s/revenue && python daily_revenue.py --date "{{ds_nodash}}" %s %s
    ''' % (local_config.WORKSPACE, key, value['use_raw'])
    revenue = BashOperator(
        task_id='pre_revenue_%s' % key,
        bash_command=revenue_template,
        dag=dag
    )
    revenue.set_upstream(wait_revenue_ready)
    revenue.set_downstream(pre_process_revenue)
