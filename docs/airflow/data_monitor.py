from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.sensors import ExternalTaskSensor, TimeDeltaSensor
from airflow.models import DAG
from datetime import datetime, timedelta
import os
import data_config
import local_config
import airflow_utils
import logging
args = {
    'owner': 'libin.yang',
    'start_date': datetime(2017, 8, 31),
    'retries': 0,
    'email': data_config.AIRFLOW_USERS
}

dag = DAG(
    dag_id='data_monitor',
    default_args=args,
    schedule_interval=timedelta(days=1),
    dagrun_timeout=timedelta(hours=airflow_utils.dag_timeout))

full_reg_set = '{ap,eu,us}'
full_reg_cn_set = '{ap,eu,us,cn}'
reg_list = ['ap', 'eu', 'us']
data_monitor_delta = 7

# only observe important usage path
usage_path = [
    'MSG__COMMERCIAL_REFERRER_',
]

skin_path = [
    'MSG_RESUME_APPLY',
    'MSG_AD_SHOWN',
]

monitor_list = {
    'DW_ACTIVATE': os.path.join(data_config.PATH_DICT['gb'].DW_ACTIVATE, 'parquet/%(reg_cn)s/{{ds_nodash}}'),
    'DW_ACTIVATE_SUMMARY': os.path.join(data_config.DW_ACTIVATE_SUMMARY, 'parquet/%(reg_cn)s/{{yesterday_ds_nodash}}'),
    'DW_ACTIVE': os.path.join(data_config.PATH_DICT['gb'].DW_ACTIVE, 'parquet/%(reg_cn)s/{{ds_nodash}}'),
    'DW_LAUNCH_ACTIVE': os.path.join(data_config.PATH_DICT['gb'].DW_LAUNCH_ACTIVE, 'parquet/%(reg_cn)s/{{ds_nodash}}'),
    'DW_AD_SSPSTAT': os.path.join(data_config.AD_SSPSTAT, '{{ds_nodash}}/{{ds_nodash}}_ad_sspstat/%(reg_cn)s'),
}


GAIA_DATA_LIST = {
    'ime_games_statistics': ['us'],
    'usage_swiftcall': ['cn'],
    'usage_swiftcall_iOS': ['cn'],
}

for product in data_config.PATH_DICT['gb'].MATRIX_PRODUCT_LIST + ['integrated_abtest', 'ime_no_token']:
    GAIA_DATA_LIST[product] = reg_list


monitor_list.update({'DW_USAGE': os.path.join(data_config.DW_USAGE, 'smartinput/{%s}/parquet/{us,eu,ap}/{{ds_nodash}}' % ','.join(usage_path))})
monitor_list.update({'DW_SKIN_PLUGIN': os.path.join(data_config.DW_SKIN_PLUGIN, '{%s}/parquet/{us,eu,ap}/{{ds_nodash}}' % ','.join(skin_path))})

data_monitor_list = []


def gen_data_path_list(name, delta=data_monitor_delta):
    path_list = []
    for d in range(delta):
        path = monitor_list[name] % {'reg': full_reg_set, 'reg_cn': full_reg_cn_set}
        path = path.replace('{{ds_nodash}}', '{{macros.ds_format(macros.ds_add(ds, -%d), "%%Y-%%m-%%d", "%%Y%%m%%d")}}' % d)
        path_list.append(path)

    return path_list


def add_data_monitor(data_monitor_list, name, path_list):
    data_monitor_template = '''
        cd %s/data_monitor && python timestamp_size_monitor.py --path %s
    ''' % (local_config.WORKSPACE, ' '.join(path_list))

    data_monitor = BashOperator(
        task_id='monitor_%s' % name,
        bash_command=data_monitor_template,
        pool='data_monitor_pool',
        dag=dag
    )
    data_monitor_list.append(data_monitor)

    return data_monitor


def add_file_sensor(name, regs=reg_list, downstream=None, upstream=None):
    for reg in regs:
        now = airflow_utils.FileSensor(
            dag=dag,
            task_id='wait_%s_%s' % (name, reg),
            timeout=10 * 3600,
            pool='data_monitor_pool',
            file_path=os.path.join(monitor_list[name] % {'reg': reg, 'reg_cn': reg}, '_SUCCESS')
        )
        if upstream:
            now.set_upstream(upstream)
        if downstream:
            now.set_downstream(downstream)


def add_flag_sensor(name, regs=reg_list, downstream=None, upstream=None):
    flag_header_map = {
        'DW_ACTIVATE': '%s',
        'DW_ACTIVE': '%s',
        'DW_LAUNCH_ACTIVE': '%s',
        'DW_ACTIVATE_SWIFTCALL': 'flow_%s',
        'DW_ACTIVE_SWIFTCALL': 'flow_%s',
        'DW_LAUNCH_ACTIVE_SWIFTCALL': 'flow_%s',
        'DW_AD_SSPSTAT': '%s_compress_davinci'
    }
    data_type_map = {
        'DW_ACTIVATE': 'activate_smartinput',
        'DW_ACTIVE': 'active_smartinput',
        'DW_LAUNCH_ACTIVE': 'launch_active_smartinput',
        'DW_ACTIVATE_SWIFTCALL': 'activate_swiftcall',
        'DW_ACTIVE_SWIFTCALL': 'active_swiftcall',
        'DW_LAUNCH_ACTIVE_SWIFTCALL': 'launch_active_swiftcall',
        'DW_AD_SSPSTAT': 'ad_sspstat'
    }
    for reg in regs:
        now = airflow_utils.FlagsSensor(
            dag=dag,
            task_id='wait_%s_flag_%s' % (name, reg),
            timeout=10 * 3600,
            region=flag_header_map[name] % reg,
            pool='data_monitor_pool',
            data_type=data_type_map[name]
        )
        if upstream:
            now.set_upstream(upstream)
        if downstream:
            now.set_downstream(downstream)


def airflow_add_usage_data_monitor_task(reg_list, name, upstream=None, downstream=None, delta=data_monitor_delta):
    path_list = []
    wait_data_list = []
    for reg in reg_list:
        for d in range(delta):
            path = data_config.USAGE_DATA('{{macros.ds_format(macros.ds_add(ds, -%d), "%%Y-%%m-%%d", "%%Y%%m%%d")}}' % d,
                                              name, reg, '{{macros.ds_format(macros.ds_add(ds, -%d), "%%Y-%%m-%%d", "%%Y")}}' % d)
            path_list.append(path)
            if d == 0:
                wait_data = airflow_utils.FileSensor(
                    dag=dag,
                    task_id='wait_%s_%s' % (reg, product),
                    timeout=10 * 3600,
                    file_path=path + '/_SUCCESS',
                    pool='data_monitor_pool',
                    priority_weight=-2
                )
                wait_data_list.append(wait_data)

    data_monitor_template = '''
        cd %s/data_monitor && python timestamp_size_monitor.py --path %s
    ''' % (local_config.WORKSPACE, ' '.join(path_list))
    data_monitor = BashOperator(
        task_id='monitor_%s_%s' % ('_'.join(reg_list), name),
        bash_command=data_monitor_template,
        pool='data_monitor_pool',
        dag=dag
    )
    data_monitor.set_upstream(wait_data_list)

    if upstream:
        upstream.set_downstream(data_monitor)

    if downstream:
        data_monitor.set_downstream(downstream)
    return data_monitor


for data_name in ['DW_ACTIVATE', 'DW_ACTIVE', 'DW_LAUNCH_ACTIVE']:
    data_monitor = add_data_monitor(data_monitor_list, data_name, gen_data_path_list(data_name))
    add_flag_sensor(data_name, downstream=data_monitor)
    add_flag_sensor('%s_SWIFTCALL' % data_name, regs=['cn'], downstream=data_monitor)

for data_name in ['DW_AD_SSPSTAT']:
    data_monitor = add_data_monitor(data_monitor_list, data_name, gen_data_path_list(data_name, 1))
    add_flag_sensor(data_name, regs=reg_list + ['cn'], downstream=data_monitor)

# activate summary start check data generated on Thursday every Friday schedule
def is_friday(**kwargs):
    logging.info('execution day is %s' % kwargs['ds_nodash'])
    return datetime.strptime(kwargs['ds_nodash'], '%Y%m%d').isoweekday() == 5

activate_summary_name = 'DW_ACTIVATE_SUMMARY'
cond_is_friday = ShortCircuitOperator(
    task_id='cond_is_friday', python_callable=is_friday, provide_context=True, dag=dag)

activate_summary_path_list = []
for d in [0, 7, 14]:
    path = monitor_list[activate_summary_name] % {'reg_cn': full_reg_cn_set}
    path = path.replace('{{yesterday_ds_nodash}}', '{{macros.ds_format(macros.ds_add(yesterday_ds, -%d), "%%Y-%%m-%%d", "%%Y%%m%%d")}}' % d)
    activate_summary_path_list.append(path)

activate_summary_data_monitor = add_data_monitor(data_monitor_list, activate_summary_name, activate_summary_path_list)
add_file_sensor('DW_ACTIVATE_SUMMARY', regs=reg_list + ['cn'], downstream=activate_summary_data_monitor, upstream=cond_is_friday)

# usage
usage_data_monitor = add_data_monitor(data_monitor_list, 'DW_USAGE', gen_data_path_list('DW_USAGE'))
for reg in reg_list:
    now = airflow_utils.FlagsSensor(
        dag=dag,
        task_id='wait_dw_usage_flag_%s' % reg,
        timeout=airflow_utils.wait_usage_flag_timeout * 3600,
        region=reg,
        pool='data_monitor_pool',
        data_type='usage'
    )
    now.set_downstream(usage_data_monitor)

# skin plugin
skin_data_monitor = add_data_monitor(data_monitor_list, 'DW_SKIN_PLUGIN', gen_data_path_list('DW_SKIN_PLUGIN'))
for reg in reg_list:
    skin_plugin = airflow_utils.FlagsSensor(
        dag=dag,
        task_id='wait_dw_skin_plugin_flag_%s' % reg,
        timeout=airflow_utils.wait_skin_plugin_flag_timeout * 3600,
        region=reg,
        pool='data_monitor_pool',
        data_type='skin_plugin'
    )
    skin_plugin.set_downstream(skin_data_monitor)

# gaia data
for product in GAIA_DATA_LIST:
    airflow_add_usage_data_monitor_task(GAIA_DATA_LIST[product], product, delta=1)

# data monitor report
data_monitor_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{ds_nodash}} -t {{ds_nodash}} --data_source %s --json_path %s/data_monitor
''' % (local_config.WORKSPACE, 'smartinput_data_monitor', local_config.WORKSPACE)
data_monitor_report_import = BashOperator(
    task_id='data_monitor_report_import',
    bash_command=data_monitor_report_import_template,
    dag=dag
)

data_monitor_report_template = '''
    cd %s/data_monitor && python data_monitor_report.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % local_config.WORKSPACE
data_monitor_report = BashOperator(
    task_id='data_monitor_report',
    bash_command=data_monitor_report_template,
    dag=dag
)
data_monitor_report.set_downstream(data_monitor_report_import)

placement_to_app_monitor_template = '''
    cd %s/data_monitor && python check_placement_to_app.py -f {{ds_nodash}} -t {{ds_nodash}}
''' % local_config.WORKSPACE
placement_to_app_monitor = BashOperator(
    task_id='monitor_placement_to_app',
    bash_command=placement_to_app_monitor_template,
    dag=dag
)
placement_to_app_monitor.set_downstream(data_monitor_report)


wait_placement_task = ExternalTaskSensor(
    task_id='wait_placement_map_his',
    timeout=airflow_utils.wait_ad_user_timeout * 3600,
    external_dag_id='roi',
    external_task_id='wait_placement_map_his',
    execution_delta=timedelta(0),
    pool='data_monitor_pool',
    dag=dag)
wait_placement_task.set_downstream(placement_to_app_monitor)

update_monitor_revenue_daily_template = '''
    cd %s/toolbox && python update_data_analysis.py {{yesterday_ds_nodash}} %s /home/libin.yang/revenue_alarm.json http://192.168.1.230:56789/revenue_alarm/load_data
''' % (local_config.WORKSPACE, '%s/%s' % (data_config.DW_DATA_ANALYSIS, 'smartinput_revenue_alarm'))
update_monitor_revenue_daily = BashOperator(
    task_id='update_monitor_revenue_daily',
    bash_command=update_monitor_revenue_daily_template,
    dag=dag
)

monitor_revenue_daily_template = '''
    cd %s/data_monitor && python revenue_alarm.py -f {{yesterday_ds_nodash}} -t {{yesterday_ds_nodash}}
''' % local_config.WORKSPACE
monitor_revenue_daily = BashOperator(
    task_id='monitor_revenue_daily',
    bash_command=monitor_revenue_daily_template,
    dag=dag
)
monitor_revenue_daily.set_downstream(update_monitor_revenue_daily)

wait_ad_stat_ready = ExternalTaskSensor(
    task_id='wait_ad_stat_ready',
    timeout=4 * 3600,
    external_dag_id='commercial_report',
    external_task_id='ad_stat_report',
    execution_delta=timedelta(1),
    dag=dag)
wait_ad_stat_ready.set_downstream(monitor_revenue_daily)

wait_before_ad_stat_ready = TimeDeltaSensor(
    dag=dag,
    task_id='wait_before_ad_stat_ready',
    delta=timedelta(hours=1)
)
wait_before_ad_stat_ready.set_downstream(wait_ad_stat_ready)


data_types = ['click', 'impression']
wait_task_names = ['ads_channel', 'ads_channel_impression']
for data_type, task_name in zip(data_types, wait_task_names):
    ad_sspstat_monitor_template = '''
        cd %s/data_monitor && python ad_sspstat_monitor.py {{ds_nodash}} --type %s
    ''' % (local_config.WORKSPACE, data_type)
    ad_sspstat_monitor = BashOperator(
        task_id='monitor_ad_sspstat_%s' % data_type,
        bash_command=ad_sspstat_monitor_template,
        dag=dag
    )
    ad_sspstat_monitor.set_downstream(data_monitor_report)

    wait_task = ExternalTaskSensor(
        task_id='wait_%s' % task_name,
        timeout=airflow_utils.wait_ad_user_timeout * 3600,
        external_dag_id='roi',
        external_task_id=task_name,
        execution_delta=timedelta(0),
        pool='data_monitor_pool',
        dag=dag)
    wait_task.set_downstream(ad_sspstat_monitor)
    if data_type == 'click':
        wait_task.set_downstream(placement_to_app_monitor)

if __name__ == "__main__":
    dag.cli()
