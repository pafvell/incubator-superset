from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import ExternalTaskSensor, SqlSensor
from airflow.models import DAG
from datetime import datetime, timedelta
import os

import data_config
import local_config
import airflow_utils

args = {
    'owner': 'tianfeng.jiang',
    'start_date': datetime(2017, 7, 11),
    'retries':1,
    'email': data_config.AIRFLOW_USERS
}

dag = DAG(
    dag_id='abtest_ltv_report',
    default_args=args,
    schedule_interval = timedelta(days=1),
    dagrun_timeout=timedelta(hours=airflow_utils.dag_timeout))

abtest_ltv_report_template = '''
    cd %s/abtest_report && python ./abtest_ltv_report.py {{ds_nodash}} {{ds_nodash}} 14
'''%local_config.WORKSPACE
abtest_ltv_report = BashOperator(
    task_id='abtest_ltv_report',
    bash_command = abtest_ltv_report_template,
    dag=dag
)

abtest_ltv_report_import_template = '''
    cd %s/druid_report && python druid_report_import.py -f {{yesterday_ds_nodash}} -t {{yesterday_ds_nodash}} --data_source %s --json_path %s/abtest_report
''' % (local_config.WORKSPACE, 'smartinput_abtest_ltv', local_config.WORKSPACE)
abtest_ltv_report_import = BashOperator(
    task_id='abtest_ltv_report_import',
    bash_command=abtest_ltv_report_import_template,
    dag=dag
)
abtest_ltv_report_import.set_upstream(abtest_ltv_report)

abtest_user_template = '''
    cd %s/abtest_report && python ./abtest_user.py {{ds_nodash}} {{ds_nodash}}
'''%local_config.WORKSPACE

abtest_user = BashOperator(
    task_id='abtest_user',
    bash_command = abtest_user_template,
    dag=dag
)
abtest_user.set_downstream(abtest_ltv_report) 

# retention_source
user_retention = ExternalTaskSensor(
    task_id='wait_retention_source',
    timeout=airflow_utils.wait_retention_timeout*3600,
    external_dag_id = 'launch_active_channel',
    external_task_id = 'launch_retention_source',
    execution_delta = timedelta(0), # note: previous day data
    dag=dag)
user_retention.set_downstream(abtest_ltv_report)

# ad_user
ad_user = ExternalTaskSensor(
    task_id='wait_ad_user',
    timeout=airflow_utils.wait_ad_user_timeout*3600,
    external_dag_id = 'roi',
    external_task_id = 'ad_user',
    dag=dag)
ad_user.set_downstream(abtest_ltv_report)

for reg in ['us', 'ap', 'eu']:
    usage_type = 'integrated_abtest'
    data_path = data_config.USAGE_DATA('{{ds_nodash}}', usage_type, reg, '{{macros.ds_format(ds, "%Y-%m-%d", "%Y")}}')
    wait_data = airflow_utils.FileSensor(
        dag = dag,
        task_id='wait_%s_abtest' % (reg),
        timeout=10*3600,
        file_path = data_path + '/_SUCCESS',
        priority_weight=-2
    )
    wait_data.set_downstream(abtest_user)

# abtest config
os.environ["AIRFLOW_CONN_IME_CONFIG"] = "mysql://%s:%s@%s/ime_config" % (data_config.MYSQL_US['user'], data_config.MYSQL_US['password'], data_config.MYSQL_US['host'])
for ts, tb in zip(['wait_abtest_exp', 'wait_abtest_group'], ['ime_integrated_abtest', 'ime_integrated_abtest_groups']):
    conf_data = SqlSensor(
        task_id = '%s' % ts,
        conn_id = 'ime_config',
        sql = "select count(*) from %s limit 10" % tb,
        timeout = 15*3600,
        dag=dag)
    conf_data.set_downstream(abtest_user)


if __name__ == "__main__":
    dag.cli()
