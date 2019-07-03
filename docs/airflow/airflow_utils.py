import os
import time
from datetime import timedelta, datetime
from airflow.exceptions import AirflowException
from airflow.operators.sensors import S3KeySensor, BaseSensorOperator, ExternalTaskSensor
from airflow.operators import BaseOperator
import s3_utils
import os_utils
import data_config

import redis

# Airflow config
dag_timeout = 23  # 7h D+1 Beijing time
wait_activate_flag_timeout = 10  # 18h Beijing time
wait_active_flag_timeout = 10  # 18h Beijing time
wait_usage_flag_timeout = 10  # 18h Beijing time
wait_skin_plugin_flag_timeout = 10  # 18h Beijing time
wait_smartinput_ios_timeout = 10 # 18h Beijing time

wait_activate_source_timeout = 11  # 19h Beijing time
wait_ad_user_timeout = 15  # 23h Beijing time
wait_active_channel_timeout = 13  # 20h Beijing time
wait_retention_timeout = 13  # 20h Beijing time
wait_revenue_timeout = 14.25  # 22h15m Beijing time

Thursday = 4
Friday=5
Saturday=6

def get_last_weekday(this_date, weekday):
    this_weekday = this_date.isoweekday()
    if this_weekday >= weekday:
        return this_date - timedelta(days=this_weekday - weekday)
    else:
        return this_date - timedelta(days=this_weekday + 7 - weekday)

def get_last_weekly_schdule(this_date, weekday):
    return get_last_weekday(this_date, weekday) - timedelta(days=7)

def get_last_friday_schedule(this_date):
    return get_last_weekly_schdule(this_date, Friday)

def get_quarter_start(this_date):
    this_date = this_date.strftime('%Y%m%d')
    year = int(this_date[:4])
    month = int(this_date[4:6])
    begine_month = (month-1)//3 * 3 + 1
    return datetime(year, begine_month, 1)

def create_set_flag_task(dag, flag_key, flag_value, upstream=None, downstream=None):
    set_matrix_flag = RedisFlagOperator(
        dag=dag,
        task_id='set_%s_%s_flag' % (flag_key, flag_value.lower()),
        value=flag_value,
        flag_key=flag_key,
        priority_weight=20
    )
    if upstream:
        upstream.set_downstream(set_matrix_flag)
    if downstream:
        set_matrix_flag.set_downstream(downstream)
    return set_matrix_flag

def airflow_wait_file_ready(fpath, timeout=None):
    if fpath.startswith('s3://'):
        if s3_utils.s3_wait_file_ready(fpath, timeout):
            return True
    raise AirflowException('wait file failed: %s' % fpath)

class RegionS3KeySensor(S3KeySensor):
    def __init__(self, bucket_key,
            bucket_name=None,
            *args, **kwargs):
        super(RegionS3KeySensor, self).__init__(bucket_key = bucket_key, bucket_name = bucket_name, *args, **kwargs)
        if self.bucket_name == 'af-ext-raw-data':
            self.region = 'appsflyer'
        elif self.bucket_name.startswith('eu.'):
            self.region = 'eu'
        elif self.bucket_name.startswith('ap.'):
            self.region = 'ap'
        else:
            self.region = 'us'

    def poke(self, context):
        ret = False
        if self.region == 'appsflyer':
            ret = s3_utils.s3_is_file_exist('s3://%s/%s-%s/_SUCCESS' % (self.bucket_name, self.bucket_key, context['ds']))
        elif self.region == 'us':
            ret = super(RegionS3KeySensor, self).poke(context)
        else:
            ret = s3_utils.s3_is_file_exist('s3://%s/%s' % (self.bucket_name, self.bucket_key))
        if ret:
            time.sleep(600)
        return ret

class FileSensor(BaseSensorOperator):
    template_fields = ('file_path',)
    def __init__(self, file_path,
                 *args, **kwargs):
        super(FileSensor, self).__init__(*args, **kwargs)
        self.file_path = file_path

    def poke(self, context):
        os_utils.kinit()
        ret = os_utils.is_file_exist(self.file_path)
        if ret:
            time.sleep(600)
        return ret

class FuncExternalTaskSensor(ExternalTaskSensor):
    def __init__(self, external_dag_id,
                 external_task_id,
                 func,
                 *args, **kwargs):
        super(FuncExternalTaskSensor, self).__init__(external_dag_id=external_dag_id, external_task_id=external_task_id, *args, **kwargs)
        self.execution_date_func = func

    def poke(self, context):
        self.execution_delta = context['execution_date'] - self.execution_date_func(context['execution_date'])
        return super(FuncExternalTaskSensor, self).poke(context)


class FlagsSensor(BaseSensorOperator):
    def __init__(self, region, data_type, interval='daily', *args, **kwargs):
        super(FlagsSensor, self).__init__(*args, **kwargs)
        self.region = region
        self.data_type = data_type
        self.interval = interval
        if data_config.DATA_LOCATION == data_config.S3:
            self.func = self.__file_flag__
        else:
            self.func = self.__redis_flag__

    def poke(self, context):
        ds_nodash = context['ds_nodash']
        ts = context['ts']
        return self.func(self.region, self.data_type, self.interval, ds_nodash, ts)

    def __redis_flag__(self, region, data_type, interval, ds_nodash, ts):
        if interval == 'daily':
            check_time = ds_nodash
        elif interval == 'hourly':
            check_time = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S").strftime("%Y%m%d%H")
        else:
            raise Exception('unknown interval')
        ret = os_utils.is_flag_exist('%s_%s' % (region, data_type), check_time, data_config.redis_flag_config)
        if ret:
            return 1
        else:
            return 0

    def __file_flag__(self, region, data_type, interval, ds_nodash, ts):
        if interval == 'daily':
            return os_utils.is_file_exist(data_config.USAGE_FLAG_DIR + '/%s_%s_%s_SUCCESS' % (ds_nodash, region, data_type))
        else:
            raise Exception('unknown interval')


class RedisFlagOperator(BaseOperator):
    """
    set specific flag to True or False
    """
    def __init__(self, flag_key, value, *args, **kwargs):
        super(RedisFlagOperator, self).__init__(*args, **kwargs)
        self.flag_key = flag_key
        self.value = value

    def execute(self, context):
        ds_nodash = context['ds_nodash']
        os_utils.update_redis_flag(self.flag_key, ds_nodash, self.value, data_config.redis_flag_config)