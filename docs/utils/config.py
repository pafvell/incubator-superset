#!/usr/bin/env python
#coding:utf8

#for email
MAIL_CONF = {
    'host': 'smtp.partner.outlook.cn',
    'port': 587,
    'pw': 'IIWMgic%B9ua',
    'user': 'noreply.ime@cootek.cn',
    'msg_from': 'noreply.ime@cootek.cn',
}
# for airflow users
AIRFLOW_USERS = ['lin.zou@cootek.cn', 'kevin.yang@cootek.cn', 'libin.yang@cootek.cn', 'tianfeng.jiang@cootek.cn', 'yang.zhou@cootek.cn']

# DB Config
# mmm03 is slave of mmm01, read only on this db
MYSQL_US1 = {
    'host': 'mmm03.uscasv2.cootek.com',
    'user': 'ime_rw',
    'password': 'ime_rw_passwd'
}
MYSQL_US = {
    'host': 'ime05.uscasv2.cootek.com',
    'user': 'ime_rw',
    'password': 'ime_rw_passwd'
}

MYSQL_DATA_CONFIG = {
    'host': 'mmm01.uscasv2.cootek.com',
    'user': 'ime_rw',
    'password': 'ime_rw_passwd'
}

MYSQL_LOCAL = {
    'user': 'imedata',
    'password': 'imedatarw',
    'host': 'localhost',
}

MYSQL_FINANCE = {
    'user': 'root',
    'password': 'imefinance',
    'host': 'finance01.corp.cootek.com',
    'db': 'ime_finance'
}

MYSQL_APP_INFO = {
    'user': 'gb_data',
    'password': 'tzsakdsasadd22QX2V',
    'host': 'rainbowdb01.corp.cootek.com',
    'db': 'picasso'
}


MYSQL_REF_INFO = {
    'user': 'ime_rw',
    'password': 'ime_rw_passwd',
    'host': 'mysql-mmm-master.rds.corp.cootek.com',
    'db': 'ime_plugin_statistic',
    'table': 'tb_gain_plugin'
}

MYSQL_UA_INFO = {
    'user': 'ime_rw',
    'password': 'ime_rw_passwd',
    'host': 'mysql-mmm-master.rds.corp.cootek.com',
    'port': 3306
}

redis_flag_config = {
    'host': 'redis02',
    'port': 17999,
    'db': 0
}

S3 = 's3://'
S3_NATIVE = 's3n://'
#S3_NATIVE = 's3a://'
HDFS = 'hdfs://'

HDFS_DATA = '/data'

#for amazon emr
EMR_HOME='s3://user.cootek/imecore'
EMR_OUT = '%s/out' % EMR_HOME
DIST_DIR = 'hdfs:///data'
DIST_DIR_OUT = 'hdfs:///out'
JAR_PATH = EMR_HOME + '/jar'

HDFS_TMP_DIR = 'hdfs:///tmp'

# queues on yarn
GB_ETL_DAILY_VH = 'root.gb'  # 'root.gb-root.etl.dailyetl.veryhigh'
GB_ETL_DAILY_H = 'root.gb'  # 'root.gb-root.etl.dailyetl.high'
GB_ETL_DAILY_N = 'root.gb'  # 'root.gb-root.etl.dailyetl.normal'
GB_ETL_DAILY_L = 'root.gb'  # 'root.gb-root.etl.dailyetl.low'
GB_ETL_HOURLY_VH = 'root.gb'  # 'root.gb-root.etl.hourlyetl.veryhigh'
GB_ETL_HOURLY_H = 'root.gb'  # 'root.gb-root.etl.hourlyetl.high'
GB_ETL_HOURLY_N = 'root.gb'  # 'root.gb-root.etl.hourlyetl.normal'
GB_ETL_HOURLY_L = 'root.gb'  # 'root.gb-root.etl.hourlyetl.low'

DATA_LOCATION = HDFS
if DATA_LOCATION == S3:
    IMECORE_HOME = 's3://user.cootek/imecore'
    DW_ROOT = 's3://user.cootek/data_warehouse'
    DW_GB = 's3://user.cootek/data_warehouse'
    IMECORE_DATA='s3://user.cootek/imecore/data'
    def USAGE_DATA(date, usage_type, region):
        usage_type = usage_type.replace('_iOS', '_ios')
        if region == 'us':
            return 's3://ime.data/%s/%s_%s' % (date, date, usage_type)
        else:
            return 's3://user.cootek/imecore/region_data/%s/%s/%s_%s' % (usage_type, region, date, usage_type)
    DW_USAGE = DW_ROOT + '/{usage,eu.usage,ap.usage}'
    USAGE_FLAG_DIR = 's3://user.cootek/data_warehouse/flags'
    FDAU_CONFIG_JSON = 'user.cootek/lin.zou/data/fdau_config'
    DW_SKIN_PLUGIN = DW_ROOT + '/usage/{skin_plugin,eu.skin_plugin,ap.skin_plugin}'
elif DATA_LOCATION == HDFS:
    IMECORE_HOME = 'hdfs:///user/gbdata'
    DW_ROOT = 'hdfs:///data/dw'
    DW_GB = 'hdfs:///user/gbdata/dw'
    RAW_GB = 'hdfs:///user/gbdata/raw'
    TMP_GB = 'hdfs:///user/gbdata/tmp'
    UTILS_GB = 'hdfs:///user/gbdata/utils'
    IMECORE_DATA='hdfs:///user/gbdata/data'
    AD_SSPSTAT = 'hdfs:///data/ad'
    DW_USER = 'hdfs:///user/dw'

    def USAGE_DATA(date, usage_type, region, year=None):
        usage_type = usage_type.replace('_iOS', '_ios')
        if not year:
            year = date[:4]
        return 'hdfs:///data/gaia/online/usage/%s/%s/%s_%s/%s' % (year, date, date, usage_type, region)
    DW_USAGE = DW_ROOT + '/usage'
    FDAU_CONFIG_JSON = DW_GB + '/fdau_config'
    DW_SKIN_PLUGIN = DW_USAGE + '/skin_plugin'


def merge_path(*path):
    path_list = []
    for p in list(path):
        if p.startswith(DATA_LOCATION + '/'):
            path_list.append(p.split(DATA_LOCATION + '/')[1])
        else:
            raise Exception('path %s is not correct' % p)
    return DATA_LOCATION + '/{%s}' % ','.join(path_list)


IP2LOCATION_DATA_NAME = 'IP-COUNTRY-REGION-CITY.BIN'
IP2LOCATION_DATA_PATH = UTILS_GB + '/' + IP2LOCATION_DATA_NAME
IP2LOCATION_DATAV6_NAME = 'IPV6-COUNTRY.BIN'
IP2LOCATION_DATAV6_PATH = UTILS_GB + '/' + IP2LOCATION_DATAV6_NAME

GEOIP2_DATA_NAME = 'GeoIP2-City.mmdb'
GEOIP2_DATA_PATH = UTILS_GB + '/' + GEOIP2_DATA_NAME


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


PATH_DICT = {}


def path_builder(product_section):
    paths = AttrDict()
    # From /data/dw/ Read Only
    paths.DW_ACTIVATE = DW_ROOT + '/activate/%s/*' % product_section
    paths.DW_ACTIVATE_JSON = paths.DW_ACTIVATE + '/json/{eu,ap,us,cn}'
    paths.DW_ACTIVATE_PARQUET = paths.DW_ACTIVATE + '/parquet/{eu,ap,us,cn}'

    paths.DW_ACTIVATE_H = DW_ROOT + '/activate_h/%s/*' % product_section
    paths.DW_ACTIVATE_H_JSON = paths.DW_ACTIVATE_H + '/json/{eu,ap,us,cn}'
    paths.DW_ACTIVATE_H_PARQUET = paths.DW_ACTIVATE_H + '/parquet/{eu,ap,us,cn}'

    paths.DW_EFFECTIVE_ACTIVATE = DW_ROOT + '/effective_activate/daily/%s/*' % product_section
    paths.DW_EFFECTIVE_ACTIVATE_JSON = paths.DW_EFFECTIVE_ACTIVATE + '/json/{eu,ap,us,cn}'
    paths.DW_EFFECTIVE_ACTIVATE_PARQUET = paths.DW_EFFECTIVE_ACTIVATE + '/parquet/{eu,ap,us,cn}'

    paths.DW_ACTIVE = DW_ROOT + '/active/%s/*' % product_section
    paths.DW_ACTIVE_JSON = paths.DW_ACTIVE + '/json/{eu,ap,us,cn}'
    paths.DW_ACTIVE_PARQUET = paths.DW_ACTIVE + '/parquet/{eu,ap,us,cn}'

    paths.DW_ACTIVE_H = DW_ROOT + '/active_h/%s/*' % product_section
    paths.DW_ACTIVE_H_PARQUET = paths.DW_ACTIVE_H + '/parquet/{eu,ap,us,cn}'

    paths.DW_LAUNCH_ACTIVE = DW_ROOT + '/launch_active/%s/*' % product_section
    paths.DW_LAUNCH_ACTIVE_JSON = paths.DW_LAUNCH_ACTIVE + '/json/{eu,ap,us,cn}'
    paths.DW_LAUNCH_ACTIVE_PARQUET = paths.DW_LAUNCH_ACTIVE + '/parquet/{eu,ap,us,cn}'

    paths.DW_RDAU = DW_ROOT + '/rdau/%s/*' % product_section
    paths.DW_RDAU_JSON = paths.DW_RDAU + '/json/{eu,ap,us,cn}'
    paths.DW_RDAU_PARQUET = paths.DW_RDAU + '/parquet/{eu,ap,us,cn}'

    # From /user/gbdata/dw Read and write
    paths.DW_ACTIVATE_SOURCE = DW_ROOT + '/activate_source/%s/*' % product_section
    paths.DW_ACTIVATE_SOURCE_JSON = paths.DW_ACTIVATE_SOURCE + '/json/all'
    paths.DW_ACTIVATE_SOURCE_PARQUET = paths.DW_ACTIVATE_SOURCE + '/parquet/all'

    # From /user/gbdata/dw Read and write
    paths.DW_ACTIVATE_SOURCE_H = DW_GB + '/activate_source_h/%s/*' % product_section
    paths.DW_ACTIVATE_SOURCE_H_JSON = paths.DW_ACTIVATE_SOURCE_H + '/json/all'
    paths.DW_ACTIVATE_SOURCE_H_PARQUET = paths.DW_ACTIVATE_SOURCE_H + '/parquet/all'

    paths.DW_ACTIVATE_SOURCE_SUMMARY = DW_GB + '/activate_source_summary/%s/*/rowkey_based' % product_section
    paths.DW_ACTIVATE_SOURCE_SUMMARY_JSON = paths.DW_ACTIVATE_SOURCE_SUMMARY + '/json'
    paths.DW_ACTIVATE_SOURCE_SUMMARY_PARQUET = paths.DW_ACTIVATE_SOURCE_SUMMARY + '/parquet'

    paths.DW_ACTIVATE_SOURCE_SUMMARY_OLD = DW_GB + '/old_activate_source_summary/%s/*/rowkey_based' % product_section
    paths.DW_ACTIVATE_SOURCE_SUMMARY_OLD_JSON = paths.DW_ACTIVATE_SOURCE_SUMMARY_OLD + '/json/20160930'
    paths.DW_ACTIVATE_SOURCE_SUMMARY_OLD_PARQUET = paths.DW_ACTIVATE_SOURCE_SUMMARY_OLD + '/parquet/20160930'

    paths.DW_ACTIVE_CHANNEL = DW_ROOT + '/active_channel/%s/*' % product_section
    paths.DW_ACTIVE_CHANNEL_JSON = paths.DW_ACTIVE_CHANNEL + '/json/all'
    paths.DW_ACTIVE_CHANNEL_PARQUET = paths.DW_ACTIVE_CHANNEL + '/parquet/all'

    paths.DW_LAUNCH_ACTIVE_CHANNEL = DW_ROOT + '/launch_active_channel/%s/*' % product_section
    paths.DW_LAUNCH_ACTIVE_CHANNEL_JSON = paths.DW_LAUNCH_ACTIVE_CHANNEL + '/json/all'
    paths.DW_LAUNCH_ACTIVE_CHANNEL_PARQUET = paths.DW_LAUNCH_ACTIVE_CHANNEL + '/parquet/all'

    paths.DW_RDAU_CHANNEL = DW_ROOT + '/rdau_channel/%s/*' % product_section
    paths.DW_RDAU_CHANNEL_JSON = paths.DW_RDAU_CHANNEL + '/json/all'
    paths.DW_RDAU_CHANNEL_PARQUET = paths.DW_RDAU_CHANNEL + '/parquet/all'

    paths.DW_USER_RETENTION = DW_ROOT + '/launch_retention_source/%s/*' % product_section
    paths.DW_USER_RETENTION_JSON = paths.DW_USER_RETENTION + '/json/all'
    paths.DW_USER_RETENTION_PARQUET = paths.DW_USER_RETENTION + '/parquet/all'

    paths.DW_USER_RETENTION_MONTH = DW_ROOT + '/launch_retention_source_month/%s/*' % product_section
    paths.DW_USER_RETENTION_MONTH_JSON = paths.DW_USER_RETENTION_MONTH + '/json/all'
    paths.DW_USER_RETENTION_MONTH_PARQUET = paths.DW_USER_RETENTION_MONTH + '/parquet/all'

    paths.DW_USER_RETENTION_WEEK = DW_ROOT + '/launch_retention_source_week/%s/*' % product_section
    paths.DW_USER_RETENTION_WEEK_JSON = paths.DW_USER_RETENTION_WEEK + '/json/all'
    paths.DW_USER_RETENTION_WEEK_PARQUET = paths.DW_USER_RETENTION_WEEK + '/parquet/all'

    paths.DW_PAGE_RETENTION_CHANNEL = DW_GB + '/page_retention_channel/%s/*' % product_section
    paths.DW_PAGE_RETENTION_CHANNEL_PARQUET = paths.DW_PAGE_RETENTION_CHANNEL + '/parquet/all'

    paths.DW_USER_GROUP = DW_GB + '/user_group/%s/*' % product_section
    paths.DW_USER_GROUP_JSON = paths.DW_USER_GROUP + '/json/all'
    paths.DW_USER_GROUP_PARQUET = paths.DW_USER_GROUP + '/parquet/all'

    paths.DW_ACTIVATE_SOURCE_SUMMARY_FLAG = '%s_dw_activate_source_summary' % product_section

    PATH_DICT[product_section] = paths


PRODUCT_SECTIONS = ['gb', 'gct']

for product_section in PRODUCT_SECTIONS:
    path_builder(product_section)

# From /data/dw/ Read Only
DW_ACTIVATE_SUMMARY = DW_ROOT + '/activate_summary/rowkey_based'
DW_ACTIVATE_SUMMARY_JSON = DW_ACTIVATE_SUMMARY + '/json/{eu,ap,us}'
DW_ACTIVATE_SUMMARY_PARQUET = DW_ACTIVATE_SUMMARY + '/parquet/{eu,ap,us}'

# From /user/gbdata/dw Read Only
DW_ACTIVATE = DW_ROOT + '/activate/{%s}/*' % ','.join(PRODUCT_SECTIONS)
DW_ACTIVATE_JSON = DW_ACTIVATE + '/json/{eu,ap,us,cn}'
DW_ACTIVATE_PARQUET = DW_ACTIVATE + '/parquet/{eu,ap,us,cn}'

DW_ACTIVATE_SOURCE = DW_ROOT + '/activate_source/*/*'
DW_ACTIVATE_SOURCE_JSON = DW_ACTIVATE_SOURCE + '/json/all'
DW_ACTIVATE_SOURCE_PARQUET = DW_ACTIVATE_SOURCE + '/parquet/all'

DW_ACTIVATE_SOURCE_SUMMARY = DW_GB + '/activate_source_summary/*/*/rowkey_based'
DW_ACTIVATE_SOURCE_SUMMARY_JSON = DW_ACTIVATE_SOURCE_SUMMARY + '/json'
DW_ACTIVATE_SOURCE_SUMMARY_PARQUET = DW_ACTIVATE_SOURCE_SUMMARY + '/parquet'

DW_ACTIVATE_SOURCE_SUMMARY_OLD = DW_GB + '/old_activate_source_summary/*/*/rowkey_based'
DW_ACTIVATE_SOURCE_SUMMARY_OLD_JSON = DW_ACTIVATE_SOURCE_SUMMARY_OLD + '/json/20160930'
DW_ACTIVATE_SOURCE_SUMMARY_OLD_PARQUET = DW_ACTIVATE_SOURCE_SUMMARY_OLD + '/parquet/20160930'

DW_ACTIVE_JSON = DW_ROOT + '/active/*/*/json/{eu,ap,us,cn}'
DW_ACTIVE_PARQUET = DW_ROOT + '/active/*/*/parquet/{eu,ap,us,cn}'

DW_ACTIVE_CHANNEL_JSON = DW_ROOT + '/active_channel/*/*/json/all'
DW_ACTIVE_CHANNEL_PARQUET = DW_ROOT + '/active_channel/*/*/parquet/all'

DW_LAUNCH_ACTIVE_CHANNEL_JSON = DW_ROOT + '/launch_active_channel/*/*/json/all'
DW_LAUNCH_ACTIVE_CHANNEL_PARQUET = DW_ROOT + '/launch_active_channel/*/*/parquet/all'

DW_RDAU_CHANNEL_JSON = DW_ROOT + '/rdau_channel/*/*/json/all'
DW_RDAU_CHANNEL_PARQUET = DW_ROOT + '/rdau_channel/*/*/parquet/all'

DW_USER_RETENTION_JSON = DW_ROOT + '/launch_retention_source/*/*/json/all'
DW_USER_RETENTION_PARQUET = DW_ROOT + '/launch_retention_source/*/*/parquet/all'

DW_USER_RETENTION_MONTH_JSON = DW_ROOT + '/launch_retention_source_month/*/*/json/all'
DW_USER_RETENTION_MONTH_PARQUET = DW_ROOT + '/launch_retention_source_month/*/*/parquet/all'

DW_USER_RETENTION_WEEK_JSON = DW_ROOT + '/launch_retention_source_week/*/*/json/all'
DW_USER_RETENTION_WEEK_PARQUET = DW_ROOT + '/launch_retention_source_week/*/*/parquet/all'

DW_PAGE_RETENTION_CHANNEL_PARQUET = DW_GB + '/page_retention_channel/*/*/parquet/all'

# From /user/gbdata/dw Read and write
DW_IME_NO_TOKEN = DW_GB + '/ime_no_token'

DW_PERFORMANCE = DW_GB + '/performance/gb/smartinput'
DW_PERFORMANCE_JSON = DW_PERFORMANCE + '/json'
DW_PERFORMANCE_PARQUET = DW_PERFORMANCE + '/parquet'

# From /user/gbdata/data Read and Write
DW_ACTIVATE_SUMMARY_CHANNEL_JSON = DW_GB + '/activate_summary_channel/rowkey_based/json'
DW_ACTIVATE_SUMMARY_CHANNEL_PARQUET = DW_GB + '/activate_summary_channel/rowkey_based/parquet'

DW_AD_SSPSTAT_CLICK = DW_GB + '/ad_sspstat/click'
DW_AD_SSPSTAT_CLICK_JSON = DW_AD_SSPSTAT_CLICK + '/json'
DW_AD_SSPSTAT_CLICK_PARQUET = DW_AD_SSPSTAT_CLICK + '/parquet'

DW_AD_SSPSTAT_IMPRESSION = DW_GB + '/ad_sspstat/impression'
DW_AD_SSPSTAT_IMPRESSION_JSON = DW_AD_SSPSTAT_IMPRESSION + '/json'
DW_AD_SSPSTAT_IMPRESSION_PARQUET = DW_AD_SSPSTAT_IMPRESSION + '/parquet'

REVENUE_RAW = IMECORE_DATA + '/revenue_raw'
REVENUE = IMECORE_DATA + '/revenue/smartinput'
REVENUE_PARQUET = REVENUE + '/parquet'
REVENUE_JSON = REVENUE + '/json'
REVENUE_CHANNEL = IMECORE_DATA + '/revenue_channel/smartinput'
REVENUE_CHANNEL_JSON = REVENUE_CHANNEL + '/json'
REVENUE_CHANNEL_PARQUET = REVENUE_CHANNEL + '/parquet'

USER_ACQUIRE = IMECORE_DATA + '/user_acquire'
USER_ACQUIRE_PARQUET = USER_ACQUIRE + '/parquet'
USER_ACQUIRE_JSON = USER_ACQUIRE + '/json'
USER_ACQUIRE_RAW = USER_ACQUIRE + '/raw'
USER_ACQUIRE_AD_RAW = USER_ACQUIRE + '/ad_creative_raw'
USER_ACQUIRE_CONFIG = USER_ACQUIRE + '/config'
USER_ACQUIRE_AD_CREATIVE_INFO = USER_ACQUIRE + '/ad_creative_info'

HOURLY_USER_ACQUIRE = IMECORE_DATA + '/hourly_user_acquire'
HOURLY_USER_ACQUIRE_PARQUET = HOURLY_USER_ACQUIRE + '/parquet'
HOURLY_USER_ACQUIRE_JSON = HOURLY_USER_ACQUIRE + '/json'
HOURLY_USER_ACQUIRE_RAW = HOURLY_USER_ACQUIRE + '/raw'

ROI_CHANNEL = IMECORE_DATA + '/roi_channel/smartinput'
HOURLY_USER_ACQUIRE_AD_RAW = HOURLY_USER_ACQUIRE + '/ad_creative_raw'
ROI_CHANNEL_JSON = ROI_CHANNEL + '/json'
ROI_CHANNEL_PARQUET = ROI_CHANNEL + '/parquet'

UA_ACCOUNT_MAP = USER_ACQUIRE + "/account_map.csv"

DW_USAGE_HOUR = DW_ROOT + '/usage_hourly'

DW_USAGE_TU = DW_GB + '/usage_tu'
DW_USAGE_TU_JSON = DW_USAGE_TU + '/json'
DW_USAGE_TU_PARQUET = DW_USAGE_TU + '/parquet'

DW_UNIT_USAGE_TU = DW_GB + '/unit_usage_tu'
DW_UNIT_USAGE_TU_JSON = DW_UNIT_USAGE_TU + '/json'
DW_UNIT_USAGE_TU_PARQUET = DW_UNIT_USAGE_TU + '/parquet'

AD_PLATFORM_DETAIL = IMECORE_DATA + '/ad_detail/smartinput'
AD_PLATFORM_DETAIL_PARQUET = IMECORE_DATA + '/ad_detail/smartinput/parquet'
AD_PLATFORM_DETAIL_JSON = IMECORE_DATA + '/ad_detail/smartinput/json'

AD_PLATFORM_DETAIL_HOURLY = IMECORE_DATA + '/ad_detail_hourly/smartinput'
AD_PLATFORM_DETAIL_HOURLY_PARQUET = AD_PLATFORM_DETAIL_HOURLY + '/parquet'
AD_PLATFORM_DETAIL_HOURLY_JSON = AD_PLATFORM_DETAIL_HOURLY + '/json'

AD_USER_ROOT = IMECORE_DATA + '/ad_user'
AD_USER = AD_USER_ROOT + '/*'
AD_USER_JSON = AD_USER + '/json'
AD_USER_PARQUET = AD_USER + '/parquet'

AD_USER_BY_APP_ROOT = IMECORE_DATA + '/ad_user_by_app'
AD_USER_BY_APP = AD_USER_BY_APP_ROOT + '/*/*'
AD_USER_BY_APP_JSON = AD_USER_BY_APP + '/json'
AD_USER_BY_APP_PARQUET = AD_USER_BY_APP + '/parquet'

DW_ACTIVATE_UPGRADE_JSON = DW_GB + '/activate_upgrade/smartinput/json'
DW_ACTIVATE_UPGRADE_PARQUET = DW_GB + '/activate_upgrade/smartinput/parquet'

DW_FAST_ROI_MODEL = IMECORE_DATA + '/fast_roi_model'

FDAU_CHANNEL_JSON = DW_GB + '/fdau_channel/json'

RAW_SSP = IMECORE_DATA + '/raw_ssp'
RAW_SSP_STAT = RAW_SSP + '/stat'
RAW_SSP_REVENUE = RAW_SSP + '/revenue'
RAW_SSP_RESULT = RAW_SSP + '/result'

# data in hourly path is UTC time zone, not platform time
RAW_SPP_HOURLY = RAW_SSP + '_hourly'
RAW_SSP_HOURLY_STAT = RAW_SPP_HOURLY + '/stat'
RAW_SSP_HOURLY_RESULT = RAW_SPP_HOURLY + '/result'

PLACEMENT2SSP = RAW_SSP + '/config/placement2ssp.json'

GOBLIN_DATA = IMECORE_DATA + '/goblin'
GOBLIN_CHANNEL = IMECORE_DATA + '/goblin_channel'
GOBLIN_CHANNEL_JSON = GOBLIN_CHANNEL + '/json'
GOBLIN_CHANNEL_PARQUET = GOBLIN_CHANNEL + '/parquet'

RAW_APPSFLYER_DATA_LOCKER = RAW_GB + '/appsflyer_data_locker'
RAW_APPSFLYER = RAW_GB + '/appsflyer'
DW_APPSFLYER = DW_GB + '/appsflyer'
RAW_APPSFLYER_RAW = RAW_APPSFLYER + '/raw'
RAW_APPSFLYER_JSON = DW_APPSFLYER + '/json'

RAW_APPSFLYER_UNINSTALL = RAW_GB + '/appsflyer_uninstall'
DW_APPSFLYER_UNINSTALL = DW_GB + '/appsflyer_uninstall'
RAW_APPSFLYER_UNINSTALL_RAW = RAW_APPSFLYER_UNINSTALL + '/raw'
RAW_APPSFLYER_UNINSTALL_JSON = DW_APPSFLYER_UNINSTALL + '/json'

RAW_APPSFLYER_RETENTION = RAW_GB + '/appsflyer_rentention'
DW_APPSFLYER_RETENTION = DW_GB + '/appsflyer_rentention'
RAW_APPSFLYER_RETENTION_CSV = RAW_APPSFLYER_RETENTION + '/csv'
DW_APPSFLYER_RETENTION_JSON = DW_APPSFLYER_RETENTION + '/json'

DW_APPSFLYER_PUSH_H = DW_GB + '/appsflyer_push_h'
DW_APPSFLYER_PUSH_H_JSON = DW_APPSFLYER_PUSH_H + '/json'
DW_APPSFLYER_PUSH_H_PARQUET = DW_APPSFLYER_PUSH_H + '/parquet'

DW_ABTEST_USER = DW_GB + '/abtest_user'
DW_ABTEST_USER_JSON = DW_ABTEST_USER + '/json'
DW_ABTEST_USER_PARQUET = DW_ABTEST_USER + '/parquet'

REFERRER_RECOGNIZE_USER_PARQUET = DW_GB + '/referrer_recognize_user/parquet'
DW_ACTIVE_CHANNEL_OVERLAP_PARQUET = DW_GB + '/active_channel_overlap/parquet'

RAW_SWIFTCALL_DB_DATA = RAW_GB + '/swiftcall'
DW_SWIFTCALL = DW_GB + '/swiftcall'
SWIFTCALL_LINECOST_CHANNEL = DW_SWIFTCALL + '/swiftcall_linecost_channel'
SWIFTCALL_LINECOST_CHANNEL_JSON = SWIFTCALL_LINECOST_CHANNEL + '/json'
SWIFTCALL_LINECOST_CHANNEL_PARQUET = SWIFTCALL_LINECOST_CHANNEL + '/parquet'

SWIFTCALL_CREDIT_CHANNEL = DW_SWIFTCALL + '/swiftcall_credit_channel'
SWIFTCALL_CREDIT_CHANNEL_JSON = SWIFTCALL_CREDIT_CHANNEL + '/json'
SWIFTCALL_CREDIT_CHANNEL_PARQUET = SWIFTCALL_CREDIT_CHANNEL + '/parquet'

SWIFTCALL_MESSAGECOST_CHANNEL = DW_SWIFTCALL + '/swiftcall_messagecost_channel'
SWIFTCALL_MESSAGECOST_CHANNEL_JSON = SWIFTCALL_MESSAGECOST_CHANNEL + '/json'
SWIFTCALL_MESSAGECOST_CHANNEL_PARQUET = SWIFTCALL_MESSAGECOST_CHANNEL + '/parquet'

PURCHASE_CHANNEL = IMECORE_DATA + '/purchase_channel'
PURCHASE_CHANNEL_JSON = PURCHASE_CHANNEL + '/json'
PURCHASE_CHANNEL_PARQUET = PURCHASE_CHANNEL + '/parquet'

ACCOUNT_CHANNEL = DW_GB + '/account_channel'
ACCOUNT_CHANNEL_JSON = ACCOUNT_CHANNEL + '/json'
ACCOUNT_CHANNEL_PARQUET = ACCOUNT_CHANNEL + '/parquet'

DW_PAGEVIEW = DW_GB + '/pageview_channel'
DW_PAGEVIEW_JSON = DW_PAGEVIEW + '/json'
DW_PAGEVIEW_PARQUET = DW_PAGEVIEW + '/parquet'

DW_DATA_MONITOR = IMECORE_DATA + '/data_monitor'

DW_DRUID_REPORT = IMECORE_DATA + '/druid_report'
DW_REDIS_IMPORT = IMECORE_DATA + '/redis_import'
DW_DATA_ANALYSIS = IMECORE_DATA + '/data_analysis'
DW_REDIS_BACKUP = IMECORE_HOME + '/backfill/redis_backup'

DW_TALIA_CLICK = DW_GB + '/talia/click'
DW_TALIA_CLICK_JSON = DW_TALIA_CLICK + '/json'
DW_TALIA_CLICK_PARQUET = DW_TALIA_CLICK + '/parquet'

OLD_TALIA_HOME = 'hdfs:///data/ai_data/talia/etl'
TALIA_HOME = DW_GB + '/talia/etl'
TALIA_USAGE = TALIA_HOME + '/usage'
TALIA_SUGGESTION = TALIA_USAGE + '/omnibox_suggestion_click'
TALIA_GO_SEARCH = TALIA_USAGE + '/omnibox_go_search_click'
TALIA_PLACEMENT_MAP = 'hdfs:///data/ai_data/talia/info/talia_placementid_mapping'

# activate report datasource name list
activate_report_name_list = [
    'smartinput_activate_source',
    'smartinput_upgrade_type'
]

# ad_stat report datasource name list
ad_stat_report_name_list = [
    'smartinput_ad_stat',
    'smartinput_detail_ad_stat',
    'smartinput_summary_ad_stat']

# commercial report datasource name list
commercial_report_name_list = [
    'smartinput_commercial_detail',
    'smartinput_detail_commercial_detail',
    'smartinput_commercial_category',
    'smartinput_detail_commercial_category',
    'smartinput_commercial_agg']

fdau_report_name_list = [
    'smartinput_fdau_stat',
    'smartinput_detail_fdau_stat']

# page duration datasource name list
page_duration_report_name_list = [
    'smartinput_page_duration_detail',
    'smartinput_page_duration'
]

# performance datasource name list
background_stat_report_name_list = [
    'smartinput_background_battery',
    'smartinput_background_traffic',
    'smartinput_background_thread_memory',
    'smartinput_background_thread_cut'
]

# referrer recognize datasource name list
referrer_recognize_report_name = 'smartinput_referrer_recognize'

# method_lag_report_name datasource name list
method_lag_report_name = 'smartinput_method_lag'
process_lifetime_report_name = 'smartinput_process_lifetime'

# store report
store_track_report_name = 'smartinput_store_track'
store_pageview_report_name = 'smartinput_store_pageview'
duration_funnel_report_name = 'smartinput_duration_funnel'

# ime_game
game_report_name = "smartinput_ime_game"

silent_install_report = 'smartinput_silent_install'

# swift
swift_performance_report = "smartinput_swiftcall_call_performance"
swift_user_activity_report = 'smartinput_swiftcall_user_activity'

# gdpr_report
gdpr_report_name = 'smartinput_gdpr'

# commercial_coverage_report
commercial_coverage_report_name = 'smartinput_commercial_coverage'

# ar_emoji
ar_emoji_name = 'smartinput_ar_emoji'

# login_stat
login_stat_report_name = 'smartinput_login_stat'

# referrer_report
referrer_report_name = 'smartinput_referrer'

# hades
hades_not_show_report_name = 'smartinput_hades_not_show'

# crash_ratio_report
crash_ratio_report_name = 'smartinput_crash_ratio'
cdn_report_name = 'smartinput_cdn'
webview_report_name = 'smartinput_webview'

## matrix product
PATH_DICT['gb'].MATRIX_PRODUCT_LIST = [
    'usage_ime_filter',
    'ime_dialer',
    'audio_clock',
    #'touchist',  app removed 20171110
    #'usage_newsbee_international',
    'usage_ime_touchfun',
    'GB_wallpaper',
    'gamepack',
    'drinkwater',
    'usage_todolist',
    'usage_sevenfit',
    'usage_flashlight',
    'usage_wordchef',
    'usage_callshow',
    'usage_music', # added 20171203, activated 20171215
    'usage_sevenfit_ios', # added 20171212, activated 20171217
    'gb_hades_internal', # added 20180110
    'usage_steps', # added 20180116, activated 20180129
    'usage_horoscope', # added 20180129, activated 20180129
    'usage_period', # added 20180208, activated 20180208
    'usage_pixeldraw', # added 20180212, activated 20180212
    'usage_sleep', # added 20180314, activated 20180314
    'usage_clock', # added 20180314, activated 20180314
    'usage_onedraw',  # added 20180315, activated 20180315
    'usage_scanner',  # added 20180428, activated 20180428
    'usage_camera',  # added 20180525, activated 20180525
    'usage_iosword',  # added 20180525, activated 20180525
    'usage_radiofm',  # added 20180612, activated 20180612
    'usage_messenger',  # added 20180614, activated 20180614
    'usage_gdpr',  # added 20180717, activated 20180717
    'gb_hades_internal_vice', # added 20181015
    'usage_wallpaper', # added 20181106
    'usage_calculator', # added 20181226
    'usage_pcsplugin', # added 20181226
]

PATH_DICT['gb'].OTHER_MATRIX_PRODUCT_LIST = [
    'usage_swiftcall',
    'usage_swiftcall_iOS',
]

PATH_DICT['gb'].SERVER_PRODUCT_LOG_LIST = [
    'gb_server_swiftcall',
]

PATH_DICT['gct'].MATRIX_PRODUCT_LIST = [
    'usage_biu',
    'usage_veeu'
]


SWIFTCALL_APP_NAME = [
    "swift.free.phone.call.wifi.chat",
    "swift.free.wifi.phone.call.chat",
    "swift.free.phone.call.wifi.chat.ios",
    "free.phone.call.abroad.overseas.calling",
    "free.phone.call.abroad.overseas.calling.light.india",
    "dacall.overseas.free.call.wifi.india",
    "dacall.overseas.free.call.wifi.pakistan",
    "dacall.overseas.free.call.wifi.bangladesh",
]

GOBLIN_START_DATE = '20171101'
GOBLIN_INCOME_START_DATE = '20180701'
ACCOUNT_INFO_START_DATE = '20171123'
PURCHASE_START_DATE = '20180619'
MESSAGE_COST_START_DATE = '20180614'
PATH_DICT['gb'].RDAU_START_DATE = '20180701'
PATH_DICT['gct'].RDAU_START_DATE = '20181128'
RDAU_CHANNEL_START_DATE = '20180725'
DAU_OVERLAP_START_DATE = "20181112"
TALIA_CLICK_START_DATE = '20180801'
REAL_ROI_SUMMARY_DATES = [1, 2, 3, 5, 7, 15, 30, 45, 60, 75, 90, 120, 150, 180]
REAL_ROI_SUMMARY_IMPORT_DATES = [1, 2, 3, 5, 7, 15, 30, 45, 60, 75, 90]
ROI_PERFORMANCE_DATES = [30, 45, 60, 75, 90]
ACT_AD_STAT_REPORT_DATES = [1, 2, 3, 5, 7]
RETENTION_REPORT_DAYS = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 30, 60, 90]
RETENTION_MONTHLY_REPORT_MONTHS = [1, 2, 3, 4, 5, 6, 7]
RETENTION_WEEKLY_REPORT_WEEKS = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
AD_USER_LTV_DAYS = [7, 15, 30, 45, 60, 75, 90]

LT_REPORT_DAYS = [7, 15, 30, 60, 90]

APPSFLYER_RETENTION_DAYS = [2, 4, 6, 8, 14]
EZALTER_LT_DAYS = [15, 30]
METRIC_SUFFIX = ['_f', ''] # f stands for fuzzy

CODE2REGION = {'us':'us-east-1', 'eu':'eu-central-1', 'ap':'ap-southeast-1'}
CODE2REGION_PREFIX = {'us':'', 'eu':'eu.', 'ap':'ap.'}
#ap: subnet-ae1afdc7
CODE2SUBNET = {'us':'subnet-2173c756', 'eu':'subnet-51dadc29', 'ap':'subnet-ee23238b'}

RETENTION_TYPE_DAILY = 'date'
RETENTION_TYPE_MONTHLY = 'month'
RETENTION_TYPE_WEEKLY = 'week'
RETENTION_DICT = {
    RETENTION_TYPE_DAILY: RETENTION_REPORT_DAYS,
    RETENTION_TYPE_MONTHLY: RETENTION_MONTHLY_REPORT_MONTHS,
    RETENTION_TYPE_WEEKLY: RETENTION_WEEKLY_REPORT_WEEKS
}

LT_DICT = {
    RETENTION_TYPE_DAILY: LT_REPORT_DAYS,
    RETENTION_TYPE_MONTHLY: [],
    RETENTION_TYPE_WEEKLY: []
}

PAGE_RETENTION_DICT = {
    RETENTION_TYPE_DAILY: [1, 2, 3, 5, 7, 14, 30, 60]
}
