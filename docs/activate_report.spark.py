import sys, json, os
import argparse
from datetime import datetime, timedelta
import logging

from pyspark import SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, countDistinct, udf
from pyspark.sql import functions as func
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import StringType, BooleanType
from pyspark.storagelevel import StorageLevel

sys.path.append(os.path.join(os.path.dirname(__file__), '../utils'))
import config
import os_utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import local_config

def activate_report(activate_path, activate_source_path, activate_upgrade_path, res_source_path, res_upgrade_path):
    
    sc = SparkContext(appName="activate report")
    sc.setLogLevel('ERROR')

    spark = SparkSession(sc).builder.getOrCreate()
    sc.addPyFile('../utils/ime_utils.py')
    sc.addPyFile('../utils/config.py')
    import ime_utils, config
    
    # loading data
    act_key = ['identifier', 'app_name', 'app_version', 'channel_code', 'recommend_channel']
    act_src_col = act_key + ['mf', 'osv', 'di', 'locale', 'country', 'act_time', 'campaign', 'campaign_id', 'media_source', 'partner', 'source_type', 'referrer', 'package_name', 'utm_source', 'utm_campaign', 'utm_medium']
    activate_new = spark.read.parquet(activate_source_path)
    activate_new = ime_utils.add_missing_source_column(activate_new) \
                    .withColumnRenamed('act_country', 'country')\
                    .select(act_src_col)\
                    .fillna('none', subset=act_key)

    activate_upgrade = spark.read.parquet(activate_upgrade_path).withColumnRenamed('ip_city', 'country')\
    
    dfs = [activate_new, activate_upgrade]
    dims = [
        ['date', 'app_name', 'app_version', 'channel_code', 'recommend_channel', 'campaign', 'campaign_id', 'media_source', 'partner',
         'source_type', 'referrer', 'package_name', 'mf', 'osv', 'di', 'locale', 'country', 'utm_source', 'utm_campaign', 'utm_medium'],
        ['date', 'upgrade_type', 'app_name', 'app_version', 'channel_code', 'recommend_channel', 'package_name', 'mf', 'osv', 'di', 'locale', 'country']
    ]
    res_dfs_path = [res_source_path, res_upgrade_path]
    
    for df, dim, res_path in zip(dfs, dims, res_dfs_path):
        df = df.withColumn('date', col('act_time').substr(1,8))\
                     .fillna('none', subset=dim).groupBy(dim).agg(countDistinct('identifier').alias('uv')).cache()
        df.repartition(1).write.mode('overwrite').json(res_path)
        logging.info("write %s successfully" % res_path)

    # drop exception data
    dim = ['date', 'app_name', 'app_version', 'recommend_channel', 'channel_code', 'country', 'referrer', 'source_type', 'utm_medium']
    udf_is_keyboard = udf(ime_utils.is_keyboard, BooleanType())
    referrer = activate_new.withColumn('date', col('act_time').substr(1,8))\
        .where((udf_is_keyboard(col('app_name'))) & (func.length(col('referrer')) <= 128)) \
        .groupBy(dim).agg(countDistinct('identifier').alias('uv')).cache()
    if referrer.count() > 0:
        # backup to mysql
        db_cfg = config.MYSQL_REF_INFO
        mysql_url = "jdbc:mysql://%s/%s?user=%s&password=%s" % (db_cfg['host'], db_cfg['db'], db_cfg['user'], db_cfg['password'])
        referrer.fillna('none').write.jdbc(url=mysql_url, table=db_cfg['table'], mode='append')
            
    sc.stop()
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='activate statistic')
    parser.add_argument('activate', help = 'input data')
    parser.add_argument('activate_source', help = 'input data')
    parser.add_argument('activate_upgrade', help = 'input data')
    parser.add_argument('res_source', help = 'output data')
    parser.add_argument('res_upgrade', help = 'output data')
    args = parser.parse_args()
    
    activate_report(args.activate, args.activate_source, args.activate_upgrade, args.res_source, args.res_upgrade)
