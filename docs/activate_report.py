import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
import subprocess

sys.path.append(os.path.join(os.path.dirname(__file__), '../utils'))
import config
import os_utils, s3_utils, ime_utils, cluster
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import local_config

def activate_report(date, mode='hadoop'):
    activate_path = config.PATH_DICT['gb'].DW_ACTIVATE_PARQUET + '/' + date
    activate_source_path = config.DW_ACTIVATE_SOURCE_PARQUET + '/' + date 
    activate_upgrade_path = config.DW_ACTIVATE_UPGRADE_PARQUET + '/' + date 

    path_list = [os.path.join(config.DW_DRUID_REPORT, name, date) for name in config.activate_report_name_list]
    
    params = tuple([
        activate_path,
        activate_source_path,
        activate_upgrade_path,
    ])

    # clear db env
    db_cfg = config.MYSQL_REF_INFO
    db_cmd = 'delete from %s where date=%s' % (db_cfg['table'], date)
    cmd = 'mysql -u%s -p%s -h %s %s -e "%s"' % (db_cfg['user'], db_cfg['password'], db_cfg['host'], db_cfg['db'], db_cmd)
    ret, _, _ = os_utils.local_run(cmd)
    if ret != 0:
        raise Exception('clear referrer database faild')

    dname, fname = os.path.split(os.path.abspath(__file__))
    sub = 'activate_report.spark.py %s %s %s ' % params + " ".join(path_list)
    if mode == 'local':
        clu = cluster.ClusterLocal('activate_report')
        cmd = "cd %s && spark-submit --conf \'spark.driver.memory=6g\' --master=local[6] %s" % (dname, sub)
    else:
        clu = cluster.ClusterHadoop('activate_report')
        cmd = "cd %s && spark-submit --master=yarn --driver-memory 2G --executor-memory 5g --queue %s --num-executors 5  --executor-cores 3 %s" % (dname, config.GB_ETL_DAILY_VH, sub)
    logging.info(cmd)
       
    clu.run_task_cmd(cmd, output_path=path_list, realtime_log_output=True)
    clu.close()

    return 0
        
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description='activate report runner')
    parser.add_argument('begin', help = 'start date')
    parser.add_argument('end', help = 'end date')
    args = parser.parse_args()
    
    if args.begin > args.end:
        raise Exception('parameter error: begin need <= end')
    
    for date in ime_utils.gen_days_list(args.begin, args.end):
        ret = activate_report(date)
        if ret != 0:
            logging.error('ERROR: %s activate report failed' % this_date)
            sys.exit(ret)

    logging.info('activate_report done')

        
