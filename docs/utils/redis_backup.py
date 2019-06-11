import argparse
import os
import config
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import local_config

import os_utils
import redis, json, time, logging
from concurrent.futures import ThreadPoolExecutor, as_completed

BATCH_SIZE = 10000
SLEEP_TIME = 0.1


# backup hset in batch of 100000
def backup_task(server, temp_dir, hdfs_save_path, date, name):
    r = redis.Redis(host=server['host'], port=server['port'])
    logging.info('%s -- get table: %s started' % (server, name))
    region_tmp_dir = os.path.join(temp_dir, server['region'], date)
    region_hdfs_save_path = os.path.join(hdfs_save_path, server['region'], date)
    file_path = os.path.join(region_tmp_dir, name+".json")
    os.system('rm -rf %s && mkdir -p %s' % (region_tmp_dir, region_tmp_dir))
    logging.info('save path: %s', file_path)
    with open(file_path, mode='w') as f:
        cursor = 0
        while True:
            time.sleep(SLEEP_TIME)
            cursor, fields = r.hscan(name, cursor, count=BATCH_SIZE)
            hkeys = fields.keys()
            if len(hkeys) > 0:
                kv_list = [{"key": k, "ime_value": fields[k]} for k in hkeys]
                for item in kv_list:
                    f.write(json.dumps(item)+'\n')
            if cursor == 0:
                break
    os_utils.sync(os_utils.LOCAL + region_tmp_dir, region_hdfs_save_path)
    logging.info('%s -- save table: %s finished' % (server, name))


def redis_backup(date, cf_file):
    with open(cf_file, 'r') as f:
        cfg = json.loads(f.read())
    servers = cfg['servers']

    temp_dir = os.path.join(local_config.LOCAL_TMP_DIR,  'redis_backup')
    backup_hdfs_path = config.DW_REDIS_BACKUP
    table_name = 'ime_' + date

    fs = {}
    with ThreadPoolExecutor(4) as executor:
        for server in servers:
            task = executor.submit(backup_task, server, temp_dir, backup_hdfs_path, date, table_name)
            fs[task] = server
    for future in as_completed(fs):
        logging.info('thread return %s' % fs[future])
    # delete local temp dir
    os.system('rm -rf %s' % temp_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='redis import tasks.')
    parser.add_argument('--date', '-d', type=str, required=True, help='redis table name: date')
    args = parser.parse_args()

    dname, _ = os.path.split(os.path.abspath(__file__))
    cf_file_path = os.path.join(dname, 'redis_config.json')

    logging.basicConfig(level=logging.INFO)
    redis_backup(args.date, cf_file_path)
