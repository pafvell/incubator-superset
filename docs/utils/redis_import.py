import argparse
import redis, json, time, logging
from concurrent.futures import ThreadPoolExecutor, as_completed

BATCH_SIZE = 10000
SLEEP_TIME = 0.1
## delete hset in batch of 100000
def redis_delete(server, r, name):
    logging.info('%s -- delete table: %s started' % (server, name))
    cursor = 0
    while True:
        time.sleep(SLEEP_TIME)
        cursor, fields = r.hscan(name, cursor, count=BATCH_SIZE)
        hkeys = fields.keys()
        if len(hkeys)>0:
            r.hdel(name, *hkeys)
        if cursor==0:
            break

    logging.info('%s -- delete table: %s finished' % (server, name))


def redis_replace(server, r, name, name_tmp):
    logging.info('%s -- replace table: %s started' % (server, name))
    pipe = r.pipeline(transaction=False)

    # delete key that not in tmp table
    cursor = 0
    while True:
        time.sleep(SLEEP_TIME)
        cursor, fields = r.hscan(name, cursor, count=BATCH_SIZE)
        for key in fields.keys():
            pipe.hexists(name_tmp, key)
        flags = pipe.execute()
        invalid_hkeys = [key for (key, flag) in zip(fields.keys(), flags) if not flag]
        if len(invalid_hkeys) > 0:
            r.hdel(name, *invalid_hkeys)
        if cursor == 0:
            break

    # copy data from tmp table
    cursor = 0
    while True:
        time.sleep(SLEEP_TIME)
        cursor, fields = r.hscan(name_tmp, cursor, count=BATCH_SIZE)
        for key, val in fields.items():
            pipe.hset(name, key, val)
        pipe.execute()
        if cursor == 0:
            break

    # delete tmp table
    redis_delete(server, r, name_tmp)
    logging.info('%s -- replace table: %s done' % (server, name))


def import_task(server, df_file, name, key, value, expire_time):
    r = redis.Redis(host=server['host'], port=server['port'])
    if r.exists(name):
        name_tmp = '%s_tmp' % name
        redis_delete(server, r, name_tmp)
    else:
        name_tmp = name

    # build table
    pipe = r.pipeline(transaction=False)
    cnt = 0
    with open(df_file, 'r') as f:
        for line in f:
            if cnt == BATCH_SIZE:
                pipe.execute()
                cnt = 0
            item = json.loads(line)
            pipe.hset(name_tmp, str(item[key]), str(item[value]))
            cnt += 1
    pipe.expire(name_tmp, expire_time)
    pipe.execute()

    # replace table
    if name_tmp != name:
        redis_replace(server, r, name, name_tmp)
    r.expire(name, expire_time)


def redis_import(df_file, cf_file):
    with open(cf_file, 'r') as f:
        cfg = json.loads(f.read())

    servers = cfg['servers']
    name, key, value = cfg['name'], cfg['key'], cfg['value']
    expire_time = int(float(cfg['time']))

    fs = {}
    with ThreadPoolExecutor(4) as executor:
        for server in servers:
            task = executor.submit(import_task, server, df_file, name, key, value, expire_time)
            fs[task] = server

    for future in as_completed(fs):
        logging.info('thread return %s' % fs[future])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='redis import tasks.')
    parser.add_argument('--df_file', '-df', type=str, required=True, help='JSON file: data')
    parser.add_argument('--cf_file', '-cf', type=str, required=True, help='JSON file: config')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    redis_import(args.df_file, args.cf_file)
