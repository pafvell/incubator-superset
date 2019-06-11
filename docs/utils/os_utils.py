import os
import sys
import subprocess
from datetime import datetime, timedelta
import tempfile
import logging
import copy
import re
import uuid
from threading import Thread
import json
import hashlib
import time

try:
    import ujson as json
except Exception as e:
    import json

from multiprocessing import Pool
import s3_utils

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import local_config
from itertools import islice

#rainbow03
DRUID_HOST = '%s192.168.1.47' % (local_config.RAINBOW_USER + '@')
HADOOP = '/usr/local/hadoop-2.6.3/bin/hadoop'

S3 = 's3://'
HDFS = 'hdfs://'
LOCAL = 'file://'
RETRIES = 3
POOL_NUM = 20

#subprocess run cmd
#cmd: input cmd
#realtime_log_output:
#   True: with real-time log output
#   False: get output after subprocess ends.
#   Default: False
def local_run(cmd,realtime_log_output=False):
    logging.info(cmd)
    
    if realtime_log_output:
        import Queue
        def read_output(pipe, funcs):
            for line in iter(pipe.readline, b''):
                for func in funcs:
                    func(line.decode('utf-8'))
            pipe.close()
        def write_output(get):
            for line in iter(get, None):
                try:
                    sys.stdout.write(line)
                except UnicodeEncodeError as e:
                    pass
        popen = subprocess.Popen(cmd, close_fds=True, bufsize=1, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        out_data = []
        err_data = []
        
        q = Queue.Queue()
        
        #stdout thread, put stdout info to q and store stdout info in out_data
        stdout_thread = Thread(target=read_output, args=(popen.stdout, [q.put, out_data.append]))
        #stderr thread, put stderr info to q and store stderr info in err_data
        stderr_thread = Thread(target=read_output, args=(popen.stderr, [q.put, err_data.append]))
        #output thread, output info from q
        writer_thread = Thread(target=write_output, args=(q.get,))

        for t in (stdout_thread, stderr_thread, writer_thread):
            #A thread can be flagged as a "daemon thread". The significance of this flag is that the entire Python program exits when only daemon threads are left.
            t.daemon = True
            t.start()

        popen.wait()

        for t in (stdout_thread, stderr_thread):
            t.join()

        q.put(None)
        
        out_data = ' '.join(out_data)
        err_data = ' '.join(err_data)
    else:
        popen = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out_data, err_data = [it.strip() for it in popen.communicate()]
        if type(err_data) == bytes:
            err_data = err_data.decode('utf8').strip()
        if type(out_data) == bytes:
            out_data = out_data.decode('utf8').strip()
    return int(popen.returncode), out_data, err_data

def local_run_parallel(cmds):
    handles = []
    for cmd in cmds:
        popen = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        handles.append(popen)
    rets = []
    for popen in handles:
        out_data, err_data = [it.strip() for it in popen.communicate()]
        if type(err_data) == bytes:
            err_data = err_data.decode('utf8').strip()
        if type(out_data) == bytes:
            out_data = out_data.decode('utf8').strip()
        rets.append((int(popen.returncode), out_data, err_data))
    return rets

#NOTE: account info must be setup correctly in ~/.ssh/config
#for example:
#Host 192.168.1.47
#    IdentityFile /home/lin.zou/.ssh/xxxx
#    User lin.zou

def remote_run(host, cmd, files = [], keep_dir=False):
    try:
        remote_dir = '/tmp/remote_run_%s/%s' % (local_config.KEY_NAME, uuid.uuid4())
        lcmd = 'ssh %s "rm -rf %s && mkdir -p %s"' % (host, remote_dir, remote_dir)
        clean_cmd = 'ssh %s "rm -rf %s"' % (host, remote_dir)
        returncode, out_data, err_data = local_run(lcmd)
        if returncode != 0:
            raise Exception('error cmd: %s' % lcmd)
    
        if files:
            if not keep_dir:
                lcmd = 'scp -C %s %s:%s' % (' '.join(files), host, remote_dir)
            else:
                lcmd = 'tar -cf - %s | ssh -C %s "cd %s && tar -xf -"' % (' '.join(files), host, remote_dir)
            returncode, out_data, err_data = local_run(lcmd)
            if returncode != 0:
                logging.info(out_data)
                logging.error(err_data)
                raise Exception('error cmd: %s' % lcmd)
    
        lcmd = 'ssh %s "cd %s && %s"' % (host, remote_dir, cmd)
        returncode, out_data, err_data = local_run(lcmd)
        if returncode != 0:
            logging.info(out_data)
            logging.error(err_data)
            raise Exception('error cmd: %s' % lcmd)
        local_run(clean_cmd)
    except Exception as e:
        logging.error(e)
        local_run(clean_cmd)
        
    return returncode, out_data, err_data


def redis_import(name, date, table, exp_seconds):
    import config
    local_tmp_dir = os.path.join(local_config.LOCAL_TMP_DIR, name, date)
    os.system('rm -rf %s && mkdir -p %s' % (local_tmp_dir, local_tmp_dir))

    # get file from hdfs to local tmp
    sync(os.path.join(config.DW_REDIS_IMPORT, name, date), LOCAL + local_tmp_dir)
    FILE_NAME_RE = r'^part-00000.*\.json$'
    data_path = None
    for fn in os.listdir(local_tmp_dir):
        result = re.match(FILE_NAME_RE, fn)
        if result:
            data_path = os.path.join(local_tmp_dir, fn)
            break

    if not data_path:
        raise Exception('output file not exist!')

    dname, _ = os.path.split(os.path.abspath(__file__))
    ## fill template
    index_fname = 'redis_config.json'
    index_path = os.path.join(dname, index_fname)
    templated_index_path = os.path.join(local_tmp_dir, index_fname)
    with open(index_path) as fin:
        cont = fin.read().replace('{{name}}', table).replace('{{time}}', str(exp_seconds))
    with open(templated_index_path, mode='w') as fout:
        fout.write(cont)

    cmd = 'cd %s && python redis_import.py -cf %s -df %s' % (dname, templated_index_path, data_path)
    return local_run('%s && rm -rf %s' % (cmd, local_tmp_dir))
    

def druid_import(report_name, path_index, path_data, start_date, end_date, dtformat='%Y%m%d', host=DRUID_HOST, business_unit_prefix='smartinput'):
    #copy data into hdfs
    hadoop_dir = '/user/%s/druid_report/%s' % (local_config.RAINBOW_USER, report_name)
    hadoop_file = '%s/%s_%s' % (hadoop_dir, start_date, end_date)
    data_dname, data_fname = os.path.split(path_data)
    cmd = '%s fs -rm -f %s && %s fs -mkdir -p %s && %s fs -put %s %s' % (HADOOP, hadoop_file, HADOOP, hadoop_dir, HADOOP, data_fname, hadoop_file)
    returncode, out_data, err_data = remote_run(host, cmd, files=[path_data])
    if returncode != 0:
        return returncode, out_data, err_data

    #run import
    index_dname, index_fname = os.path.split(path_index)
    os.system('mkdir -p %s' % ('/tmp/smartinput_druid_import_%s' % local_config.KEY_NAME))
    tmp_dir = tempfile.mkdtemp(dir='/tmp/smartinput_druid_import_%s' % local_config.KEY_NAME)
    templated_index_path = tmp_dir + '/' + index_fname
    if dtformat=='%Y%m%d':
        start_date_1 = datetime.strptime(start_date, dtformat).strftime('%Y-%m-%d')
        delta=timedelta(days=1)
        grain = 'day'
        end_date_1 = (datetime.strptime(end_date, dtformat) + delta).strftime('%Y-%m-%d')
    elif dtformat=='%Y%m%d%H':
        start_date_1 = datetime.strptime(start_date, dtformat).strftime('%Y-%m-%dT%H:%M:%S')
        delta=timedelta(hours=1)
        grain = 'hour'
        end_date_1 = (datetime.strptime(end_date, dtformat) + delta).strftime('%Y-%m-%dT%H:%M:%S')
    else:
        raise Exception('dtformat(date format) error')

    with open(path_index) as fin:
        cont = fin.read().replace('{{grain}}', grain)\
                .replace('{{date}}', start_date_1 + '/' + end_date_1)\
                .replace('{{import_path}}', 'hdfs://%s' % hadoop_file)\
                .replace('{{business_unit_prefix}}', business_unit_prefix)\
                .replace('{{queue}}', "root.gb")
    with open(templated_index_path, mode='w') as fout:
        fout.write(cont)

    import ConfigParser
    config = ConfigParser.ConfigParser()
    config.read('../utils/credential.conf')
    user_name = config.get('druid_%s' % business_unit_prefix, 'user')
    password = config.get('druid_%s' % business_unit_prefix, 'password')
    cmd = '/usr/local/imply-1.3.0/bin/import_data_new %s %s %s' % (index_fname, user_name, password)
    for retry in range(2):
        returncode, out_data, err_data = remote_run(host, cmd, files=[templated_index_path])
        if returncode == 0:
            break
    local_run('rm -rf %s' % tmp_dir)
    return returncode, out_data, err_data
    


def check_ret(returncode, err_data):
    if returncode != 0:
        logging.error(err_data)
        raise Exception("Error:", err_data)


def local_run_cmd_and_check(cmd):
    returncode, out_data, err_data = local_run(cmd)
    check_ret(returncode, err_data)
    logging.info(out_data + '\n' + err_data)
    return returncode, out_data, err_data

def __mkdir_hdfs__(dir):
    os.system('hadoop fs -mkdir -p %s' % dir)
    return 0

def __file_s3_to_hdfs__(param):
    region_code = s3_utils.get_region_code_from_fname(param[0])
    region = s3_utils.CODE2REGION[region_code]
    cmd = 'aws s3 cp %s - --region %s | hadoop fs -put -f - %s' % (param[0], region, param[1])
    returncode, out_data, err_data = local_run(cmd)
    if returncode != 0:
        logging.error(err_data)
    return returncode


def __file_hdfs_to_s3__(param):
    region_code = s3_utils.get_region_code_from_fname(param[1])
    region = s3_utils.CODE2REGION[region_code]
    cmd = 'hadoop fs -cat %s | aws s3 cp - %s --region %s' % (param[0], param[1], region)
    returncode, out_data, err_data = local_run(cmd)
    if returncode != 0:
        logging.error(err_data)
    return returncode

crc_file_re = re.compile(r'''(^|/)\.[^/]*\.crc\Z''')
def __dir_s3_to_hdfs__(src_path, dest_path):
    if src_path == S3:
        raise Exception("Error: No bucket in source path")

    src_path = src_path.strip('/') + '/'
    dest_path = dest_path.strip('/') + '/'

    path_after_s3 = src_path[len(S3):]
    bucket = path_after_s3[:path_after_s3.index('/')]

    region_code = s3_utils.get_region_code_from_fname(src_path)
    region = s3_utils.CODE2REGION[region_code]

    cmd = 'aws s3 ls %s --recursive --region %s | awk \'{print $4}\'' % (src_path, region)
    returncode, filenames, err_data = local_run(cmd)
    filenames = filenames.strip()
    if filenames == '':
        raise Exception('No file in source path')
    else:
        filenames = filenames.split('\n')

    src_path_starts = src_path[len(S3 + bucket + '/'):]

    cp_list = []
    dir_set = set()
    for name in filenames:
        add = name[len(src_path_starts):]
        if '$folder$' in add or crc_file_re.search(add):
            continue

        cur_src = S3 + bucket + '/' + name
        cur_dest = dest_path + add

        dest_dir = cur_dest[:cur_dest.rfind('/')]
        if len(dest_dir) > 0 and dest_dir not in dir_set:
            dir_set.add(dest_dir)

        cp_list.append((cur_src, cur_dest))

    pool_with_retries(list(dir_set), __mkdir_hdfs__)
    pool_with_retries(cp_list, __file_s3_to_hdfs__)
    return 0


def __dir_hdfs_to_s3__(src_path, dest_path):
    if src_path == HDFS:
        raise Exception("Error: cannot sync from hdfs://")

    src_path = src_path.strip('/') + '/'
    dest_path = dest_path.strip('/') + '/'

    cmd = 'hadoop fs -ls -R %s | awk \'{print $2" "$8}\'' % src_path
    returncode, filenames, err_data = local_run(cmd)
    filenames = filenames.strip()
    if filenames == '':
        raise Exception('No file in source path')
    else:
        filenames = filenames.split('\n')

    cp_list = []
    for name in filenames:
        detail = name.split()
        if detail[0] == '-' or '$folder$' in detail[1] or crc_file_re.search(detail[1]):
            continue

        add = detail[1][len(src_path):]
        cur_src = detail[1]
        cur_dest = dest_path + add
        cp_list.append((cur_src, cur_dest))

    pool_with_retries(cp_list, __file_hdfs_to_s3__)
    return 0


def __dir_local_to_hdfs__(src_path, dest_path):
    if not os.path.exists(src_path[len(LOCAL):]):
        raise Exception("Error: source path does not exist")
    os.system('hadoop fs -mkdir -p %s' % dest_path)
    cmd_sync = 'hadoop fs -put -f %s/* %s' % (src_path.strip('/')[len(LOCAL):], dest_path)
    returncode, out_data, err_data = local_run(cmd_sync)
    check_ret(returncode, err_data)
    return 0


def __dir_hdfs_to_local__(src_path, dest_path):
    os.system('mkdir -p %s' % dest_path[len(LOCAL):])
    # cannot use get -f (Illegal option) or copyToLocal -f
    # when using cp -f , dest_path should start with file://
    cmd_copy = 'hadoop fs -cp -f %s/* %s' % (src_path.strip('/'), dest_path)
    returncode, out_data, err_data = local_run(cmd_copy)
    check_ret(returncode, err_data)
    return 0


# from directory to directory: src/* --> dest/
def sync(src, dest):
    src = src.replace('s3n://', S3).replace('s3a://', S3)
    dest = dest.replace('s3n://', S3).replace('s3a://', S3)

    src_temp = src.replace(S3, '').replace(HDFS, '').replace(LOCAL, '')
    dest_temp = dest.replace(S3, '').replace(HDFS, '').replace(LOCAL, '')
    if '//' in src_temp or '//' in dest_temp:
        raise Exception("Error: path contains too much //")

    if src.startswith(S3) and dest.startswith(HDFS):
        __dir_s3_to_hdfs__(src, dest)
    elif src.startswith(S3) and dest.startswith(LOCAL):
        s3_utils.s3_sync(src, dest[len(LOCAL):])
    elif src.startswith(HDFS) and dest.startswith(S3):
        __dir_hdfs_to_s3__(src, dest)
    elif src.startswith(HDFS) and dest.startswith(LOCAL):
        __dir_hdfs_to_local__(src, dest)
    elif src.startswith(LOCAL) and dest.startswith(S3):
        s3_utils.s3_sync(src[len(LOCAL):], dest)
    elif src.startswith(LOCAL) and dest.startswith(HDFS):
        __dir_local_to_hdfs__(src, dest)
    else:
        raise Exception("Error: path error!")

    logging.info("sync done!")
    return 0


# from directory to directory: src/* --> dest/
def sync2(src, dest):
    src = src.replace('s3n://', S3).replace('s3a://', S3)
    dest = dest.replace('s3n://', S3).replace('s3a://', S3)

    src_temp = src.replace(S3, '').replace(HDFS, '').replace(LOCAL, '')
    dest_temp = dest.replace(S3, '').replace(HDFS, '').replace(LOCAL, '')
    if '//' in src_temp or '//' in dest_temp:
        raise Exception("Error: path contains too much //")

    local_tmp_dir = '%s%s/%s' % (LOCAL, local_config.LOCAL_TMP_DIR, uuid.uuid4())

    if src.startswith(S3) and dest.startswith(HDFS):
        s3_utils.s3_sync(src, local_tmp_dir[len(LOCAL):])
        __dir_local_to_hdfs__(local_tmp_dir, dest)
    elif src.startswith(S3) and dest.startswith(LOCAL):
        s3_utils.s3_sync(src, dest[len(LOCAL):])
    elif src.startswith(HDFS) and dest.startswith(S3):
        __dir_hdfs_to_local__(src, local_tmp_dir)
        s3_utils.s3_sync(local_tmp_dir[len(LOCAL):], dest)
    elif src.startswith(HDFS) and dest.startswith(LOCAL):
        __dir_hdfs_to_local__(src, dest)
    elif src.startswith(LOCAL) and dest.startswith(S3):
        s3_utils.s3_sync(src[len(LOCAL):], dest)
    elif src.startswith(LOCAL) and dest.startswith(HDFS):
        __dir_local_to_hdfs__(src, dest)
    else:
        raise Exception("Error: path error!")

    os.system('rm -rf %s' % local_tmp_dir[len(LOCAL):])
    logging.info("sync alternative done!")
    return 0

def pool_with_retries(param_list, fun):
    for _ in range(RETRIES):
        pool = Pool(POOL_NUM)
        failure_list = []
        ret_list = pool.map(fun, param_list)
        for idx, ret in enumerate(ret_list):
            if ret != 0:
                failure_list.append(param_list[idx])
        pool.close()
        pool.join()

        param_list = failure_list
        if len(param_list) == 0:
            return 0

    if len(param_list) > 0:
        raise Exception("Error: some tasks failed using func: pool_with_retries")
    return 0


def is_file_exist(fpath):
    fpath = fpath.replace('s3n://', S3).replace('s3a://', S3)
    if fpath.startswith(LOCAL):
        return os.path.exists(fpath[len(LOCAL):])
    elif fpath.startswith(S3):
        return s3_utils.s3_is_file_exist(fpath)
    elif fpath.startswith(HDFS):
        return os.system('hadoop fs -test -e %s' % fpath) == 0
    else:
        raise Exception("Error: path error!")


def rmr(dest):
    dest = dest.replace('s3n://', S3).replace('s3a://', S3)
    if dest.startswith(LOCAL):
        os.system('rm -rf %s' % dest[len(LOCAL):])
    elif dest.startswith(S3):
        s3_utils.s3_rm(dest, recursive=True)
    elif dest.startswith(HDFS):
        os.system('hadoop fs -rm -r -f -skipTrash %s' % dest)
    else:
        raise Exception("Error: path error!")
    return 0

def kinit():
    os.system('kinit -kt %s %s' % (local_config.KNIT, local_config.KEY_NAME))
    
    
# use_no_active_url=True will append "_info_without_act_stat" to url
# thus calling this url will not cause influence on tdau
def token_validation(token_http_urls, token, use_no_active_url=True):
    import requests
    import time
    import module_pb2
    import base64
    from protoc_2_dict import protoc_2_dict
    
    if not token_http_urls or not token:
        return None, None
    
    time.sleep(0.002)
    activation_info = None
    token_server = None
    if use_no_active_url:
        data = json.dumps({"token": token})
        for url in token_http_urls:
            url += '_info_without_act_stat'
            r = requests.post(url, data=data)
            if r.status_code != 200:
                continue
            result = json.loads(r.text)
            if result and 'result' in result:
                s = result['result']
                if 'activation_info' in s:  # need exist "account" filed
                    activation_info = s['activation_info']
                    if 'user_id' not in activation_info and 'user_id' in s:
                        # add user id info
                        activation_info['user_id'] = s['user_id']
                    token_server = url
                    break
    else:
        data = json.dumps([token])
        for url in token_http_urls:
            r = requests.post(url, data=data)
            if r.status_code != 200:
                continue

            result = json.loads(r.text)
            if result:
                s = result[0]
                a = module_pb2.VerifyResult()
                a.ParseFromString(base64.b64decode(s))
                if a.HasField('account'):  # need exist "account" filed
                    activation_info = protoc_2_dict(a.account.activation_info)
                    token_server = url
                    break

    if not activation_info:
        logging.error('%s -- %s' % (token_http_urls, token))
    
    return activation_info, token_server


# return all matching hadoop wildcards parts in dict format
# if 1 wildcard is used, return in format {'smartinput': {}, 'com.eyefilter.night': {}, ...}
# if 2 wildcards are used, return in format {'smartinput': {'20171214': {}, '20171221':{}}, 'com.eyefilter.night': ...}
# only wildcards between slashes are allowed
def find_match_path(path_pattern):
    # slash inside of bracket not allowed like: '{aa/bs,cc/dd}'
    if not path_pattern.startswith(HDFS) and not path_pattern.startswith(LOCAL):
        raise Exception("Error: only hdfs path is supported!")
    else:
        cmd = 'hadoop fs -ls %s | awk \'{print $8}\'' % path_pattern
    returncode, out_data, err_data = local_run(cmd)
    if returncode or (len(err_data) > 0):
        raise Exception(returncode, out_data, err_data)
    file_paths = out_data.strip().split('\n')

    path_pattern_split = path_pattern.split('/')
    diff_dict = {}
    for file in file_paths:
        file_split = file.split('/')
        diff_parent = diff_dict
        for i in range(min(len(path_pattern_split), len(file_split))):
            if file_split[i] != path_pattern_split[i]:
                if file_split[i] not in diff_parent:
                    diff_parent[file_split[i]] = {}
                diff_parent = diff_parent[file_split[i]]

    return diff_dict


# replace all hadoop wildcards part by give strings in order
def apply_path_key(path_pattern, path_key_list):
    # only support string and list
    if isinstance(path_key_list, basestring):
        path_key_list = [path_key_list]
    elif not isinstance(path_key_list, list):
        raise Exception('Unsupported type')
    # hadoop path wildcards characters
    hadoop_chars = set('*?[]{}')
    path_pattern_split = path_pattern.split('/')
    key_idx = 0
    pattern_idx = 0
    while (key_idx < len(path_key_list)) and (pattern_idx < len(path_pattern_split)):
        if any((c in hadoop_chars) for c in path_pattern_split[pattern_idx]):
            path_pattern_split[pattern_idx] = path_key_list[key_idx]
            key_idx += 1
        pattern_idx += 1
    return '/'.join(path_pattern_split)


def is_flag_exist(flag_key, end_date, redis_config):
    import redis
    r = redis.Redis(host=redis_config['host'], port=redis_config['port'], db=redis_config['db'], socket_connect_timeout=30)
    ret = r.hget(flag_key, end_date)
    if type(ret) == bytes:
        ret = ret.decode('utf8')
    if ret == 'True':
        return True
    else:
        logging.info('%s %s not found' % (flag_key, end_date))
        return False


def update_redis_flag(flag_key, end_date, value, redis_config):
    import redis
    r = redis.Redis(host=redis_config['host'], port=redis_config['port'], db=redis_config['db'], socket_connect_timeout=30)
    ret = r.hset(flag_key, end_date, value)
    logging.info('%s %s created/updated with %s' % (flag_key, end_date, value))


def find_activate_source_summary_date(end_date, range_size=8):
    import config
    end_time = datetime.strptime(str(end_date), '%Y%m%d')
    for i in range(range_size):
        tmp_date = (end_time - timedelta(days=i)).strftime('%Y%m%d')
        if all([is_flag_exist(config.PATH_DICT[p].DW_ACTIVATE_SOURCE_SUMMARY_FLAG, tmp_date, config.redis_flag_config) for p in config.PATH_DICT.keys()]):
            logging.info('find activate source summary %s' % (tmp_date))
            return tmp_date, (end_time - timedelta(days=i))
    logging.warn('cant find source summary')
    return None, datetime.strptime('20160930', '%Y%m%d')


def json2db(json_path, table, date, config):
    import MySQLdb
    conn = MySQLdb.connect(**config)
    cur = conn.cursor()
    f = open(json_path)
    revenue_datas = []
    is_column_get = False
    col_names = []
    for line in islice(f, 0, None):
        line_data = json.loads(line)
        if not is_column_get:
            col_names = line_data.keys()
            is_column_get = True
        temp_list = []
        for col_name in col_names:
            temp_list.append(line_data.get(col_name))
        revenue_datas.append(temp_list)
    cur.execute("delete from %s where date = %s" % (table, date))
    columns = ','.join(col_names)
    values_space = ','.join(['%s' for c in col_names])
    cur.executemany("replace into %s(%s) values(%s)"
                    % (table, columns, values_space), revenue_datas)
    f.close()
    cur.close()
    conn.commit()
    conn.close()



# get app referrer path dict
# parameters:
#   product_section: specify product section, like gb
#   path_name: specify path, like MSG__COMMERCIAL_REFERRER_
#   this_day: specify process date, like 20180410
#   app_name_list: app name list
def get_usage_path(product_section, path_name, date_range, app_name_list):
    import config, os_utils
    import MySQLdb
    # basic config
    root_path = config.DW_USAGE
    server_address = '{ap,eu,us,cn}'
    data_format = '{json,parquet}'

    # fetch usage data info from mysql
    ### mysql config
    db_conf = {
        'user': config.MYSQL_APP_INFO['user'],
        'passwd': config.MYSQL_APP_INFO['password'],
        'host': config.MYSQL_APP_INFO['host'],
        'db': config.MYSQL_APP_INFO['db']
    }
    db = MySQLdb.connect(**db_conf)
    cur = db.cursor(MySQLdb.cursors.DictCursor)
    ### query
    table_name = 'app_info_%s' % product_section
    sql = 'SELECT distinct app_path, usage_path from %s where LOWER(product_group) = %s' % (table_name, '%s')
    params = (product_section,)
    cur.execute(sql, params)
    db_usage_info = {}
    for row in cur.fetchall():
        db_usage_info[row['app_path']] = db_usage_info.get(row['app_path']) + [row['usage_path']] if db_usage_info.get(row['app_path']) else [row['usage_path']]
    ### close mysql
    cur.close()
    db.close()

    # get usage info in app_name_list from usage_info
    app_usage_info = {}
    wrong_app_name_list = []
    for app_name in app_name_list:
        if app_name in db_usage_info:
            app_usage_info[app_name] = db_usage_info[app_name]
        else:
            wrong_app_name_list.append(app_name)
    if wrong_app_name_list:
        raise Exception("ERROR: some app can not find usage type: {}".format(','.join(wrong_app_name_list)))

    # find valid usage type set and corresponding data format
    usage_type_set = set(filter(lambda usage_type: bool(usage_type), [','.join(x) for x in app_usage_info.values()]))
    if usage_type_set:
        check_path = os.path.join(root_path, '{%s}' % ','.join(usage_type_set), path_name, data_format, server_address, date_range, '_SUCCESS')
        path_info = os_utils.find_match_path(check_path)
    else:
        path_info = {}

    # return referrer path dict in app_name_list
    path_dict = {}
    for app_name in app_name_list:
        usage_types = [x for x in app_usage_info[app_name] if x in path_info]
        if not usage_types:
            path_dict[app_name] = ''
            continue
        data_formats = []
        for utype in usage_types:
            for upath in path_info[utype].keys():
                data_formats.append(path_info[utype][upath].keys()[0])
        if len(set(data_formats)) == 1: # usage_types data need a same format
            usage_type = '{%s}' % ','.join(usage_types)
            usage_data_format = data_formats[0]
            path_dict[app_name] = os.path.join(root_path, usage_type, path_name, usage_data_format, server_address, date_range)
        else:
            raise Exception("ERROR: usage data {} have diff format -- {}".format(','.join(usage_types), ','.join(data_formats)))
    return path_dict


# get packages from mysql by dates, return the union result
def get_appsflyer_packages(dates):
    import config
    db_conf = {
        'user': config.MYSQL_DATA_CONFIG['user'],
        'passwd': config.MYSQL_DATA_CONFIG['password'],
        'host': config.MYSQL_DATA_CONFIG['host'],
        'db': 'ime_data_config'
    }
    import MySQLdb
    db = MySQLdb.connect(**db_conf)
    results = set()
    with db.cursor(MySQLdb.cursors.DictCursor) as cur:
        for date in dates:
            query = """select t.`package_name` from appsflyer_packages t where t.is_active = 1 and t.start_date <= %s and t.end_date >= %s""" % (date, date)
            cur.execute(query)
            for res in cur.fetchall():
                results.add(res['package_name'])
    return results

# save data content into disk, return file path, file content check code
# parameters:
#   root_path: root path
#   file_name: file name, will add uuid before file extension
#   file_content: file content to write
def save_to_disk(root_path, file_name, file_content):
    name, extension = os.path.splitext(file_name)
    fpath = os.path.join(root_path, '%s.%s%s' % (name, uuid.uuid1(), extension))
    if os.path.exists(fpath):
        raise Exception("ERROR: file %s already exists." % fpath)
    else:
        with open(fpath,'wb') as f:
            # get md5 code from file content
            fcode = hashlib.md5(file_content).hexdigest()
            # write content info to file
            f.write(file_content)
            # flush the internal buffer
            f.flush()
            # force write of file with filedescriptor fd to disk.
            os.fsync(f)
            # close file
            f.close()

    return fpath, fcode

# check file content with given check code, return file content
def check_file_code(fpath, fcode):
    # get file content from fpath
    f = open(fpath,'r')
    file_content = f.read()
    f.close()

    file_check_code = hashlib.md5(file_content).hexdigest()
    if file_check_code != fcode:
        raise Exception("ERROR: input file content's md5 has been modified!")

    return file_content


# get chrome driver
def get_webdriver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    # chrome web driver
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    driver = webdriver.Chrome(chrome_options=chrome_options)

    return driver


# get appsflyer cookie using selenium with given username and password
def get_appsflyer_cookie(driver, username, password):
    # simulate login appsflyer website
    driver.get("https://hq1.appsflyer.com/auth/login")
    driver.find_element_by_xpath(".//*[@type='email']").send_keys(username)
    driver.find_element_by_xpath(".//*[@type='password']").send_keys(password)
    driver.find_element_by_xpath(".//*[@type='submit']").click()
    time.sleep(15)

    # get cookie info
    cookies = [item["name"] + "=" + item["value"] for item in driver.get_cookies()]
    cookiestr = ';'.join(item for item in cookies)
    driver.implicitly_wait(100)

    return cookiestr


# get token from hubble
def get_hubble_token(config, name):
    import urllib2
    import cookielib

    hubble_name = 'facebook_hubble'
    username = config.get(hubble_name, 'username')
    password = config.get(hubble_name, 'password')

    url = 'https://hubble.cootekos.com/hubble/user/'

    cookies = cookielib.CookieJar()
    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookies), urllib2.HTTPHandler())
    body = 'username=%s&password=%s' % (username, password)
    req = urllib2.Request(url+'login', body)
    opener.open(req)

    req = urllib2.Request(url+'user_info')
    tokens_info = json.loads(opener.open(req).read())

    app_id = config.get(name, 'app_id')
    token_info = filter(lambda line: line['app_id'] == app_id, tokens_info['data']['user_platforms'])
    if len(token_info) != 1:
        raise Exception('Hubble token wrong: %s' % token_info)
    access_token = token_info[0]['platform_token']

    return access_token
