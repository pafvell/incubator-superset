import sys
import os
import subprocess
import time
import logging

MAX_RETRY_CNT = 10
DEFAULT_WAIT_TIME = 600

CODE2REGION = {'us':'us-east-1', 'eu':'eu-central-1', 'ap':'ap-southeast-1', 'appsflyer': 'eu-west-1'}

SUCCESS_RETURN_CODE = set([0])

def get_region_code_from_fname(fname):
    region_code = None
    if fname:
        if fname.startswith('s3://af-ext-raw-data/cootek-gmail-com/'):
            region_code = 'appsflyer'
        elif fname.startswith('s3://ap.'):
            region_code = 'ap'
        elif fname.startswith('s3://eu.'):
            region_code = 'eu'
        elif fname.startswith('s3://'):
            region_code = 'us'
    return region_code

def run_cmd(cmd, retry_cnt = MAX_RETRY_CNT, must_suc = False, wait_time = DEFAULT_WAIT_TIME, sucess_ret = SUCCESS_RETURN_CODE):
    #print(cmd)
    cnt = 0
    while 1:
        cnt += 1
        popen = subprocess.Popen(cmd, shell = True, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        out_data, err_data = [it.strip() for it in popen.communicate()]
        if err_data:
            if type(err_data) == bytes:
                err_data = err_data.decode('utf8').strip()
            logging.error('%s: %s' % (cmd, err_data))
            return False
        if type(out_data) == bytes:
            out_data = out_data.decode('utf8').strip()
        if popen.returncode in sucess_ret:
        #if popen.returncode == 0 and out_data:
            return True
        if cnt >= retry_cnt:
            break
        time.sleep(wait_time)
        logging.warning(cmd)

    return False

def gen_cmd(cmd_type, src, dest, recursive = False):
    src_region_code = get_region_code_from_fname(src)
    dest_region_code = get_region_code_from_fname(dest)
    cmd = 'aws s3 %s' % (cmd_type)
    if src:
        cmd += ' %s' % (src)
    if dest:
        cmd += ' %s' % (dest)
    if recursive:
        cmd += ' --recursive'
    if src_region_code and dest_region_code:
        cmd += ' --source-region %s' % (CODE2REGION[src_region_code])
        cmd += ' --region %s' % (CODE2REGION[dest_region_code])
    elif dest_region_code:
        cmd += ' --region %s' % (CODE2REGION[dest_region_code])
    elif src_region_code:
        cmd += ' --region %s' % (CODE2REGION[src_region_code])

    return cmd

def s3_is_file_exist(fpath):
    region_code = get_region_code_from_fname(fpath)
    if region_code:
        cmd = 'aws s3 ls %s --region %s' % (fpath, CODE2REGION[region_code])
        return run_cmd(cmd, retry_cnt = 1)
    elif os.path.exists(fpath):
        return True
    else:
        return False

def s3_wait_file_ready(fpath, timeout=None):
    cnt = 0
    if timeout:
        wait_time = int(timeout) / MAX_RETRY_CNT
    else:
        wait_time = DEFAULT_WAIT_TIME
    while not s3_is_file_exist(fpath):
        cnt += 1
        if cnt >= MAX_RETRY_CNT:
            return False
        time.sleep(wait_time)
        logging.warning(fpath)

    return True

def s3_wait_file_success(fpath, timeout=None):
    return s3_wait_file_ready(fpath + '_SUCCESS', timeout)

def s3_cp(src, dest, recursive = False):
    if not s3_is_file_exist(src):
        return False
    cmd = gen_cmd('cp', src, dest, recursive)
    ret = run_cmd(cmd)
    return ret

def s3_sync(src, dest):
    if not s3_is_file_exist(src):
        return False
    cmd = gen_cmd('sync', src, dest)
    sync_suc_ret = set([0,2])
    ret = run_cmd(cmd, sucess_ret=sync_suc_ret)
    return ret

def s3_mv(src, dest, recursive = False):
    if not s3_is_file_exist(src):
        return False
    cmd = gen_cmd('mv', src, dest, recursive)
    ret = run_cmd(cmd)
    return ret

def s3_rm(src, recursive = False):
    if not s3_is_file_exist(src):
        return False
    src_region_code = get_region_code_from_fname(src)
    cmd = 'aws s3 rm %s' % (src)
    if recursive:
        cmd += ' --recursive'
    if src_region_code:
        cmd += ' --region %s' % (CODE2REGION[src_region_code])
    ret = run_cmd(cmd)
    return ret

