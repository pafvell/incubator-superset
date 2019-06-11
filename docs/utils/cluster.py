#!/usr/bin/env python
#coding:utf8

import json
import sys
import os
import logging
import time
import subprocess
import re
from datetime import datetime
import uuid

import config
import os_utils
import s3_utils

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import local_config

class Cluster:
    CLUSTER_STATUS_STARTING = 1
    CLUSTER_STATUS_READY = 2
    CLUSTER_STATUS_TERMINATE = 3

    def __init__(self, task_name):
        self.task_name = task_name
        self.output_replace_dict = {}

    def run_task_cmd(self, cmd, output_path=[], rm_dest=True, realtime_log_output=False):
        for path in output_path:
            new_path = self.__new_output_path__(path)
            if new_path and new_path != path:
                if path in self.output_replace_dict and self.output_replace_dict[path] != new_path:
                    raise Exception('new path conflists: %s' % path)
                self.output_replace_dict[path] = new_path

        for k,v in self.output_replace_dict.items():
            if len(re.findall(k, cmd)) != 1:
                raise Exception('ERROR: output_path has conflics with cmd: %s -- %s' % (k, cmd))
            cmd = cmd.replace(k, v)

        returncode, out_data, err_data = os_utils.local_run(cmd, realtime_log_output=realtime_log_output)
        if not realtime_log_output:
            logging.error(err_data)
        if returncode != 0:
            if not realtime_log_output:
                logging.info(out_data)
            raise Exception('ERROR: run_task_cmd: %s' % (cmd))

        for k,v in self.output_replace_dict.items():
            self.__clean_output_path__(v)
            if rm_dest:
                os_utils.rmr(k)
            returncode1  = os_utils.sync(v, k)
            if returncode1 != 0:
                raise Exception('ERROR: sync failed: %s -- %s' % (v, k))

        return returncode, out_data, err_data

    def __new_output_path__(self, path):
        return None

    def __clean_output_path__(self, path):
        pass

    def close(self):
        pass

class ClusterEmr(Cluster):
    def __init__(self, task_name='lin_corpus',
                 auto_terminate=True, key_name=local_config.KEY_NAME, log_path='user.cootek/linzou/log',
                 cnt=8, bid=True, debug=False, pem_path=local_config.PEM_PATH, region_code='us', reused_cluster_id=None):
        Cluster.__init__(self, task_name=task_name)

        self.region_code = region_code
        self.region = config.CODE2REGION[region_code]

        if bid:
            bids = 'BidPrice=0.4,'
        else:
            bids = ''
        master_type = 'InstanceGroupType=MASTER,InstanceCount=1,%sInstanceType=m3.xlarge' % bids
        #core_type = 'InstanceGroupType=CORE,InstanceCount=%d,%sInstanceType=c3.4xlarge' % (cnt, bids)
        core_type = 'InstanceGroupType=CORE,InstanceCount=%d,%sInstanceType=m3.2xlarge' % (cnt, bids)

        if debug:
            debug = '--enable-debugging'
        else:
            debug = ''

        if not region_code or region_code not in config.CODE2SUBNET:
            logging.error('cluster: illegal region: %s' % (region_code))
            return None
        region_prefix = '%s.'%(config.CODE2REGION[region_code]) if region_code != 'us' else ''
        #--log-uri s3://%s
        command = 'aws emr create-cluster --service-role EMR_DefaultRole --ec2-attributes KeyName=%s,SubnetId=%s,InstanceProfile=EMR_EC2_DefaultRole --region %s --name %s %s --release-label emr-5.5.0 \
        --instance-groups %s %s \
        --applications Name=GANGLIA Name=Pig Name=Spark Name=Zeppelin --configurations file://%s/cluster_conf.json \
        --bootstrap-action Path=s3://user.cootek/imecore/data/bootstrap/install_python_modules.sh' \
        % (key_name, config.CODE2SUBNET[region_code], config.CODE2REGION[region_code], task_name, debug, master_type, core_type, os.path.dirname(__file__))
        if auto_terminate:
            command += ' --auto-terminate'
        self.auto_terminate = auto_terminate

        if reused_cluster_id:
            self.clusterid = reused_cluster_id
        else:
            d = self.__create_cluster__(command)
            self.clusterid = d['ClusterId']

        self.pem_path = pem_path

        self.master = None


    def get_cluster_id(self):
        return self.clusterid

    def get_cluster_status(self):
        f = subprocess.Popen("aws emr describe-cluster --cluster-id %s --region %s" % (self.clusterid, self.region), shell=True, stdout=subprocess.PIPE).stdout
        content = f.read()
        try:
            d = json.loads(content)
        except:
            return None

        status = d['Cluster']['Status']['State']
        if 'TERMINAT' in status:
            return Cluster.CLUSTER_STATUS_TERMINATE
        elif status == 'RUNNING' or status == 'WAITING':
            return Cluster.CLUSTER_STATUS_READY
        else:
            return Cluster.CLUSTER_STATUS_STARTING


    def add_streaming_steps(self, jsonname):
        d = self.__run_command__("aws emr add-steps --cluster-id %s --region %s --steps file://./%s" % (self.clusterid, self.region, jsonname))
        return d['StepIds'][0]

    def wait_step_finish(self, step_id):
        ret = None
        while 1:
            time.sleep(60)
            d = self.__run_command__('aws emr describe-step --cluster-id %s --region %s --step-id %s' % (self.clusterid, self.region, step_id))
            state = d['Step']['Status']['State']
            if state == 'COMPLETED':
                ret = True
                break
            elif state == 'FAILED' or state == 'CANCELLED':
                ret = False
                break

        return ret

    def run_task_cmd(self, cmd, output_path = [], files=[]):
        while True:
            status = self.get_cluster_status()
            if status == Cluster.CLUSTER_STATUS_TERMINATE:
                sys.stderr.write('error: cluster terminated\n')
                return
            elif status == Cluster.CLUSTER_STATUS_READY:
                self.master = self.__get_master_node__()
                if not self.master:
                    sys.stderr.write('error: can\'t get master address')
                    return
                for copy_file in files:
                    for line in subprocess.Popen("scp -i %s -o StrictHostKeyChecking=no %s hadoop@%s:~/" % (self.pem_path, copy_file, self.master), shell=True, stdout=subprocess.PIPE).stdout:
                        sys.stdout.write(line)
                for line in subprocess.Popen("ssh -i %s -o StrictHostKeyChecking=no hadoop@%s %s" % (self.pem_path, self.master, cmd), shell=True, stdout=subprocess.PIPE).stdout:
                    sys.stdout.write(line)
                return
            elif status == Cluster.CLUSTER_STATUS_STARTING:
                sys.stdout.write('cluster starting\n')
                time.sleep(30)
                continue
            else:
                sys.stderr.write('error: can\'t get cluster status, exit!!')
                return

    def distcp(self, src, dest, srcPattern=None):
        #this command will overwirte the dest
        #cmd = 'hadoop jar /home/hadoop/lib/emr-s3distcp-1.0.jar --src %s --dest %s' % (src, dest)
        cmd = 's3-dist-cp --src %s --dest %s' % (src, dest)
        if srcPattern:
            cmd += ' --srcPattern %s' % (srcPattern)
        self.run_from_master(cmd)

    def close(self):
        if not self.auto_terminate:
            self.__run_command__('aws emr terminate-clusters --cluster-ids %s --region %s' % (self.clusterid, self.region))


    def __run_command__(self, command):
        logging.info(command)
        ret, out, err = os_utils.local_run(command)
        if ret != 0:
            raise Exception('ERROR: %s -- %s -- %s' % (command, ret, err))
        if out:
            d = json.loads(out)
            logging.info(d)
        else:
            d = None
        return d

    def __create_cluster__(self, cmd):
        d = None
        restart_cnt = 0
        while not d:
            d = self.__run_command__(cmd)
            check_cnt = 0
            d2 = None
            while not d2 and check_cnt <= 10:
                time.sleep(2*60)
                f = subprocess.Popen("aws emr describe-cluster --cluster-id %s --region %s" % (d['ClusterId'], self.region), shell=True, stdout=subprocess.PIPE).stdout
                try:
                    d2 = json.loads(f.read())
                except Exception as e:
                    check_cnt += 1
                    d2 = None
                    continue
            if check_cnt > 10:
                logging.error('error: start cluster failed: check cnt exceeded')
                self.close()
                return None
            if d2['Cluster']['Status']['State'] == 'TERMINATED_WITH_ERRORS' and 'StateChangeReason' in d2['Cluster']['Status'] and d2['Cluster']['Status']['StateChangeReason']['Code'] == 'VALIDATION_ERROR':
                d = None
                restart_cnt += 1
                if restart_cnt >= 4:
                    break
                time.sleep(1200*restart_cnt)
        return d

    def __get_master_node__(self):
        f = subprocess.Popen("aws emr describe-cluster --cluster-id %s --region %s" % (self.clusterid, self.region), shell=True, stdout=subprocess.PIPE).stdout
        content = f.read()
        try:
            d1 = json.loads(content)
        except:
            logging.error('__get_master_node__, describe-cluster')
            return None
        if 'MasterPublicDnsName' in d1['Cluster'] and d1['Cluster']['MasterPublicDnsName']:
            return d1['Cluster']['MasterPublicDnsName']

        master_id = None
        for ins in d1['Cluster']['InstanceGroups']:
            if ins['InstanceGroupType'] == 'MASTER':
                master_id = ins['Id']
                break
        if not master_id:
            logging.error('__get_master_node__, master-id')
            return None

        master_node = None
        f = subprocess.Popen("aws emr list-instances --cluster-id %s --region %s" % (self.clusterid, self.region), shell=True, stdout=subprocess.PIPE).stdout
        content = f.read()
        try:
            d2 = json.loads(content)
        except:
            logging.error('__get_master_node__, list-instances')
            return None
        for ins in d2['Instances']:
            if ins['InstanceGroupId'] == master_id:
                master_node = ins['PublicIpAddress']
                break
        if not master_node:
            logging.error('__get_master_node__, master-node')
            return None

        return master_node


class ClusterLocal(Cluster):
    def __init__(self, task_name):
        Cluster.__init__(self, task_name=task_name)
        self.new_output_path = []

    #if output to s3, must set output_path
    def __new_output_path__(self, path):
        this_date = datetime.today().strftime('%Y%m%d')
        if path.startswith('s3://') or path.startswith('s3n://'):
            local_path = 'file://' + os.path.join(local_config.LOCAL_TMP_DIR, self.task_name + this_date, str(uuid.uuid4()))
            self.new_output_path.append(local_path)
            return local_path
        else:
            return None

    def __clean_output_path__(self, path):
        if path.startswith('file://'):
            os_utils.local_run('find %s -name ".*.crc" -delete' % (path.replace('file://', '')))

    def close(self):
        for path in self.new_output_path:
            os_utils.rmr(path)

class ClusterHadoop(Cluster):
    def __init__(self, task_name):
        Cluster.__init__(self, task_name=task_name)
        os_utils.kinit()
        self.new_output_path = []

    #if output to local, must first output to hdfs, then copy back
    def __new_output_path__(self, path):
        if path.startswith('file://'):
            this_date = datetime.today().strftime('%Y%m%d')
            hdfs_path = os.path.join(config.TMP_GB, 'imecore', self.task_name + this_date, str(uuid.uuid4()))
            self.new_output_path.append(hdfs_path)
            return hdfs_path
        elif path.startswith('s3://') or path.startswith('s3n://'):
            raise Exception('s3 path not allowed in ClusterHadoop')
        else:
            return None

    def close(self):
        for path in self.new_output_path:
            os_utils.rmr(path)


