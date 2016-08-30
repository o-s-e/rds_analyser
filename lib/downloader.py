#!/usr/bin/env python

import logging
import os.path
import sys
import argparse
import boto3
import time
import subprocess
from distutils import spawn
from multiprocessing import cpu_count
from botocore.exceptions import ClientError, ConnectionError
from concurrent import futures
from concurrent.futures import ProcessPoolExecutor as Pool

__author__ = 'ose@recommind.com'

logger = logging.getLogger('rds_log_downloader')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

parallel_processes = int(cpu_count()) + 1
today = time.strftime("%Y-%m-%d")

parser = argparse.ArgumentParser(description='Simplistic RDS logfile '
                                             'downloader')
parser.add_argument('--region', default='us-east-1')
parser.add_argument('--rds-instance', required=True, help='The RDS name')
parser.add_argument('--date', default=today, help='define the date')

args = parser.parse_args()
region = args.region
rds_instance = args.rds_instance
date = args.date
pg_badger_path = spawn.find_executable('pgbadger')

if pg_badger_path is None:
    sys.exit('Please install pgbadger')

logger.debug('Path: {}'.format(str(pg_badger_path)))
cmd = "/usr/bin/pgbadger -v -j {} -p '%t:%r:%u@%d:[%p]:' postgresql.log.{}.*  -o postgresql.{}.html".format(
    str(parallel_processes), str(date), str(date))


def list_rds_log_files():
    try:
        rds = boto3.client('rds', region)
    except ClientError as e:
        logger.error(e)
    try:
        response = rds.describe_db_log_files(
            DBInstanceIdentifier=rds_instance,
            FilenameContains=date)
        logger.debug('RDS logfiles dict: {}'.format(str(response)))
        logfile_list = map(lambda d: d['LogFileName'], response['DescribeDBLogFiles'])
        logger.debug('logfile list: {}'.format(str(logfile_list)))
        return logfile_list
    except ClientError as e:
        logger.error(e)
        sys.exit(2)


def download(log_file):
    local_log_file = os.path.basename(log_file)
    try:
        rds = boto3.client('rds', region)
    except ClientError as e:
        logger.error(e)
    with open(local_log_file, 'w') as f:
        logger.info('downloading {rds} log file {file}'.format(rds=rds_instance, file=log_file))
        token = '0'
        # logger.debug('Logfile: {}. Init token: {}'.format(str(log_file), str(token)))
        try:
            response = rds.download_db_log_file_portion(
                DBInstanceIdentifier=rds_instance,
                LogFileName=log_file,
                Marker=token)
            f.write(response['LogFileData'])
            while response['AdditionalDataPending']:
                try:
                    token = response['Marker']
                    # logger.debug('Logfile: {}. Response token: {}'.format(str(log_file), str(token)))
                    response = rds.download_db_log_file_portion(
                        DBInstanceIdentifier=rds_instance,
                        LogFileName=log_file,
                        Marker=token)
                    f.write(response['LogFileData'])
                except ConnectionError as e:
                    logger.debug('Last token during exception: {}'.format(str(token)))
                    continue
            else:
                if not response['AdditionalDataPending']:
                    logger.info('file {} completed'.format(str(log_file)))
                else:
                    logger.error('Response sucks: {}'.format(str(response)))

        except ClientError as e:
            logger.error(e)
            sys.exit(2)


if __name__ == '__main__':

    logger.info('Running parallel rds log file download on {} cores with {} processes'.format(
        str(cpu_count()), str(parallel_processes)))
    try:
        logfiles = list_rds_log_files()
        with Pool(max_workers=int(parallel_processes)) as executor:
            logfile_future = dict((executor.submit(download, logfile), logfile)
                                  for logfile in logfiles)
            for future in futures.as_completed(logfile_future):
                file_result = logfile_future[future]
                if future.exception() is not None:
                    logger.error('{} generated an Exception: {}. class: {}'.format(file_result, future.exception()),
                                 future.exception().__class__.__name__)
                else:
                    logger.info('done')

        logger.info('Downloading logs finished. Proceeding with analysis')
        logger.debug('Commandline: {}'.format(str(cmd)))
        pg_badger = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
        while True:
            out = pg_badger.stderr.read(1)
            if out == '' and pg_badger.poll() is not None:
                break
            if out != '':
                sys.stdout.write(out)
                sys.stdout.flush()


    except Exception as e:
        logger.error('ups: {}'.format(str(e.message)))
