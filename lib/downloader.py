#!/usr/bin/env python

import logging
import os.path
import sys
import argparse
import boto3
import time
from botocore.exceptions import NoRegionError, ClientError
from concurrent.futures import ProcessPoolExecutor

__author__ = 'ose@recommind.com'

logger = logging.getLogger('rds_log_downloader')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

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


def list_rds_log_files():
    try:
        rds = boto3.client('rds', region)
    except NoRegionError:
        logger.warn('Using default region: {}'.format(region))
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
    except NoRegionError:
        logger.warn('Using default region: {}'.format(region))
        rds = boto3.client('rds', region)
    with open(local_log_file, 'w') as f:
        logger.info('downloading {rds} log file {file}'.format(rds=rds_instance, file=log_file))
        token = '0'
        try:
            response = rds.download_db_log_file_portion(
                DBInstanceIdentifier=rds_instance,
                LogFileName=log_file,
                Marker=token)
            f.write(response['LogFileData'])
            while response['AdditionalDataPending']:
                token = response['Marker']
                response = rds.download_db_log_file_portion(
                    DBInstanceIdentifier=rds_instance,
                    LogFileName=log_file,
                    Marker=token)
                f.write(response['LogFileData'])
        except ClientError as e:
            logger.error(e)
            sys.exit(2)


if __name__ == '__main__':
    try:
        logfiles = list_rds_log_files()
        executor = ProcessPoolExecutor(5)
        futures = [executor.submit(download(logfile) for logfile in logfiles)]

    except Exception as e:
        logger.error('ups: {}'.format(str(e.message)))
