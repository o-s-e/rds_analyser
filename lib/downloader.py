#!/usr/bin/env python

from __future__ import print_function
import logging
import os.path
import sys
import argparse
import boto3
import time
from botocore.exceptions import NoRegionError, ClientError

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


def list_rds_log_files(rds_instance, region, date):
    default_region = 'us-east-1'
    try:
        rds = boto3.client('rds', region)
    except NoRegionError:
        logger.warn('Using default region: {}'.format(default_region))
    try:
        response = rds.describe_db_log_files(
            DBInstanceIdentifier=rds_instance,
            FilenameContains=date)
        logger.debug('RDS logfiles dict: {}'.format(str(response)))
    except ClientError as e:
        logger.error(e)
        sys.exit(2)


def download(log_file, rds_instance, region):
    local_log_file = os.path.basename(log_file)
    default_region = 'us-east-1'
    try:
        rds = boto3.client('rds', region)
    except NoRegionError:
        logger.warn('Using default region: {}'.format(default_region))
        rds = boto3.client('rds', default_region)
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
        list_rds_log_files(args.rds_instance, args.date)
    except Exception as e:
        logger.error('ups: {}'.format(str(e.message)))
