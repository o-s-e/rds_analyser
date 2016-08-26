#!/usr/bin/env python

from __future__ import print_function

import argparse
import os.path
import sys

import boto3
from botocore.exceptions import NoRegionError, ClientError

__author__ = 'ose@recommind.com'

parser = argparse.ArgumentParser(description='Simplistic RDS logfile '
                                             'downloader')
parser.add_argument('--region', default='us-east-1')
parser.add_argument('--rds-instance', required=True, help='The RDS name')
parser.add_argument('--rds-log-file', required=True, help='The log file to '
                                                          'download')
parser.add_argument('--local-log-file', help='The path on the local file '
                                             'system for the log file. Default basename of the '
                                             'rds-log-file')

args = parser.parse_args()
region = args.region
rds_instance = args.rds_instance
log_file = args.rds_log_file

if args.local_log_file:
    local_log_file = args.local_log_file
else:
    local_log_file = os.path.basename(log_file)

try:
    rds = boto3.client('rds', region)
except NoRegionError:
    rds = boto3.client('rds', 'us-east-1')

with open(local_log_file, 'w') as f:
    print('downloading {rds} log file {file}'.format(rds=rds_instance, file=log_file))
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
        print(e)
        sys.exit(2)
