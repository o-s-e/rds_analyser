#!/usr/bin/env python

import logging
import os.path
import sys
import argparse
import boto3
import time
import subprocess
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
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
parser.add_argument('--email', default=None, help='define the email recipient')
parser.add_argument('--nodl', default=False, help='do not download, because the files are already there')

args = parser.parse_args()
region = args.region
rds_instance = args.rds_instance
log_date = args.date
pg_badger_path = spawn.find_executable('pgbadger')
email_recipient = args.email
if pg_badger_path is None:
    sys.exit('Please install pgbadger')

logger.debug('Path: {}'.format(str(pg_badger_path)))
cmd = "{} -v -j {} -p '%t:%r:%u@%d:[%p]:' postgresql.log.{}.* -o postgresql.{}.html".format(str(pg_badger_path),
                                                                                            str(parallel_processes),
                                                                                            str(log_date),
                                                                                            str(log_date))


def list_rds_log_files():
    try:
        rds = boto3.client('rds', region)
    except ClientError as e:
        logger.error(e)
    try:
        response = rds.describe_db_log_files(
            DBInstanceIdentifier=rds_instance,
            FilenameContains=log_date)
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


def email_result(recipient, attachment):
    msg = MIMEMultipart()
    msg['Subject'] = 'Pgpadger report from {}'.format(str(log_date))
    msg['From'] = 'ose@recommind.com'
    msg['To'] = recipient

    msg.preamble = 'Multipart message.\n'

    # the message body
    part = MIMEText('Howdy -- here is the daily PgBadger report from {}.'.format(str(log_date)))
    msg.attach(part)

    part = MIMEApplication(open(attachment, 'rb').read())
    part.add_header('Content-Disposition', 'attachment', filename='postgresql.{}.html'.format(str(log_date)))
    msg.attach(part)

    try:
        ses = boto3.client('ses', region)
        response = ses.send_raw_email(
            Source='ose@recommind.com',
            Destination={'ToAddresses':
                             [recipient]
                         },
            RawMessage={'Data': msg}

        )
        logger.info('Email send: {}'.format(str(response)))

    except ClientError as e:
        logger.error('Could not sent email: {}'.format(str(e.message)))


if __name__ == '__main__':

    try:
        logger.info('Running parallel rds log file download on {} cores with {} processes'.format(
            str(cpu_count()), str(parallel_processes)))

        if args.nodl:
            logfiles = list_rds_log_files()
            try:
                with Pool(max_workers=int(parallel_processes * 2)) as executor:
                    logfile_future = dict((executor.submit(download, logfile), logfile)
                                          for logfile in logfiles)
                    for future in futures.as_completed(logfile_future):
                        file_result = logfile_future[future]
                        if future.exception() is not None:
                            logger.error(
                                '{} generated an Exception: {}. class: {}'.format(file_result, future.exception()),
                                future.exception().__class__.__name__)
                        else:
                            logger.info('done')
            except Exception as e:
                logger.error(
                    'something got wrong: {}. Exceptionclass: {}'.format(str(e.message), str(e.__class__.__name__)))
        else:
            logger.info('nodl switch used, proceed with analysis')

            logger.info('Downloading logs finished. Proceeding with analysis')
            logger.debug('Commandline: {}'.format(str(cmd)))
            pg_badger = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            while True:
                out = pg_badger.stderr.read(1)
                if out == '' and pg_badger.poll() is not None:
                    break
                if out != '':
                    sys.stdout.write(out)
                    sys.stdout.flush()

            email_result(email_recipient, 'postgresql.{}.html'.format(str(log_date)))

    except Exception as e:
        logger.error('ups: {}'.format(str(e.message)))
