#!/usr/bin/env python

import logging
import traceback
import os
import sys
import argparse
import boto3
from datetime import timedelta, date
import subprocess
from logging.handlers import SysLogHandler
from distutils import spawn
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from multiprocessing import cpu_count
from botocore.exceptions import ClientError, ConnectionError
from concurrent import futures
from concurrent.futures import ProcessPoolExecutor as Pool

__author__ = 'ose@recommind.com'


class RetryError(Exception):
    pass


logger = logging.getLogger('rds_log_downloader')
logger.setLevel(logging.DEBUG)
console = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)

syslog = SysLogHandler(address='/dev/log')
syslog.setLevel(logging.DEBUG)
syslog.setFormatter(formatter)
logger.addHandler(syslog)

parallel_processes = int(cpu_count()) + 1
today = date.today()
yesterday = today - timedelta(1)
parser = argparse.ArgumentParser(description='Simplistic RDS logfile '
                                             'downloader')
parser.add_argument('--region', default='us-east-1')
parser.add_argument('--rds-instance', required=True, help='The RDS name')
parser.add_argument('--date', default=today, help='define the date')
parser.add_argument('--email', default=None, help='define the email recipient')
parser.add_argument('--nodl', default=False, help='do not download, because the files are already there')
parser.add_argument('--cron', default=False, help='Only for cron usage, sets date to yesterday')
parser.add_argument('--workdir', default=os.getcwd(), help='Define the working dir')

args = parser.parse_args()
region = args.region
rds_instance = args.rds_instance
pg_badger_path = spawn.find_executable('pgbadger')
email_recipient = args.email
workdir = os.path.join(args.workdir, rds_instance)
fatal_logs = []
pool_state = {}

if not os.path.exists(workdir):
    os.makedirs(workdir, 0755)

if args.cron is False:
    log_date = str(args.date)
else:
    log_date = str(yesterday)

if pg_badger_path is None:
    sys.exit('Please install pgbadger')

logger.debug('Path: {}'.format(str(pg_badger_path)))
cmd = "{} -v -j {} -p '%t:%r:%u@%d:[%p]:' {}/postgresql.log.{}-* -o {}/postgresql.{}.{}.html".format(
    str(pg_badger_path),
    str(parallel_processes),
    str(workdir),
    str(log_date),
    str(workdir),
    str(rds_instance),
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


def download(log_file, token='0'):
    logger.debug('Token = {}'.format(str(token)))
    local_log_file = os.path.join(workdir, str(log_file).replace('error/', ''))
    logger.debug(local_log_file)
    try:
        if token == '0' and os.path.exists(local_log_file):
            logger.debug('Removing old logfile: {}'.format(str(local_log_file)))
            os.remove(local_log_file)
    except IOError as e:
        logger.error('Could not delete file: {}, error : {}'.format(str(local_log_file), str(e.message)))
    retries = 0
    if int(token) > 0:
        pool_state[log_file] += 1
        logger.info('This is retry # {}'.format(str(pool_state[log_file])))
    if pool_state[log_file] > 5:
        fatal_logs.append(log_file)
        logger.fatal('Could not completely download file{}'.format(str(log_file)))

    with open(local_log_file, 'a') as f:
        logger.info('downloading {rds} log file {file}'.format(rds=rds_instance, file=log_file))
        try:
            rds = boto3.client('rds', region)
            response = rds.download_db_log_file_portion(
                DBInstanceIdentifier=rds_instance,
                LogFileName=log_file,
                Marker=token,
                NumberOfLines=5000
            )
            f.write(response['LogFileData'])
            while response['AdditionalDataPending']:
                token = response['Marker']
                response = rds.download_db_log_file_portion(
                    DBInstanceIdentifier=rds_instance,
                    LogFileName=log_file,
                    Marker=token,
                    NumberOfLines=5000
                )
                f.write(response['LogFileData'])
            else:
                if not response['AdditionalDataPending']:
                    logger.debug('file {} completed'.format(str(log_file)))
                    f.write(response['LogFileData'])
                else:
                    logger.error('Response Error: {}'.format(str(response)))
        except Exception as e:
            logger.debug('ExceptionClass download: {}'.format(e.__class__.__name__))
            logger.error('ClientError: {}'.format(str(e.message)))
            raise RetryError(token)


def email_result(recipient, attachment):
    recipient_list = recipient.split(",")
    msg = MIMEMultipart('alternative')
    msg.set_charset('UTF-8')
    msg['Subject'] = 'Pgpadger report from {}'.format(str(log_date))
    msg['From'] = 'ose@recommind.com'
    msg['To'] = recipient
    msg.preamble = 'Multipart message.\r\n'

    # the message body
    text = 'Howdy -- here is the daily PgBadger report from {}for {}.' \
           ' \r\n Could not download {}'.format(str(log_date), str(rds_instance), str(fatal_logs))

    html = """\
    <html>
      <head>Howdy -- here is the daily PgBadger report from {}</head>
      <body>
        <p>For db {}.<br>
        <p>Could not download {}<br>
           <br>

        </p>
      </body>
    </html>
    """.format(str(log_date), str(rds_instance), str(fatal_logs))

    part1 = MIMEText(text, 'plain', 'utf-8')
    part2 = MIMEText(html, 'html', 'utf-8')
    msg.attach(part1)
    msg.attach(part2)

    part3 = MIMEApplication(open(attachment, 'rb').read())
    part3.add_header('Content-Disposition', 'attachment',
                     filename='postgresql.{}.{}.html'.format(str(rds_instance), str(log_date)))
    msg.attach(part3)
    try:
        ses = boto3.client('ses', region)
        response = ses.send_raw_email(
            Source=msg['From'],
            Destinations=recipient_list,
            RawMessage={'Data': msg.as_string()}
        )
        logger.info('Email send:'.format(str(response)))
        logger.debug('email:'.format(str(msg.as_string())))

    except ClientError as e:
        logger.error('Could not sent email: {}'.format(str(e.message)))


def run():
    if not args.nodl:
        logger.info('Running parallel log file download on {} cores with {} processes'.format(str(cpu_count()),
                                                                                              str(parallel_processes)
                                                                                              )
                    )
        logfiles = list_rds_log_files()
        try:
            with Pool(max_workers=int(parallel_processes * 3)) as executor:
                logfile_future = dict((executor.submit(download, logfile), logfile)
                                      for logfile in logfiles)
                for future in futures.as_completed(logfile_future):
                    file_result = logfile_future[future]
                    if future.exception() is not None:
                        logger.error('{} failed with an Exception: {}.'.format(file_result, future.exception()))
                        logger.info('Retrying, time : {}, token: {}'.format(str(file_result), str(future.exception())))
                        if future.exception() == 'fatal':
                            logger.fatal('{} will be skipped'.format(str(file_result)))
                        else:
                            logger.warn('Retry for {}'.format(str(file_result)))
                            executor.submit(download, file_result, str(future.exception()))
                    else:
                        logger.info('{} done'.format(str(file_result)))
        except Exception as e:
            logger.error('{}. Exception class: {}. Traceback: {}'.format(str(e.message),
                                                                         str(e.__class__.__name__),
                                                                         str(traceback.print_stack())
                                                                         )
                         )
    else:
        logger.info('nodl switch used, proceed with analysis')


def run_external_cmd(commandline):
    logger.debug('Commandline: {}'.format(str(commandline)))
    shell = subprocess.Popen(commandline, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    while True:
        out = shell.stderr.read(1)
        if out == '' and shell.poll() is not None:
            break
        if out != '':
            sys.stdout.write(out)
            sys.stdout.flush()


if __name__ == '__main__':

    try:
        try:
            run()
            logger.info('Proceeding with analysis')
            run_external_cmd(cmd)
            if args.email is None:
                logger.info('No recipient, no email')
            else:
                email_result(email_recipient,
                             os.path.join(workdir, 'postgresql.{}.{}.html'.format(str(rds_instance), str(log_date))))

        except Exception as e:
            logger.exception('ups: {}'.format(str(e.message)))
    except KeyboardInterrupt:
        sys.exit(2)
