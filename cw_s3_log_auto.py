import json
import boto3
import time
import math
from datetime import datetime, timedelta, timezone
import logging
import os
from botocore.exceptions import ClientError

import pprint


logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler():
    region = 'ap-northeast-2'
    log_file = boto3.client('logs')
    log_group = 'API-Gateway-Execution-Logs_4vylxak9bk/test'
    KST = datetime.now() + timedelta(hours=9)
    nDays = 4
    deletionDate = KST - timedelta(days=nDays)
    startOfDay = deletionDate.replace(hour=0, minute=0, second=0, microsecond=0)
    endOfDay = deletionDate.replace(hour=23, minute=59, second=59, microsecond=999999)
    # file_name = '/tmp/' + str(deletionDate.strftime('%Y-%m-%d')) + '.json'
    file_name = str(deletionDate.strftime('%Y-%m-%d')) + '.json'
    # object_name = str(deletionDate.strftime('%Y-%m-%d')) + '/' + str(deletionDate.strftime('%Y-%m-%d')) + '.json'
    object_name = file_name
    bucket = 'agw-logs'
    print('object_name happy new year:             ', object_name)
    print('file_name:                   ', file_name)
    print('time now:                 ', KST)
    print('start of day:     ', startOfDay)
    print('end of day:       ', endOfDay)
    
    # cloudwatch
    resp = log_file.filter_log_events(
        logGroupName=log_group, 
        startTime=math.floor(startOfDay.timestamp() * 1000), 
        endTime=math.floor(endOfDay.timestamp() * 1000), 
        )
    
    for i in resp['events']:
        # print(datetime.strptime(i, '%Y%m%d'))
        i['timestamp'] = datetime.fromtimestamp(i['timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        i['ingestionTime'] = datetime.fromtimestamp(i['ingestionTime'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
    
    json_val = json.dumps(resp['events'], separators=(',', ':'))

    with open(file_name, 'w') as sample:
        sample.write(json_val)
    
    v1 = '[{'
    v1_new = '{'
    v2 = '}]'
    v2_new = '}'
    v3 = '},'
    v3_new = '},\n'

    with open(file_name, 'r') as file:
        content = file.read()
        new1 = content.replace(v1, v1_new)
        
    with open(file_name, 'w') as new_file1:
        new_file1.write(new1)

    with open(file_name, 'r') as file2:
        content2 = file2.read()
        new2 = content2.replace(v2, v2_new)

    with open(file_name, 'w') as new_file2:
        new_file2.write(new2)
    
    with open(file_name, 'r') as file3:
        content3 = file3.read()
        new3 = content2.replace(v3, v3_new)

    with open(file_name, 'w') as new_file3:
        new_file3.write(new3)

    
    # s3         
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

    #log
    logger.info('## ENVIRONMENT VARIABLES')
    logger.info(os.environ)
    logger.info('## EVENT')
    # logger.info(event)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

lambda_handler()