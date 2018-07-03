"""
Created on Nov 10 2017

@author: lgarzio
@brief: This script is used to push new annotations or update existing annotations from a csv to uFrame via the M2M API
@usage:
anno_csv: csv with annotations to push to uFrame with headers: id,subsite(r),node,sensor,stream,method,parameters,
            beginDate(r),endDate,beginDT,endDT,exclusionFlag(r),qcFlag,source,annotation(r): (r) = required
source: email address to associate with annotation
username: username to access the OOI API
token: password to access the OOI API
url: annotation endpoint
"""

import requests
import json
import ast
from datetime import datetime
import netCDF4 as nc
import pandas as pd
import numpy as np

anno_csv = '/Users/lgarzio/Documents/OOI/Annotations/new_annotations.csv'
source = 'lgarzio@marine.rutgers.edu'

# production
username = 'username'
token = 'token'
url = 'https://ooinet.oceanobservatories.org/api/m2m/12580/anno/'

session = requests.session()

# ooinet-dev-01
# username = 'username'
# token = 'token'
# url = 'https://ooinet-dev-01.oceanobservatories.org/api/m2m/12580/anno/'


def check_dates(beginDate,endDate):
    begin_DT = datetime.strptime(beginDate,'%Y-%m-%dT%H:%M:%SZ')
    beginDT = int(nc.date2num(begin_DT,'seconds since 1970-01-01')*1000)
    try:
        end_DT = datetime.strptime(endDate,'%Y-%m-%dT%H:%M:%SZ')
        endDT = int(nc.date2num(end_DT,'seconds since 1970-01-01')*1000)
        if endDT >= beginDT:
            return beginDT, endDT
        else:
            raise Exception('beginDate (%s) is after endDate (%s)' %(begin_DT,end_DT))
    except ValueError:
        endDT = ''
        return beginDT, endDT


def check_exclusionFlag(exclusionFlag):
    if exclusionFlag:
        exclusionFlag = 1
    else:
        exclusionFlag = 0
    return exclusionFlag


def check_qcFlag(qcFlag):
    qcFlag_list = ['','not_operational','not_available','pending_ingest','not_evaluated','suspect','fail','pass']
    if qcFlag in qcFlag_list:
        return qcFlag
    else:
        raise Exception('Invalid qcFlag: %s' %qcFlag)


df = pd.read_csv(anno_csv)
df = df.replace(np.nan, '', regex=True)
df['beginDate'] = pd.to_datetime(df['beginDate'])
df['beginDate'] = df['beginDate'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

df['endDate'] = pd.to_datetime(df['endDate'])
df['endDate'] = df['endDate'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
#df['uploaded'] = ''
df['status_code'] = ''
df['message'] = ''
#df['annotation_id'] = ''

for index, row in df.iterrows():
    # print row
    # print 'Reading csv row %s' #%csv_row
    d = {'@class': '.AnnotationRecord'}
    d['subsite'] = row['subsite']
    d['node'] = row['node']
    d['sensor'] = row['sensor']
    d['stream'] = row['stream']
    d['method'] = row['method']
    if row['parameters'] == '':
        d['parameters'] = []
    else:
        d['parameters'] = ast.literal_eval(row['parameters'])

    beginDate = row['beginDate']
    endDate = row['endDate']
    beginDT, endDT = check_dates(beginDate,endDate)
    d['beginDT'] = beginDT
    d['endDT'] = endDT

    d['exclusionFlag'] = check_exclusionFlag(row['exclusionFlag'])
    d['qcFlag'] = check_qcFlag(row['qcFlag'])
    d['annotation'] = row['annotation']
    d['source'] = source
    if row['id']: # if an id is specified in the csv, update the annotation
        d['id'] = int(row['id'])
        jsond = json.dumps(d).replace('""', 'null')
        uurl = url + str(int(row['id']))
        r = session.put(uurl, data=jsond, auth=(username, token))
        response = r.json()

    if not row['id']: # if no id is specified in the csv, creates a new annotation
        jsond = json.dumps(d).replace('""', 'null')
        r = session.post(url, data=jsond, auth=(username, token))
        response = r.json()

    df.loc[row.name, 'status_code'] = r.status_code
    df.loc[row.name, 'message'] = str(response['message'])
    try:
        df.loc[row.name, 'id'] = response['id']
    except KeyError:
        df.loc[row.name, 'id'] = ''
    # if r.status_code == 201:
    #     df.loc[row.name, 'uploaded'] = True
    #     df.loc[row.name, 'id'] = response['id']
    #
    # else:
    #     df.loc[row.name, 'id'] = ''
    #     df.loc[row.name, 'uploaded'] = False

df.to_csv(anno_csv.split('.')[0] + '_run.csv', index=False)