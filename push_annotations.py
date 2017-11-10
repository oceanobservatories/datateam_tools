"""
Created on Nov 10 2017

@author: lgarzio
@brief: This script is used to push new annotations from a csv to uFrame via the M2M API
@usage:
anno_csv: csv with annotations to push to uFrame with headers: subsite,node,sensor,stream,method,parameters,beginDate,
            endDate,exclusionFlag,qcFlag,annotation
source: email address to associate with annotation
username: username to access the OOI API
token: password to access the OOI API
url: annotation endpoint
"""

import requests
import csv
import json
import ast
from datetime import datetime
import netCDF4 as nc

anno_csv = '/Users/lgarzio/Documents/OOI/test/new_annotations.csv'
source = 'lgarzio@marine.rutgers.edu'

# production
username = 'username'
token = 'token'
url = 'https://ooinet.oceanobservatories.org/api/m2m/12580/anno/'

# ooinet-dev-01
# username = 'username'
# token = 'token'
# url = 'https://ooinet-dev-01.oceanobservatories.org/api/m2m/12580/anno/'


def check_dates(begin_DT,end_DT):
    if begin_DT >= end_DT:
        beginDT = int(nc.date2num(begin_DT,'seconds since 1970-01-01')*1000)
        endDT = int(nc.date2num(end_DT,'seconds since 1970-01-01')*1000)
        return beginDT, endDT
    else:
        raise ValueError('beginDate (%s) is after endDate (%s)' %(begin_DT,end_DT))


def check_exclusionFlag(exclusionFlag):
    if exclusionFlag == 'FALSE':
        exclusionFlag = 0
        return exclusionFlag
    elif exclusionFlag == 'TRUE':
        exclusionFlag = 1
        return exclusionFlag
    else:
        raise ValueError('Invalid exclusionFlag: %s' %exclusionFlag)


def check_qcFlag(qcFlag):
    qcFlag_set = set(['','not_operational','not_available','pending_ingest','not_evaluated','suspect','fail','pass'])
    if qcFlag in qcFlag_set:
        return qcFlag
    else:
        raise ValueError('Invalid qcFlag: %s' %qcFlag)


with open(anno_csv, 'r') as infile:
    reader = csv.DictReader(infile)
    for i, row in enumerate(reader):
        csv_row = i+2
        print 'Reading csv row %s' %csv_row
        d = {'@class':'.AnnotationRecord'}
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
        begin_DT = datetime.strptime(beginDate,'%Y-%m-%dT%H:%M:%SZ')
        endDate = row['endDate']
        end_DT = datetime.strptime(endDate,'%Y-%m-%dT%H:%M:%SZ')
        beginDT, endDT = check_dates(begin_DT,end_DT)
        d['beginDT'] = beginDT
        d['endDT'] = endDT

        d['exclusionFlag'] = check_exclusionFlag(row['exclusionFlag'])
        d['qcFlag'] = check_qcFlag(row['qcFlag'])
        d['annotation'] = row['annotation']
        d['source'] = source
        jsond = json.dumps(d).replace('""','null')

        r = requests.post(url, data=jsond, auth=(username, token))
        if r.status_code == 201:
            response = r.json()
            print 'Message: %s' %response['message']
            print 'id: %s\n' %response['id']
        else:
            response = r.json()
            print response
            raise ValueError('csv row %s: invalid annotation\n' %csv_row)