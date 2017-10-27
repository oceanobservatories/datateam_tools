#!/usr/bin/env python
'''
Created on Oct 17 2017
@author: lgarzio@marine.rutgers.edu
@brief: This script is used to export all annotations from uFrame associated with a fully-qualified reference designator
@usage:
username: username to access the OOI API
token: password to access the OOI API
saveDir: location to save output
refdes: fully-qualified reference designator
'''


import requests
import os
import csv
from datetime import datetime


def make_dir(save_dir):
    try:  # Check if the save_dir exists already... if not, make it
        os.mkdir(save_dir)
    except OSError:
        pass


def write_annotations (username, token, refdes, outfile):
    '''
    :param username: OOI API username
    :param token: OOI API password
    :param refdes: fully-qualified reference designator
    :param outfile: csv file to which annotations are written
    :return: csv file containing all annotations from uframe for a reference designator
    '''
    anno_url = 'https://ooinet.oceanobservatories.org/api/m2m/12580/anno/find'
    timeout = 10
    timeout_read = 30
    session = requests.session() # open the connection and leave it open for the session
    today_date = int(datetime.now().strftime("%s")) * 1000 # current date

    get_params = {
    "beginDT": 1356998400000,  # 2013-01-01T00:00:00
    "endDT": today_date,
    "refdes": refdes
    }

    response = session.get(anno_url, auth=(username, token),params=get_params,timeout=(timeout, timeout_read),verify=False)

    if response.status_code == 200:
        data = response.json()
        for d in range(len(data)):
            dd = data[d]
            beginDate = datetime.utcfromtimestamp(float(dd['beginDT'])/1000).strftime('%Y-%m-%dT%H:%M:%S')
            try:
                endDate = datetime.utcfromtimestamp(float(dd['endDT'])/1000).strftime('%Y-%m-%dT%H:%M:%S')
            except TypeError: # if end date is blank
                endDate = []
            writer = csv.writer(outfile)
            writer.writerow([dd['id'],dd['subsite'],dd['node'],dd['sensor'],dd['stream'],dd['method'],dd['parameters'],
                             beginDate,endDate,dd['beginDT'],dd['endDT'],dd['exclusionFlag'],dd['source'],
                             dd['annotation'].encode('utf-8')])


def main(username, token, saveDir, refdes):
    annotations_dir = os.path.join(saveDir, 'uframe_annotations')
    make_dir(annotations_dir)

    f = '%s_annotations_%s.csv' % (refdes, datetime.now().strftime('%Y%m%dT%H%M%S'))
    fN = os.path.join(annotations_dir,f)

    with open(fN, 'a') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['id','subsite','node','sensor','stream','method','parameters','beginDate','endDate',
                         'beginDT','endDT','exclusionFlag','source','annotation'])
        write_annotations(username, token, refdes, outfile)


if __name__ == '__main__':
    username = 'username'
    token = 'password'
    saveDir = '/path/to/savefile'
    refdes = 'GI01SUMO-SBD11-06-METBKA000'
    main(username, token, saveDir, refdes)