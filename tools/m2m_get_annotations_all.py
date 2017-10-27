"""
Created on Oct 17 2017
@author: lgarzio
@brief: This script is used to export all annotations from uFrame to a csv
@usage:
username: username to access the OOI API
token: password to access the OOI API
saveDir: location to save output
"""

import requests
import os
import csv
from datetime import datetime


def get_ids(username, token):
    '''
    :param username: OOI API username
    :param token: OOI API password
    :return: loop_ids = list of valid annotation IDs in uFrame
    '''
    id_url = 'https://ooinet.oceanobservatories.org/api/m2m/12580/anno?max_100&select_id&start_id='
    session = requests.session() # open the connection and leave it open for the session
    for x in range(100):
        if x == 0:
            start_id = 0
            IDurl = id_url + str(start_id)
            response = session.get(url=(IDurl), auth=(username, token))
            ids = sorted(response.json())
            if len(ids) < 100:
                loop_ids = ids
                return loop_ids
            else:
                loop_ids = ids
                start_id = ids[-1] + 1
        else:
            IDurl = id_url + str(start_id)
            response = session.get(url=(IDurl), auth=(username, token))
            ids = sorted(response.json())
            if len(ids) < 100:
                loop_ids = loop_ids + ids
                return loop_ids
            else:
                loop_ids = loop_ids + ids
                start_id = ids[-1] + 1


def write_annotations(username, token, f):
    '''
    :param username: OOI API username
    :param token: OOI API password
    :param f: csv file to which annotations are written
    :return: csv file containing all annotations from uFrame
    '''
    anno_url = 'https://ooinet.oceanobservatories.org/api/m2m/12580/anno/'
    loop_ids = get_ids(username, token)
    session = requests.session() # open the connection and leave it open for the session

    for x in loop_ids:
        url = anno_url + str(x)
        anno = session.get(url, auth=(username, token))
        if anno.status_code == 200: # only write info if there is a valid response
            info = anno.json()
            beginDate = datetime.utcfromtimestamp(float(info['beginDT'])/1000).strftime('%Y-%m-%dT%H:%M:%S')
            try:
                endDate = datetime.utcfromtimestamp(float(info['endDT'])/1000).strftime('%Y-%m-%dT%H:%M:%S')
            except TypeError: # if end date is blank
                endDate = []
            writer = csv.writer(f)
            newline = [info['id'],info['subsite'],info['node'],info['sensor'],info['stream'],info['method'],
                       info['parameters'],beginDate,endDate,info['beginDT'],info['endDT'],info['exclusionFlag'],
                       info['source'],info['annotation'].encode('utf-8')]
            writer.writerow(newline)


def main(username, token, saveDir):
    f = 'all_annotations_%s.csv' % datetime.now().strftime('%Y%m%dT%H%M%S')
    fN = os.path.join(saveDir,f)

    with open(fN, 'a') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['id','subsite','node','sensor','stream','method','parameters','beginDate','endDate',
                         'beginDT','endDT','exclusionFlag','source','annotation'])
        write_annotations(username, token, outfile)


if __name__ == '__main__':
    username = 'username'
    token = 'password'
    saveDir = '/path/to/savefile'
    main(username, token, saveDir)