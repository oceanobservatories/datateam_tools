#!/usr/bin/env python
"""
Created on Oct 19 2017
@author: lgarzio
@brief: This script is used to export all annotations from uFrame associated with a subsite or glider
@usage:
username: username to access the OOI API
token: password to access the OOI API
saveDir: location to save output
subsite: Subsite (platform) or glider of interest (e.g. CP01CNSM or CP05MOAS-GL379)
"""

import requests
import os
import csv
from datetime import datetime


def get_refdes_list(username, token, subsite):
    '''
    :param username: OOI API username
    :param token: OOI API password
    :param subsite: subsite or glider
    :return: refdes_list: list of reference designators in uFrame associated with the subsite
    '''
    subsite_code = subsite[4:8]
    refdes_list = []

    if subsite_code == 'MOAS':
        url2 = 'https://ooinet.oceanobservatories.org/api/m2m/12576/sensor/inv/' + subsite.split('-')[0] + '/' + subsite.split('-')[1]
        session = requests.session() # open the connection and leave it open for the session
        sensor_info = session.get(url2, auth=(username, token))
        sensors = sensor_info.json()
        for s in range(len(sensors)):
            refdes = '-'.join([subsite,sensors[s]])
            refdes_list.append(refdes)
        return refdes_list

    else:
        url = 'https://ooinet.oceanobservatories.org/api/m2m/12576/sensor/inv/' + subsite
        session = requests.session() # open the connection and leave it open for the session
        node_info = session.get(url, auth=(username, token))
        nodes = node_info.json()
        for n in range(len(nodes)):
            url2 = url + '/' + nodes[n]
            sensor_info = session.get(url2, auth=(username, token))
            sensors = sensor_info.json()
            for s in range(len(sensors)):
                refdes = '-'.join([subsite,nodes[n],sensors[s]])
                refdes_list.append(refdes)
        return refdes_list


def write_annotations (username, token, refdes_list, outfile):
    '''
    :param username: OOI API username
    :param token: OOI API password
    :param refdes_list: output from get_refdes_list function
    :param outfile: csv file to which annotations are written
    :return: csv file containing all annotations from uframe for a subsite
    '''
    anno_url = 'https://ooinet.oceanobservatories.org/api/m2m/12580/anno/find'
    session = requests.session() # open the connection and leave it open for the session
    today_date = int(datetime.now().strftime("%s")) * 1000 # current date

    id_list = []
    for x in range(len(refdes_list)):
        refdes = refdes_list[x]

        get_params = {
        "beginDT": 1356998400000,  # 2013-01-01T00:00:00
        "endDT": today_date,
        "refdes": refdes
        }

        response = session.get(anno_url, auth=(username, token),params=get_params)
        if response.status_code == 200:
            data = response.json()

            for d in range(len(data)):
                dd = data[d]
                if dd['id'] not in id_list: # write annotation only if it hasn't already been written
                    id_list.append(dd['id'])
                    beginDate = datetime.utcfromtimestamp(float(dd['beginDT'])/1000).strftime('%Y-%m-%dT%H:%M:%S')
                    try:
                        endDate = datetime.utcfromtimestamp(float(dd['endDT'])/1000).strftime('%Y-%m-%dT%H:%M:%S')
                    except TypeError: # if end date is blank
                        endDate = []
                    writer = csv.writer(outfile)
                    writer.writerow([dd['id'],dd['subsite'],dd['node'],dd['sensor'],dd['stream'],dd['method'],dd['parameters'],
                                     beginDate,endDate,dd['beginDT'],dd['endDT'],dd['exclusionFlag'],dd['source'],
                                     dd['annotation'].encode('utf-8')])


def main(username, token, saveDir, subsite):
    f = '%s_uframe_annotations_%s.csv' % (subsite, datetime.now().strftime('%Y%m%dT%H%M%S'))
    fN = os.path.join(saveDir,f)

    with open(fN, 'a') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['id','subsite','node','sensor','stream','method','parameters','beginDate','endDate',
                         'beginDT','endDT','exclusionFlag','source','annotation'])
        refdes_list = get_refdes_list(username, token, subsite)
        write_annotations(username, token, refdes_list, outfile)


if __name__ == '__main__':
    username = 'username'
    token = 'password'
    saveDir = '/path/to/savefile'
    subsite = 'GS01SUMO'
    main(username, token, saveDir, subsite)