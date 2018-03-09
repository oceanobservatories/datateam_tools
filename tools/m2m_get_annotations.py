"""
Created on 10/17/2017
Modified on 3/9/2018

@author: lgarzio
@brief: This script is used to export annotations from uFrame to a csv
@usage:
username: username to access the OOI API
token: password to access the OOI API
refdes: string of partially- (e.g. GS01SUMO) or fully-qualified (e.g. GS01SUMO-SBD11-06-METBKA000) reference designators
        or '' if requesting all annotations. Can be multiple, i.e. 'GS01SUMO, GI01SUMO-SBD11, GI01SUMO-SBD12'
saveDir: location to save output
"""

import requests
import os
import csv
from datetime import datetime


def format_inputs(input_str):
    if ',' in input_str:
        input_str = input_str.replace(" ","") #remove any whitespace
        finput = input_str.split(',')
    else:
        finput = [input_str]

    return finput


def get_ids(username, token, session):
    # get a list of valid annotation IDs in uFrame (for writing all annotations)
    id_url = 'https://ooinet.oceanobservatories.org/api/m2m/12580/anno?max_100&select_id&start_id='
    for x in range(100):
        if x == 0:
            start_id = 0
            IDurl = id_url + str(start_id)
            response = get_response(IDurl, username, token, session)
            ids = sorted(response.json())
            if len(ids) < 100:
                loop_ids = ids
                return loop_ids
            else:
                loop_ids = ids
                start_id = ids[-1] + 1
        else:
            IDurl = id_url + str(start_id)
            response = get_response(IDurl, username, token, session)
            ids = sorted(response.json())
            if len(ids) < 100:
                loop_ids = loop_ids + ids
                return loop_ids
            else:
                loop_ids = loop_ids + ids
                start_id = ids[-1] + 1


def get_response(url, username, token, session):
    response = session.get(url=url, auth=(username, token))
    return response


def write_all_annotations(username, token, f, session):
    # write annotations if no reference designator is specified
    anno_url = 'https://ooinet.oceanobservatories.org/api/m2m/12580/anno/'
    loop_ids = get_ids(username, token, session)
    print 'Writing annotations'

    for x in loop_ids:
        url = anno_url + str(x)
        anno = get_response(url, username, token, session)
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
                       info['qcFlag'],info['source'],info['annotation'].encode('utf-8')]
            writer.writerow(newline)


def write_refdes_annotations(username, token, refdes_list, outfile, session):
    # write annotations if any reference designator is specified
    anno_url = 'https://ooinet.oceanobservatories.org/api/m2m/12580/anno/find'
    today_date = int(datetime.now().strftime("%s")) * 1000 # current date
    print 'Writing annotations'

    id_list = []
    for x in refdes_list:
        get_params = {
        "beginDT": 1356998400000,  # 2013-01-01T00:00:00
        "endDT": today_date,
        "refdes": x
        }

        response = session.get(anno_url, auth=(username, token), params=get_params)
        if response.status_code == 200:
            data = response.json()

            for d in data:
                if d['id'] not in id_list: # write annotation only if it hasn't already been written
                    id_list.append(d['id'])
                    beginDate = datetime.utcfromtimestamp(float(d['beginDT'])/1000).strftime('%Y-%m-%dT%H:%M:%S')
                    try:
                        endDate = datetime.utcfromtimestamp(float(d['endDT'])/1000).strftime('%Y-%m-%dT%H:%M:%S')
                    except TypeError: # if end date is blank
                        endDate = []
                    writer = csv.writer(outfile)
                    writer.writerow([d['id'],d['subsite'],d['node'],d['sensor'],d['stream'],d['method'],d['parameters'],
                                     beginDate,endDate,d['beginDT'],d['endDT'],d['exclusionFlag'],d['source'],
                                     d['annotation'].encode('utf-8')])


def main(username, token, refdes, saveDir):
    sensor_inv = 'https://ooinet.oceanobservatories.org/api/m2m/12576/sensor/inv/'
    f = 'uframe_annotations_%s.csv' % datetime.now().strftime('%Y%m%dT%H%M%S')
    fN = os.path.join(saveDir,f)

    session = requests.session() # open the connection and leave it open for the session
    with open(fN, 'a') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['id','subsite','node','sensor','stream','method','parameters','beginDate','endDate',
                         'beginDT','endDT','exclusionFlag','qcFlag','source','annotation'])

        if not refdes: # if no refdes specified, provide all annotations
            write_all_annotations(username, token, outfile, session)
        else:
            refdes_list = []
            frefdes = format_inputs(refdes)
            for i in frefdes:
                print i
                if len(i) == 8:
                    url = sensor_inv + i
                    node_info = session.get(url, auth=(username, token))
                    nodes = node_info.json()
                    for n in nodes:
                        url2 = url + '/' + n
                        sensor_info = session.get(url2, auth=(username, token))
                        sensors = sensor_info.json()
                        for s in sensors:
                            refdes = '-'.join([i,n,s])
                            refdes_list.append(refdes)
                elif len(i) == 14:
                    url = sensor_inv + i.split('-')[0] + '/' + i.split('-')[1]
                    sensor_info = session.get(url, auth=(username, token))
                    sensors = sensor_info.json()
                    for s in sensors:
                        refdes = '-'.join([i,s])
                        refdes_list.append(refdes)
                elif len(i) == 27:
                    refdes_list.append(i)
            refdes_unique = sorted(list(set(refdes_list)))
            write_refdes_annotations(username, token, refdes_unique, outfile, session)


if __name__ == '__main__':
    username = 'username'
    token = 'token'
    refdes = '' # 'GS01SUMO, GS01SUMO-SBD11, GS01SUMO-SBD11-06-METBKA000'
    saveDir = '/Users/lgarzio/Documents/OOI/Annotations'
    main(username, token, refdes, saveDir)