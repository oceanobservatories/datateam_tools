"""
Created on Oct 17 2017

@author: lgarzio
@brief: This script is used to export all annotations from uFrame associated with a reference designator
@usage:
username: username to access the OOI API
token: password to access the OOI API
saveDir: location to save output
refdes: Reference designator of interest
"""

import requests
import os
import csv


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


def refdes_annotations(username, token, refdes, frefdes):
    anno_url = 'https://ooinet.oceanobservatories.org/api/m2m/12580/anno/'
    loop_ids = get_ids(username, token)
    session = requests.session() # open the connection and leave it open for the session

    subsite = refdes.split('-')[0]
    node = refdes.split('-')[1]
    sensor = refdes.split('-')[2] + '-' + refdes.split('-')[3]

    for x in loop_ids:
        url = anno_url + str(x)
        anno = session.get(url, auth=(username, token))
        if anno.status_code == 200: # only write info if there is a valid response
            info = anno.json()
            writer = csv.writer(frefdes)
            newline = [info['id'],info['subsite'],info['node'],info['sensor'],info['stream'],info['method'],
                       info['parameters'],info['annotation'].encode('utf-8'),info['beginDT'],info['endDT'],info['exclusionFlag'],
                       info['source']]
            if info['subsite'] == subsite and info['node'] == None and info['sensor'] == None: # get annotations for subsite only
                writer.writerow(newline)
            elif info['subsite'] == subsite and info['node'] == node and info['sensor'] == None: # get annotations for node only:
                writer.writerow(newline)
            elif info['subsite'] == subsite and info['node'] == node and info['sensor'] == sensor: # get annotations for sensor only
                writer.writerow(newline)


def main(username, token, saveDir, refdes):
    fNrefdes = refdes + '_annotations.csv'
    fNrefdes = os.path.join(saveDir,fNrefdes) # file for all annotations for a refdes

    with open(fNrefdes, 'a') as frefdes:
        writer = csv.writer(frefdes)
        writer.writerow(['id','subsite','node','sensor','stream','method','parameters','annotation','beginDT','endDT','exclusionFlag','source'])
        refdes_annotations(username, token, refdes, frefdes)

if __name__ == '__main__':
    username = 'username'
    token = 'password'
    saveDir = 'path_to_local_directory'
    refdes = 'GS01SUMO-SBD12-08-FDCHPA000'
    main(username, token, saveDir, refdes)