#!/usr/bin/env python
"""
Created on Dec 12 2017

@author: lgarzio@marine.rutgers.edu
@brief: This script is used to list the last timestamp for every instrument/stream combination in uFrame and the Data
Team Database (http://ooi.visualocean.net/). If desired, the user can specify subsite(s) of interest. Two output files
are generated: uframe_status* = status of all valid instrument/stream/delivery methods (recovered, telemetered, and
streamed), and uframe_telemetry_status* = status of all valid telemetered and streamed data.
@usage:
saveDir: location to save output files
subsites: user-identified subsites to analyze. Must be either a list of subsites (e.g. ['GA01SUMO','GA02HYPM']), or 'all'.
"""


import pandas as pd
import requests
import os
import datetime


def define_source(df):
    df['source'] = 'x'
    df['source'][(df['source_x']=='datateam_database') & (df['source_y']=='uframe')] = 'both datateam database and uframe'
    df['source'][(df['source_x']=='datateam_database') & (df['source_y'].isnull())] = 'datateam database only'
    df['source'][(df['source_x'].isnull()) & (df['source_y']=='uframe')] = 'uframe only'
    return df


def define_status(sys_endDT, now):
    sys_endDT_num = datetime.datetime.strptime(sys_endDT[:-1],'%Y-%m-%dT%H:%M:%S.%f')
    if sys_endDT_num > now - datetime.timedelta(hours=1):
        status = '<1 hour'
    elif sys_endDT_num < now - datetime.timedelta(hours=1) and sys_endDT_num > now - datetime.timedelta(days=1):
        status = '<1 day'
    elif sys_endDT_num < now - datetime.timedelta(days=1) and sys_endDT_num > now - datetime.timedelta(days=7):
        status = '<1 week'
    elif sys_endDT_num < now - datetime.timedelta(days=7) and sys_endDT_num > now - datetime.timedelta(days=30):
        status = '<30 days'
    elif sys_endDT_num < now - datetime.timedelta(days=30) and sys_endDT_num > now - datetime.timedelta(days=365):
        status = '<1 year'
    else:
        status = '>1 year'
    return status


def get_database():
    db_inst_stream = pd.read_csv('https://raw.githubusercontent.com/seagrinch/data-team-python/master/infrastructure/data_streams.csv')
    db_stream_desc = pd.read_csv('https://raw.githubusercontent.com/seagrinch/data-team-python/master/infrastructure/stream_descriptions.csv')

    db_inst_stream = db_inst_stream[['reference_designator','method','stream_name']]
    db_stream_desc = db_stream_desc.rename(columns={'name':'stream_name'})
    db_stream_desc = db_stream_desc[['stream_name','stream_type']]
    db = pd.merge(db_inst_stream,db_stream_desc,on='stream_name',how='outer')
    db['source'] = 'datateam_database'
    db = db[db.reference_designator.notnull()]
    return db


def get_uframe_data(valid_methods, now):
    x = requests.get('https://ooinet.oceanobservatories.org/api/uframe/stream')
    stream_info = x.json()
    sys_list = []
    for i in range(len(stream_info['streams'])):
        sys_method = stream_info['streams'][i]['stream_method']
        if sys_method in valid_methods:
            sys_refdes = stream_info['streams'][i]['reference_designator']
            sys_stream = stream_info['streams'][i]['stream']
            sys_endDT = stream_info['streams'][i]['end']
            sys_method_rp = sys_method.replace('-','_')
            status = define_status(sys_endDT, now)
            source = 'uframe'
            sys_list.append([sys_refdes,sys_method_rp,sys_stream,sys_endDT,status,source])
    return sys_list


def main(saveDir, subsites):
    now = datetime.datetime.utcnow()

    valid_methods = ['streamed','telemetered','recovered-host','recovered-inst','recovered-wfp','recovered-cspp']

    sys_list = get_uframe_data(valid_methods, now)
    uframe_df = pd.DataFrame(sys_list,columns=['reference_designator','method','stream_name','endDT','status','source'])
    db = get_database()

    df = pd.merge(db,uframe_df,on=['reference_designator','method','stream_name'],how='outer')
    df['subsite'] = df.reference_designator.str.split('-').str[0]

    df = define_source(df)

    df_tel = df.loc[df.method.isin(['streamed','telemetered'])]

    if subsites == 'all':
        output_tel_final = df_tel
        fname_tel = 'uframe_telemetry_status_allsubsites_%s.csv' %now.strftime('%Y%m%dT%H%M%S')
        output_all_final = df
        fname_all = 'uframe_status_allsubsites_%s.csv' %now.strftime('%Y%m%dT%H%M%S')
    else:
        output_tel_final = df_tel[df_tel.subsite.isin(subsites)] # filter on selected subsites
        fname_tel = 'uframe_telemetry_status_filtered_%s.csv' %now.strftime('%Y%m%dT%H%M%S')
        output_all_final = df[df.subsite.isin(subsites)] # filter on selected subsites
        fname_all = 'uframe_status_filtered_%s.csv' %now.strftime('%Y%m%dT%H%M%S')

    col = ['subsite','reference_designator','method','stream_name','stream_type','endDT','status','source']
    output_tel_final.sort_values(by=['subsite','reference_designator']).to_csv(os.path.join(saveDir,fname_tel),index=False, columns=col)
    output_all_final.sort_values(by=['subsite','reference_designator']).to_csv(os.path.join(saveDir,fname_all),index=False, columns=col)

if __name__ == '__main__':
    saveDir = '/Users/lgarzio/Documents/OOI/uframe_status_reports/'
    #subsites = 'all'
    subsites = ['GA01SUMO','GA02HYPM']
    main(saveDir, subsites)