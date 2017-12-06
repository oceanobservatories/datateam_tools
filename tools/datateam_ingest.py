#!/usr/bin/env python
"""
@author Mike Smith
@email michaesm@marine.rutgers.edu
change username, api_key, api_token, and csv_path (lines 242 - 265)
"""

import requests
import pandas as pd
import os
import glob
import re
import urllib2
import pickle
import datetime as dt
import netCDF4 as nc
from pandas.io.json import json_normalize

HTTP_STATUS_OK = 200


def file_date():
    date_cutoff = raw_input('Please enter file cutoff date in the form (yyyy-mm-dd): ')
    r = re.compile('\d{4}-\d{2}-\d{2}')

    if date_cutoff:
        if r.match(date_cutoff) is None:
            print 'Incorrect date format entered.'
            date_cutoff = file_date()
    else:
        'No date entered.'
        date_cutoff = file_date()
    return date_cutoff


def csv_select(csvs, serve):
    df = pd.DataFrame()
    recovered = [x for x in csvs if 'R000' in x]
    telemetered = [x for x in csvs if 'D000' in x]
    telemetered.sort(reverse=True)
    csvs = telemetered + recovered
    if 'telemetered' in serve:
        print 'Please select csv(s) for active telemetered ingestion. Latest telemetered deployment CSV listed first'
        for csv in telemetered:
            yes = raw_input('Ingest {}? y/<n>: '.format(csv)) or 'n'
            if 'y' in yes:
                t_df = load_ingestion_sheet(csv)
                t_df['username'] = username
                t_df['deployment'] = get_deployment_number(csv)
                t_df['type'] = 'TELEMETERED'
                t_df['state'] = 'RUN'
                t_df['priority'] = priority
                begin = raw_input('Set a beginning file date to exclude any files modified before this date? y/<n>') or 'n'
                if 'y' in begin:
                    t_df['beginFileDate'] = file_date()
                df = df.append(t_df, ignore_index=True)
            else:
                continue
    else:
        print 'Please select csv(s) for recovered ingestion.'
        for csv in csvs:
            yes = raw_input('Ingest {}? <y>/n: '.format(csv)) or 'y'
            if 'y' in yes:
                t_df = load_ingestion_sheet(csv)
                t_df['username'] = username
                t_df['deployment'] = get_deployment_number(csv)
                t_df['type'] = 'RECOVERED'
                t_df['state'] = 'RUN'
                t_df['priority'] = priority
                end = raw_input('Set an ending file date to exclude files modified after this date? y/<n>') or 'n'
                if 'y' in end:
                    end_date = file_date()
                    t_df['endFileDate'] = end_date
                df = df.append(t_df, ignore_index=True)
            else:
                continue

    if df.empty:
        print('At least one csv must be selected. Please select csv')
        df = csv_select(csvs, serve)
    return df


def change_recurring_ingestion(api_key, api_token, recurring):
    state_change = pd.DataFrame()

    if not recurring.empty:
        print('Recurring ingestions found for these reference designators.')
        print(recurring)
        state = raw_input('Would you like to persist, cancel, suspend any of these ingestions? <persist>/cancel/suspend: ') or 'persist'
        if state in ('cancel', 'suspend'):
            # for i in recurring.iter():
            which = raw_input('Please list ingestion id (comma separated) that you would like to {}: '.format(state))
            if which:
                ids_to_purge = map(int, which.split(','))
                if ids_to_purge:
                    for i in ids_to_purge:
                        r = change_ingest_state(base_url, api_key, api_token, i, state.upper())
                        if r.status_code == HTTP_STATUS_OK:
                            state_json = r.json()
                        else:
                            print('Status Code: {}, Response: {}'.format(r.status_code, r.content))
                            continue
                        tdf = pd.DataFrame([state_json], columns=state_json.keys())
                        state_change = state_change.append(tdf, ignore_index=True)
                    print(state_change)
    return state_change


def get_active_ingestions(base_url, api_key, api_token):
    r = session.get('{}/api/m2m/12589/ingestrequest/jobcounts?active=true&groupBy=refDes,status'.format(base_url),
                     auth=(api_key, api_token))
    if r.ok:
        return r
    else:
        pass


def get_active_ingestions_for_refdes(base_url, api_key, api_token, ref_des):
    r = session.get('{}/api/m2m/12589/ingestrequest/jobcounts?refDes={}&groupBy=refDes,status'.format(base_url, ref_des),
                     auth=(api_key, api_token))
    if r.ok:
        return r
    else:
        pass


def get_all_ingest_requests(base_url, api_key, api_token):
    r = session.get('{}/api/m2m/12589/ingestrequest/'.format(base_url), auth=(api_key, api_token))
    if r.ok:
        return r
    else:
        pass


def build_ingest_dict(ingest_info):
    option_dict = {}
    keys = ingest_info.keys()

    adict = {k: ingest_info[k] for k in ('parserDriver', 'fileMask', 'dataSource', 'deployment', 'refDes','refDesFinal') if k in ingest_info}
    request_dict = dict(username=ingest_info['username'],
                        state=ingest_info['state'],
                        ingestRequestFileMasks=[adict],
                        type=ingest_info['type'],
                        priority=ingest_info['priority'])

    for k in ['beginFileDate', 'endFileDate']:
        if k in keys:
            option_dict[k] = ingest_info[k]

    if option_dict:
        request_dict['options'] = dict(option_dict)

    return request_dict


def ingest_data(base_url, api_key, api_token, data_dict):
    r = session.post('{}/api/m2m/12589/ingestrequest'.format(base_url),
                      json=data_dict,
                      auth=(api_key, api_token))
    if r.ok:
        return r
    else:
        pass


def change_ingest_state(base_url, api_key, api_token, ingest_id, state):
    r = session.put('{}/api/m2m/12589/ingestrequest/{}'.format(base_url, ingest_id),
                     json=dict(id=ingest_id, state=state),
                     auth=(api_key, api_token))
    if r.ok:
        return r
    else:
        pass


def check_ingest_request(base_url, api_key, api_token, ingest_id):
    r = session.get('{}/api/m2m/12589/ingestrequest/{}'.format(base_url, ingest_id),
                     auth=(api_key, api_token))
    if r.ok:
        return r
    else:
        pass


def check_ingest_file_status(base_url, api_key, api_token, ingest_id):
    r = session.get('{}/api/m2m/12589/ingestrequest/jobcounts?ingestRequestId={}&groupBy=status'.format(base_url, ingest_id),
                     auth=(api_key, api_token))
    if r.ok:
        return r
    else:
        pass


def purge_data(base_url, api_key, api_token, ref_des_dict):
    r = session.put('{}/api/m2m/12589/ingestrequest/purgerecords'.format(base_url),
                     json=ref_des_dict,
                     auth=(api_key, api_token))
    if r.ok:
        return r
    else:
        pass


def uframe_routes():
    """
    This function loads a pickle file containing all uframe_routes to their proper drivers
    :return: dictionary containing the uframe_routes to driver
    :rtype: dictionary
    """
    fopen = urllib2.urlopen('https://raw.githubusercontent.com/ooi-data-review/parse_spring_files/master/uframe_routes.pkl')
    ingest_dict = pickle.load(fopen)
    return ingest_dict


def load_ingestion_sheet(csv):
    t_df = pd.read_csv(csv, usecols=[0, 1, 2, 3])
    return t_df


def get_deployment_number(csv):
    split_csv = csv.split('_')
    deployment_number = int(re.sub('.*?([0-9]*)$', r'\1', split_csv[1]))
    return deployment_number

desired_width = 320
pd.set_option('display.width', desired_width)

# initialize requests session
global session
session = requests.session()

# Load uframe_routes from github.
ingest_dict = uframe_routes()

# Initialize empty Pandas DataFrames
purge_df = pd.DataFrame()
ingest_df = pd.DataFrame()
recurring = pd.DataFrame()

# OOINET authorization information and base_url
username = 'michaesm'

# ooinet production
base_url = 'https://ooinet.oceanobservatories.org'
api_key = 'username'
api_token = 'token'

# ooinet-dev-01
# base_url = 'https://ooinet-dev-01.oceanobservatories.org'
# api_key = 'username'
# api_token = 'token'

# ooinet-dev-03
# base_url = 'https://ooinet-dev-03.oceanobservatories.org'
# api_key = 'username'
# api_token = 'token'

# ooinet-dev-04
# base_url = 'https://ooinet-dev-04.oceanobservatories.org'
# api_key = 'username'
# api_token = 'token'

priority = 1
csv_path = '/local_path_to_ingestion-csvs/'


# Avoid these platforms right now
cabled = ['RS', 'CE02SHBP', 'CE02SHSP', 'CE04OSBP', 'CE04OSPD', 'CE04OSPS']
cabled_reg_ex = re.compile('|'.join(cabled))

# Enter ooinet username. Doesn't need to be an email
print 'url: %s' %base_url
username = raw_input('Enter ooinet username: {}'.format(username)) or username
api_key = raw_input('Enter ooinet key: {}'.format(api_key)) or api_key
api_token = raw_input('Enter ooinet token: {}'.format(api_token)) or api_token

priority = raw_input('Enter priority level (0=highest to 10=lowest) of this ingest <{}:default>: '.format(priority)) or priority
try:
    priority = int(priority)
except ValueError:
    print('Priority must be an integer. Using default priority of 0')
    priority = 0

serve = raw_input('\nWould you like to create a recovered or telemetered (recurring) ingestion? <recovered>/telemetered: ') or 'recovered'
arrays = raw_input('\nSelect an array for ingestion - CE, CP, GA, GI, GS, GP: ')
arrays = arrays.upper().split(', ')
reg_ex = re.compile('|'.join(arrays))
result = [y for x in os.walk(csv_path) for y in glob.glob(os.path.join(x[0], '*.csv')) if reg_ex.search(y) and not cabled_reg_ex.search(y)]
platforms = [x.split('/')[-2] for x in result]
platforms = set(platforms)
platforms = list(platforms)
platforms.sort()

print 'The following platforms from the previously selected arrays have ingestion csvs.'
for platform in platforms:
    print platform
selected_platform = raw_input('\nPlease select (comma separated) platform that you would like to ingest: ')
selected_platform = selected_platform.upper().split(', ')
reg_ex = re.compile('|'.join(selected_platform))
csvs = [y for x in os.walk(csv_path) for y in glob.glob(os.path.join(x[0], '*.csv')) if reg_ex.search(y)]
csvs.sort()

df = csv_select(csvs, serve)
df = df.sort_values(['deployment', 'reference_designator'])
df = df.rename(columns={'filename_mask': 'fileMask', 'reference_designator': 'refDes', 'data_source': 'dataSource',
                        'parser': 'parserDriver'})
df = df[pd.notnull(df['fileMask'])]

unique_ref_des = list(pd.unique(df.refDes.ravel()))
unique_ref_des.sort()

# Get ingestion requests with ID so that we can check if we are ingesting data that already has a recurring ingestion
all_ingest = get_all_ingest_requests(base_url, api_key, api_token)
all_ingest = all_ingest.json()

# change dictionary keys because there are duplicate key names in the dictionary
for l in all_ingest:
    l['ingest_status'] = l.pop('status')
    l['ingest_id'] = l.pop('id')
    l['entryDateStr'] = nc.num2date(l['entryDate'], 'milliseconds since 1970-01-01').strftime('%Y-%m-%d %H:%M:%S')
    l['modifiedDateStr'] = nc.num2date(l['modifiedDate'], 'milliseconds since 1970-01-01').strftime('%Y-%m-%d %H:%M:%S')

# load the json into a pandas dataframe
all_ingest = json_normalize(all_ingest, 'ingestRequestFileMasks', ['username', 'type', 'ingest_status', 'ingest_id', 'state', 'priority', 'entryDateStr', 'modifiedDateStr'])
all_ingest = pd.concat([all_ingest.drop(['refDes'], axis=1), all_ingest['refDes'].apply(pd.Series)], axis=1)
all_ingest['refDes'] = all_ingest['subsite'] + '-' + all_ingest['node'] + '-' + all_ingest['sensor']
all_ingest = all_ingest.drop(['node', 'sensor', 'subsite'], axis=1)
df_telemetered = all_ingest.loc[(all_ingest['state'] == 'RUN') & (all_ingest['type'] == 'TELEMETERED')]
df_telemetered['changed_status'] = None
df_telemetered['purged'] = None

print('\nUnique Reference Designators in these ingestion CSV files')
for rd in unique_ref_des:
    print rd
    recurring = recurring.append(df_telemetered[df_telemetered['refDes'] == rd], ignore_index=True)

yes = raw_input('\nPurge reference designators from the system? y/<n>: ') or 'n'

if 'y' in yes:
    print('\nThese reference designators may have ongoing recurring ingestions which MUST be cancelled/suspended before purging.')
    state_change = change_recurring_ingestion(api_key, api_token, recurring)
    for rd in unique_ref_des:
        split = rd.split('-')
        ref_des = dict(subsite=split[0], node=split[1], sensor='{}-{}'.format(split[2], split[3]))
        purge_info = purge_data(base_url, api_key, api_token, ref_des)
        purge_json = purge_info.json()
        tdf = pd.DataFrame([purge_json], columns=purge_json.keys())
        tdf['ReferenceDesignator'] = rd
        purge_df = purge_df.append(tdf)
    print('Purge Completed')
    print(purge_df)
else:
    if 'telemetered' in serve:
        state_change = change_recurring_ingestion(api_key, api_token, recurring)

print('\nProceeding with data ingestion\n')

# add refDesFinal
wcard_refdes = ['GA03FLMA-RIM01-02-CTDMOG000','GA03FLMB-RIM01-02-CTDMOG000',
                'GI03FLMA-RIM01-02-CTDMOG000','GI03FLMB-RIM01-02-CTDMOG000',
                'GP03FLMA-RIM01-02-CTDMOG000','GP03FLMB-RIM01-02-CTDMOG000',
                'GS03FLMA-RIM01-02-CTDMOG000','GS03FLMB-RIM01-02-CTDMOG000']

df['refDesFinal'] = ''

for row in df.iterrows():

    if '#' in row[1]['parserDriver']:
        continue
    elif row[1]['parserDriver']:
        rd = row[1].refDes
        if rd in wcard_refdes:
            row[1].refDesFinal = 'false' # the CTDMO decoder will be invoked
        else:
            row[1].refDesFinal = 'true' # the CTDMO decoder will not be invoked

        ingest_dict = build_ingest_dict(row[1].to_dict())
        r = ingest_data(base_url, api_key, api_token, ingest_dict)
        ingest_json = r.json()
        tdf = pd.DataFrame([ingest_json], columns=ingest_json.keys())
        tdf['ReferenceDesignator'] = row[1]['refDes']
        tdf['state'] = row[1]['state']
        tdf['type'] = row[1]['type']
        tdf['deployment'] = row[1]['deployment']
        tdf['username'] = row[1]['username']
        tdf['priority'] = row[1]['priority']
        tdf['refDesFinal'] = row[1]['refDesFinal']
        tdf['fileMask'] = row[1]['fileMask']
        ingest_df = ingest_df.append(tdf)
    else:
        continue

print ingest_df
utc_time = dt.datetime.utcnow().strftime('%Y%m%d_%H%M%S')

purge_df.to_csv('{}_purged.csv'.format(utc_time), index=False)
ingest_df.to_csv('{}_ingested.csv'.format(utc_time), index=False)