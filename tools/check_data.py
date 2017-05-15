#!/usr/bin/env python
"""
@file check_data.py
@author Mike Smith
@email michaesm@marine.rutgers.edu
@brief Read in thredds catalog .xml file, crawls server for .ncml files and perform a series of checks on the datasets.
@purpose The purpose is to provide a quick way for the Rutgers OOICI data team to quickly analyze data produced via uFrame.
@usage Create a python script that imports check_data from the tools folder
@ example
from tools import check_data
url = 'https://opendap.oceanobservatories.org/thredds/catalog/ooi/friedrich-knuth-gmail/20170123T165201-RS03AXBS-MJ03A-06-PRESTA301-streamed-prest_real_time/catalog.xml'
save_dir = '/Users/mikesmith/Documents/'
check_data.main(url, save_dir)
"""

import requests
import os
from thredds_crawler.crawl import Crawl
import xarray as xr
import pandas as pd
import re
import numpy as np
from datetime import datetime as dt
from haversine import haversine as distance
import logging
from collections import OrderedDict
import json

t_now = dt.now().strftime('%Y%m%d_%H%M00')
logging.basicConfig(filename='check_data_{}.log'.format(t_now), level=logging.DEBUG)


def make_dir(save_dir):
    try:  # Check if the save_dir exists already... if not, make it
        os.mkdir(save_dir)
    except OSError:
        pass


def atoi(text):
    return int(text) if text.isdigit() else text


def natural_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    '''
    return [atoi(c) for c in re.split('(\d+)', text)]


def reject_outliers(data, m=3):
    # function to reject outliers beyond 3 standard deviations of the mean.
    # data: numpy array containing data
    # m: the number of standard deviations from the mean. Default: 3
    return abs(data - np.nanmean(data)) < m * np.nanstd(data)


def test_gaps(df):
    gap_list = []
    df['diff'] = df['time'].diff()
    index_gap = df['diff'][df['diff'] > pd.Timedelta(days=1)].index.tolist()
    for i in index_gap:
        gap_list.append([pd.to_datetime(str(df['time'][i-1])).strftime('%Y-%m-%dT%H:%M:%SZ'),
                         pd.to_datetime(str(df['time'][i])).strftime('%Y-%m-%dT%H:%M:%SZ')])

    return gap_list


def eliminate_common_variables(list):
    common = ['quality_flag', 'provenance', 'id', 'deployment'] #time is in this list because it doesn't show up as a variable in an xarray ds
    regex = re.compile(r'\b(?:%s)\b' % '|'.join(common))
    list = [s for s in list if not regex.search(s)]
    return list


def insert_into_dict(d, key, value):
    if not key in d:
        d[key] = [value]
    else:
        d[key].append(value)
    return d


def request_qc_json(ref_des):
    url = 'http://ooi.visualocean.net/instruments/view/'
    ref_des_url = os.path.join(url, ref_des)
    ref_des_url += '.json'
    data = requests.get(ref_des_url).json()
    return data


def get_parameter_list(data):
    streams = {}

    for stream in data['instrument']['data_streams']:
        params = stream['stream']['parameters']
        for param in params:
            insert_into_dict(streams, stream['stream_name'], param['name'])
    return streams


def get_deployment_information(data, deployment):
    d_info = [x for x in data['instrument']['deployments'] if x['deployment_number'] == deployment]

    if d_info:
        return d_info[0]
    else:
        return None


def compare_lists(list1, list2):
    match = []
    unmatch = []
    for i in list1:
        if i in list2:
            match.append(i)
        else:
            unmatch.append(i)
    return match, unmatch


def get_global_ranges(platform, node, sensor, variable, api_user=None, api_token=None):
    port = '12578'
    base_url = '{}/qcparameters/inv/{}/{}/{}/'.format(port, platform, node, sensor)
    url = 'https://ooinet.oceanobservatories.org/api/m2m/{}'.format(base_url)
    if (api_user is None) or (api_token is None):
        r = requests.get(url, verify=False)
    else:
        r = requests.get(url, auth=(api_user, api_token), verify=False)

    if r.status_code is 200:
        if r.json():  # If r.json is not empty
            values = pd.io.json.json_normalize(r.json())
            t1 = values[values['qcParameterPK.streamParameter'] == variable]
            if not t1.empty:
                t2 = t1[t1['qcParameterPK.qcId'] == 'dataqc_globalrangetest_minmax']
                if not t2.empty:
                    local_min = float(t2[t2['qcParameterPK.parameter'] == 'dat_min'].iloc[0]['value'])
                    local_max = float(t2[t2['qcParameterPK.parameter'] == 'dat_max'].iloc[0]['value'])
                else:
                    local_min = None
                    local_max = None
            else:
                local_min = None
                local_max = None
        else:
            local_min = None
            local_max = None
    else:
        local_min = None
        local_max = None
    return [local_min, local_max]


def parse_qc(ds):
    vars = [x.split('_qc_results')[0] for x in ds.data_vars if 'qc_results' in x]
    results = [x+'_qc_results' for x in vars]
    executed = [x+'_qc_executed' for x in vars]
    key_list = vars + results + executed

    if not vars:
        # No variables were qc'ed for some reason
        df = ds['id'].to_dataframe().drop(['lat', 'lon'], axis=1)
    else:
        df = ds[key_list].to_dataframe().drop(['lat', 'lon'], axis=1)


        for var in vars:
            qc_result = var + '_qc_results'
            qc_executed = var + '_qc_executed'
            names = {
                0: var + '_global_range_test',
                1: var + '_dataqc_localrangetest',
                2: var + '_dataqc_spiketest',
                3: var + '_dataqc_polytrendtest',
                4: var + '_dataqc_stuckvaluetest',
                5: var + '_dataqc_gradienttest',
                7: var + '_dataqc_propagateflags',
            }
            # Just in case a different set of tests were run on some datapoint
            # *this should never happen*
            executed = np.bitwise_or.reduce(df[qc_executed].values)
            executed_bits = np.unpackbits(executed.astype('uint8'))
            for index, value in enumerate(executed_bits[::-1]):
                if value:
                    name = names.get(index)
                    mask = 2 ** index
                    values = (df[qc_result].values & mask) > 0
                    df[name] = values
            df.drop([qc_executed, qc_result], axis=1, inplace=True)
    return df


def find(lst, key, value):
    for i, dic in enumerate(lst):
        if dic[key] == value:
            return i
    return -1


def main(url, save_dir):
    if type(url) is str:
        if url.endswith('.html'):
            url = url.replace('.html', '.xml')
            tds_url = 'https://opendap.oceanobservatories.org/thredds/dodsC'
            c = Crawl(url, select=[".*\.nc$"], debug=False)
            datasets = [os.path.join(tds_url, x.id) for x in c.datasets]
            splitter = url.split('/')[-2].split('-')
        elif url.endswith('.xml'):
            tds_url = 'https://opendap.oceanobservatories.org/thredds/dodsC'
            c = Crawl(url, select=[".*\.nc$"], debug=False)
            datasets = [os.path.join(tds_url, x.id) for x in c.datasets]
            splitter = url.split('/')[-2].split('-')
        elif url.endswith('.nc') or url.endswith('.ncml'):
            datasets = [url]
            splitter = url.split('/')[-2].split('-')
        else:
            print 'Unrecognized input. Input must be a string of the file location(s) or list of file(s)'
    else:
        print 'Dataset must be in a string.'

    data = OrderedDict(deployments=OrderedDict())
    for dataset in datasets:
        filename = os.path.basename(dataset)
        if 'ENG000000' not in filename:  # script will not analyze glider ENG data files
            logging.info('Processing {}'.format(str(dataset)))
            try:
                print 'Opening file: {}'.format(dataset)
                with xr.open_dataset(dataset, mask_and_scale=False) as ds:
                    ref_des = '{}-{}-{}'.format(ds.subsite, ds.node, ds.sensor)
                    deployment = np.unique(ds['deployment'].data)[0]

                    qc_data = request_qc_json(ref_des)  # grab data from the qc database
                    ref_des_dict = get_parameter_list(qc_data)
                    deploy_info = get_deployment_information(qc_data, deployment)

                    if deploy_info is None:
                        print 'info from deployment ' + str(deployment) + ' does not match data'
                        continue

                    data_start = ds.time_coverage_start + 'Z'
                    data_end = ds.time_coverage_end + 'Z'


                    # Deployment Variables
                    deploy_start = str(deploy_info['start_date'] + 'Z')
                    if deploy_info['stop_date']:
                        deploy_stop = str(deploy_info['stop_date'] + 'Z')
                    else:
                        deploy_stop = str(deploy_info['stop_date'])
                    deploy_lon = deploy_info['longitude']
                    deploy_lat = deploy_info['latitude']

                    # Add reference designator to dictionary
                    try:
                        data['ref_des']
                    except KeyError:
                        data['ref_des'] = ref_des

                    deployment = 'D0000{}'.format(deployment)

                    deployments = data['deployments'].keys()

                    # Add deployment to dictionary and initialize stream sub dictionary
                    if not deployment in deployments:
                        data['deployments'][deployment] = OrderedDict(start=deploy_start,
                                                               end=deploy_stop,
                                                               lon=deploy_lon,
                                                               lat=deploy_lat,
                                                               streams=OrderedDict(),
                                                               data_times = dict(start=[], end=[]))

                    # Add data start and stop times to a data_times array. When the files are all processed, it checks data vs deployment times
                    if ds.stream == splitter[-1]:
                        data['deployments'][deployment]['data_times']['start'].append(data_start)
                        data['deployments'][deployment]['data_times']['end'].append(data_end)


                    streams = data['deployments'][deployment]['streams'].keys()

                    # Add stream to subdictionary inside deployment
                    if not ds.stream in streams:
                        data['deployments'][deployment]['streams'][ds.stream] = OrderedDict(files=OrderedDict())

                    qc_df = parse_qc(ds)

                    qc_vars = [x for x in qc_df.keys() if not 'test' in x]
                    qc_df = qc_df.reset_index()
                    variables = ds.data_vars.keys()
                    variables = eliminate_common_variables(variables)
                    variables = [x for x in variables if not 'qc' in x] # remove qc variables, because we don't care about them

                    # Gap test. Get a list of gaps
                    gap_list = test_gaps(qc_df)

                    # Deployment Distance
                    data_lat = np.unique(ds['lat'])[0]
                    data_lon = np.unique(ds['lon'])[0]
                    dist_calc = distance((deploy_lat, deploy_lon), (data_lat, data_lon))

                    # Unique times
                    time = ds['time']
                    len_time = time.__len__()
                    len_time_unique = np.unique(time).__len__()
                    if len_time == len_time_unique:
                        time_test = True
                    else:
                        time_test = False
                    db_list = ref_des_dict[ds.stream]

                    [_, unmatch1] = compare_lists(db_list, variables)
                    [_, unmatch2] = compare_lists(variables, db_list)

                    filenames = data['deployments'][deployment]['streams'][ds.stream]['files']

                    if not filename in filenames:
                        data['deployments'][deployment]['streams'][ds.stream]['files'][filename] = OrderedDict(data_start=data_start,
                                                                                                        data_end=data_end,
                                                                                                        time_gaps=gap_list,
                                                                                                        lon=data_lon,
                                                                                                        lat=data_lat,
                                                                                                        distance_from_deploy_km=dist_calc,
                                                                                                        unique_times=str(time_test),
                                                                                                        variables = OrderedDict(),
                                                                                                        vars_not_in_file=unmatch1,
                                                                                                        vars_not_in_db=unmatch2)
                    else:
                        print filename + ' already in dictionary. Skipping'

                    for v in variables:
                        # print v
                        # Availability test
                        if v in db_list:
                            available = True
                        else:
                            available = False

                        if ds[v].dtype.kind == 'S' \
                                or ds[v].dtype == np.dtype('datetime64[ns]') \
                                or 'time' in v:
                            dict_vars = data['deployments'][deployment]['streams'][ds.stream]['files'][filename]['variables'].keys()

                            if not v in dict_vars:
                                data['deployments'][deployment]['streams'][ds.stream]['files'][filename]['variables'][v] = OrderedDict(available = str(available))
                            continue
                        else:
                            var_data = ds[v].data

                            # NaN test. Make sure the parameter is not all NaNs
                            nan_test = np.all(np.isnan(var_data))
                            if not nan_test or available is False:
                                # Global range test
                                [g_min, g_max] = get_global_ranges(ds.subsite, ds.node, ds.sensor, v)
                                try:
                                    ind = reject_outliers(var_data, 3)
                                    min = float(np.nanmin(var_data[ind]))
                                    max = float(np.nanmax(var_data[ind]))
                                except (TypeError, ValueError):
                                    min = None
                                    max = None

                                # Fill Value test
                                try:
                                    fill_value = float(ds[v]._FillValue)
                                    fill_test = np.any(var_data == ds[v]._FillValue)
                                except AttributeError:
                                    fill_value = None
                                    fill_test = None

                                dict_vars = data['deployments'][deployment]['streams'][ds.stream]['files'][filename]['variables'].keys()
                                if not v in dict_vars:
                                    data['deployments'][deployment]['streams'][ds.stream]['files'][filename]['variables'][v] = OrderedDict(available=str(available),
                                                                                                                                    all_nans=str(nan_test),
                                                                                                                                    data_min = min,
                                                                                                                                    data_max = max,
                                                                                                                                    global_min=g_min,
                                                                                                                                    global_max=g_max,
                                                                                                                                    fill_test=str(fill_test),
                                                                                                                                    fill_value=fill_value)

                                if v in qc_vars:
                                    temp_list = []
                                    tests = ['global_range_test', 'dataqc_stuckvaluetest', 'dataqc_spiketest']
                                    for test in tests:
                                        var = '{}_{}'.format(v, test)
                                        group_var = 'group_{}'.format(var)
                                        try:
                                            qc_df[group_var] = qc_df[var].diff().cumsum().fillna(0)
                                        except KeyError as e:
                                            # logging.warn('Error: P')
                                            temp_list.append(['Did not run'])
                                            continue
                                        tdf = qc_df.groupby([group_var, var])['time'].agg(['first', 'last'])
                                        tdf = tdf.reset_index().drop([group_var], axis=1)
                                        tdf = tdf.loc[tdf[var] ==  False].drop(var, axis=1)
                                        tdf['first'] = tdf['first'].apply(lambda x: x.strftime('%Y-%m-%dT%H:%M:%SZ'))
                                        tdf['last'] = tdf['last'].apply(lambda x: x.strftime('%Y-%m-%dT%H:%M:%SZ'))
                                        if tdf.empty:
                                            data['deployments'][deployment]['streams'][ds.stream]['files'][filename]['variables'][v][test] = []
                                        else:
                                            data['deployments'][deployment]['streams'][ds.stream]['files'][filename]['variables'][v][test] = map(list, tdf.values)

                                else:
                                    data['deployments'][deployment]['streams'][ds.stream]['files'][filename]['variables'][v]['global_range_test'] = None
                                    data['deployments'][deployment]['streams'][ds.stream]['files'][filename]['variables'][v]['dataqc_stuckvaluetest'] = None
                                    data['deployments'][deployment]['streams'][ds.stream]['files'][filename]['variables'][v]['dataqc_spiketest'] = None
                            else:
                                dict_vars = data['deployments'][deployment]['streams'][ds.stream]['files'][filename]['variables'].keys()
                                if not v in dict_vars:
                                    data['deployments'][deployment]['streams'][ds.stream]['files'][filename]['variables'][v] = OrderedDict(available=str(available), all_nans=str(nan_test))
            except Exception as e:
                logging.warn('Error: Processing failed due to {}.'.format(str(e)))
                raise
        else:
            pass

    deployments = data['deployments'].keys()
    for d in deployments:
        data['deployments'][d]['data_times']['start'].sort(key=natural_keys)
        data['deployments'][d]['data_times']['end'].sort(key=natural_keys)

        data['deployments'][d]['data_times']['start'] = data['deployments'][d]['data_times']['start'][0]
        data['deployments'][d]['data_times']['end'] = data['deployments'][d]['data_times']['end'][-1]

    make_dir(save_dir)
    save_file = os.path.join(save_dir, '{}-{}-{}-{}__{}-{}__requested-{}.json'.format(splitter[1], splitter[2], splitter[3], splitter[4], splitter[5], splitter[6], splitter[0]))
    with open(save_file, 'w') as outfile:
        json.dump(data,outfile)
    return save_file

if __name__ == '__main__':
    # change pandas display width to view longer dataframes
    desired_width = 320
    pd.set_option('display.width', desired_width)
    url = 'https://opendap.oceanobservatories.org/thredds/catalog/ooi/michaesm-marine-rutgers/20170317T160317-CE09OSSM-RID26-07-NUTNRB000-recovered_inst-nutnr_b_instrument_recovered/catalog.html'
    save_dir = '/Users/mikesmith/Documents/'

    main(url, save_dir)