import requests
import os
from thredds_crawler.crawl import Crawl
import xarray as xr
import pandas as pd
import re
import numpy as np
from datetime import datetime as dt
from haversine import haversine as distance


def test_gaps(time):
    gap_list = []
    df = pd.DataFrame(data=time, columns=['time'])
    df['diff'] = df['time'].diff()
    index = df['diff'][df['diff'] > pd.Timedelta(days=1)].index.tolist()
    for i in index:
        gap_list.append([pd.to_datetime(str(time[i-1])).strftime('%Y-%m-%d %H:%M:%S'),
                         pd.to_datetime(str(time[i+1])).strftime('%Y-%m-%d %H:%M:%S')])
    return gap_list


def eliminate_common_variables(list):
    common = ['quality_flag', 'provenance', 'id', 'deployment'] #time is in this list because it doesn't show up as a variable in an xarray ds
    regex = re.compile('|'.join(common))
    list = [s for s in list if not regex.search(s)]
    return list


def insert_into_dict(d, key, value):
    if not key in d:
        d[key] = [value]
    else:
        d[key].append(value)
    return d


def request_qc_json(ref_des):
    url = 'https://ooi.visualocean.net/instruments/view/'
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
        if r.json(): # If r.json is not empty
            values = pd.io.json.json_normalize(r.json())
            t1 = values[values['qcParameterPK.streamParameter'] == variable]
            if not t1.empty:
                t2 = t1[t1['qcParameterPK.qcId'] == 'dataqc_globalrangetest_minmax']
                if not t2.empty:
                    local_min = float(t2[t2['qcParameterPK.parameter'] == 'dat_min'].iloc[0]['value'])
                    local_max = float(t2[t2['qcParameterPK.parameter'] == 'dat_max'].iloc[0]['value'])
                else:
                    local_min = 'N/A'
                    local_max = 'N/A'
            else:
                local_min = 'N/A'
                local_max = 'N/A'
        else:
            local_min = 'N/A'
            local_max = 'N/A'
    return [local_min, local_max]


def unpack_bits(df, var):
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
            mask = 2**index
            values = (df[qc_result].values & mask) > 0
            df[name] = values
    df.drop([qc_executed, qc_result], axis=1, inplace=True)
    return df


def parse_qc(ds):
    # keys = [val for sublist in [[x.split('_qc_results')[0], x, x.replace('_qc_results', '_qc_executed')] for x in ds.data_vars if 'qc_results' in x] for val in sublist]
    # vars = [var.split('_qc_results')[0] for var in keys if '_qc_results' in var]
    # df = ds[keys].to_dataframe()
    # df = df.set_index('time')
    # df = df.drop(['lat', 'lon'], axis=1)
    df = pd.DataFrame([])
    key_list = [[x.split('_qc_results')[0], x, x.replace('_qc_results', '_qc_executed')] for x in ds.data_vars if 'qc_results' in x]
    n = 0

    for key in key_list:
        var = [var.split('_qc_results')[0] for var in key if '_qc_results' in var][0]
        df_temp = ds[key].to_dataframe()
        df_temp = df_temp.set_index('time')
        df_temp = df_temp.drop(['lat', 'lon'], axis=1)
        df_temp = unpack_bits(df_temp, var)
        # df_temp = df_temp.reset_index()
        if n == 0:
            df = df_temp
            n = 1
        else:
            df = pd.concat([df, df_temp], axis=1, join_axes=[df.index])
    return df


def main(url, save_dir):
    tds_url = 'http://opendap.oceanobservatories.org/thredds/dodsC'
    c = Crawl(url, select=[".*ncml"])
    data = []

    for n in c.datasets:
        ncml_url = os.path.join(tds_url, n.id)
        with xr.open_dataset(ncml_url, mask_and_scale=False) as ds:
            qc_df = parse_qc(ds)
            qc_vars = [x for x in qc_df.keys() if not 'test' in x]
            qc_df = qc_df.reset_index()
            deployment = np.unique(ds['deployment'].data)[0]
            variables = ds.data_vars.keys()
            variables = eliminate_common_variables(variables)
            variables = [x for x in variables if not 'qc' in x] # remove qc variables, because we don't care about them
            ref_des = '{}-{}-{}'.format(ds.subsite, ds.node,ds.sensor)
            qc_data = request_qc_json(ref_des) # grab data from the qc database
            ref_des_dict = get_parameter_list(qc_data)
            deploy_info = get_deployment_information(qc_data, deployment)

            # Gap test. Get a list of gaps
            gap_list = test_gaps(ds['time'].data)

            # Deployment Variables
            deploy_start = deploy_info['start_date']
            deploy_stop = deploy_info['stop_date']
            deploy_lon = deploy_info['longitude']
            deploy_lat = deploy_info['latitude']
            # deploy_water = deploy_info['water_depth']
            # deploy_depth = deploy_info['deployment_depth']

            # Deployment Time
            data_start = ds['time'].data.min()
            data_stop = ds['time'].data.max()

            if deploy_start is not None:
                start = data_start > np.datetime64(dt.strptime(deploy_start, '%Y-%m-%dT%H:%M:%S'))
            else:
                start = None
            start_test = '{} [{}, {}]'.format(start, deploy_start, data_start)

            if deploy_stop is not None:
                stop = data_stop < np.datetime64(dt.strptime(deploy_stop, '%Y-%m-%dT%H:%M:%S'))
            else:
                stop = None
            stop_test = '{} [{}, {}]'.format(stop, deploy_stop, data_stop)

            # Deployment Distance
            data_lat = np.unique(ds['lat'].data)[0]
            data_lon = np.unique(ds['lon'].data)[0]
            dist_calc = distance((deploy_lat, deploy_lon), (data_lat, data_lon))
            if dist_calc < .5: # if distance is less than .5 km
                dist = True
            else:
                dist = False
            dist_test = '{} [{} km]'.format(dist, dist_calc)

            # Unique times
            time = ds['time'].data
            len_time = time.__len__()
            len_time_unique = np.unique(time).__len__()
            if len_time == len_time_unique:
                time_test = True
            else:
                time_test = False

            # depth_vars = [x for x in variables if re.compile('|'.join(['depth', 'pressure'])).search(x)]
            # tests = [x for x in [x for x in qc_df.keys() if 'test' in x] if not re.compile('|'.join(['gradient', 'trend', 'local'])).search(x)]

            for v in variables:
                var_data = ds[v].data
                print v
                # Availability test
                if v in ref_des_dict[ds.stream]:
                    available = True
                else:
                    available = False

                # Global range test
                [g_min, g_max] = get_global_ranges(ds.subsite, ds.node, ds.sensor, v)
                try:
                    min = np.nanmin(var_data)
                    max = np.nanmax(var_data)
                except TypeError:
                    min = None
                    max = None

                if g_min is not None:
                    if min >= g_min:
                        if max <= g_max:
                            gr_result = True
                        else:
                            gr_result = False
                    else:
                        gr_result = False
                else:
                    gr_result = None

                # Fill Value test
                fill_test = np.all(var_data == ds[v]._FillValue)

                try:
                    # NaN test. Make sure the parameter is not all NaNs
                    nan_test = np.all(np.isnan(var_data));
                except TypeError:
                    nan_test = 'N/A'

                qc_dict = {}
                if v in qc_vars:
                    # tests = [x for x in [x for x in qc_df.keys() if v in x] if re.compile('|'.join(['global_range', 'spiketest', 'stuckvalue'])).search(x)]
                    tests = ['dataqc_spiketest', 'global_range_test', 'dataqc_stuckvaluetest']
                    for test in tests:
                        var = '{}_{}'.format(v, test)
                        group_var = 'group_{}'.format(var)
                        qc_df[group_var] = qc_df[var].diff().cumsum().fillna(0)
                        tdf = qc_df.groupby([group_var, var])['time'].agg(['first', 'last'])
                        tdf = tdf.reset_index().drop([group_var], axis=1)
                        tdf = tdf.loc[tdf[var] == False].drop(var, axis=1)
                        tdf['first'] = tdf['first'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
                        tdf['last'] = tdf['last'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
                        if tdf.empty:
                            qc_dict[test] = []
                        else:
                            qc_dict[test] = map(list, tdf.values)
                            # qc_dict[test] = qc_df.groupby([group_var, var])['time'].agg(['first', 'last'])
                        # print result
                    data.append((ref_des, ds.stream, deployment,
                                 start_test, stop_test, dist_test,
                                 time_test, v, available, gr_result,
                                 [g_min, min], [g_max, max], fill_test,
                                 ds[v]._FillValue, nan_test, gap_list,
                                 qc_dict['global_range_test'],
                                 qc_dict['dataqc_stuckvaluetest'],
                                 qc_dict['dataqc_spiketest']))
                else:
                    data.append((ref_des, ds.stream, deployment, start_test, stop_test, dist_test, time_test,
                                 v, available, gr_result, [g_min, min], [g_max, max], fill_test, ds[v]._FillValue,
                                 nan_test, gap_list, 'N/A', 'N/A', 'N/A'))


    df = pd.DataFrame(data, columns=['ref_des', 'stream', 'deployment',
                                     'start[deploy,data]', 'stop[deploy,data]', 'distance_from_deploy_<=.5km',
                                     'time_unique', 'variable', 'availability',
                                     'global_range_test', 'min[global, data]', 'max[global, data]',
                                     'fill_test', 'fill_value', 'all_nans',
                                     'gaps', 'global_range', 'stuck_value', 'spike_test'])
    df.to_csv(os.path.join(save_dir, '{}-{}-{}_RIC_{}.csv'.format(ds.subsite, ds.node, ds.sensor, dt.now().strftime('%Y-%m-%dT%H%M00'))), index=False)

if __name__ == '__main__':
    # change pandas display width to view longer dataframes
    desired_width = 320
    pd.set_option('display.width', desired_width)
    url = 'https://opendap.oceanobservatories.org/thredds/catalog/ooi/m-smith3887-gmail/20170105T205028-CE01ISSM-MFD37-03-CTDBPC000-telemetered-ctdbp_cdef_dcl_instrument/catalog.xml'
    save_dir = '/Users/michaesm/Documents/'
    main(url, save_dir)