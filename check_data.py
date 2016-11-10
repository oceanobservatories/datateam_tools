import requests
import os
from thredds_crawler.crawl import Crawl
import xarray as xr
import pandas as pd
import re
import numpy as np
import datetime as dt


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
    common = ['quality_flag', 'provenance', 'id', 'deployment', 'time'] #time is in this list because it doesn't show up as a variable in an xarray ds
    regex = re.compile('|'.join(common))
    list = [s for s in list if not regex.search(s)]
    return list


def insert_into_dict(d, key, value):
    if not key in d:
        d[key] = [value]
    else:
        d[key].append(value)
    return d

def get_parameter_list(ref_des):
    url = 'https://ooi.visualocean.net/instruments/view/'
    ref_des_url = os.path.join(url, ref_des)
    ref_des_url += '.json'

    data = requests.get(ref_des_url).json()

    streams = {}

    for stream in data['instrument']['data_streams']:
        params = stream['stream']['parameters']
        for param in params:
            insert_into_dict(streams, stream['stream_name'], param['name'])
    return streams

def compare_lists(list1, list2):
    match = []
    unmatch = []
    for i in list1:
        if i in list2:
            match.append(i)
        else:
            unmatch.append(i)
    return match, unmatch


def get_global_ranges(platform, node, sensor, variable):
    url = 'http://ooiufs01.ooi.rutgers.edu:12578/qcparameters/inv/{}/{}/{}/'.format(platform, node, sensor)

    r = requests.get(url)

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
                    local_min = None
                    local_max = None
            else:
                local_min = None
                local_max = None
        else:
            local_min = None
            local_max = None
    return [local_min, local_max]

def main(url, save_dir):
    tds_url = 'http://opendap.oceanobservatories.org/thredds/dodsC'
    c = Crawl(url, select=[".*ncml"])
    data = []

    for n in c.datasets:
        ncml_url = os.path.join(tds_url, n.id)
        with xr.open_dataset(ncml_url, mask_and_scale=False) as ds:
            deployment = np.unique(ds['deployment'].data)[0]
            variables = ds.data_vars.keys()
            variables = eliminate_common_variables(variables)
            variables = [x for x in variables if not 'qc' in x] # remove qc variables, because we don't care about them
            ref_des = '{}-{}-{}'.format(ds.subsite, ds.node,ds.sensor)
            ref_des_dict = get_parameter_list(ref_des)

            # Gap test. Get a list of gaps
            gap_list = test_gaps(ds['time'].data)

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
                    min = 'n/a'
                    max = 'n/a'

                if g_min is not None:
                    if min > g_min:
                        if max < g_max:
                            gr_result = True
                        else:
                            gr_result = False
                    else:
                        gr_result = False
                else:
                    gr_result = 'None'

                # Fill Value test
                fill_test = np.all(var_data == ds[v]._FillValue)

                try:
                    # NaN test. Make sure the parameter is not all NaNs
                    nan_test = np.all(np.isnan(var_data));
                except TypeError:
                    nan_test = 'None'

                data.append((ref_des, ds.stream, deployment, v, available, gr_result, [g_min, min], [g_max, max], fill_test,
                             ds[v]._FillValue, nan_test, gap_list))
        df = pd.DataFrame(data, columns=['ref_des', 'stream', 'deployment', 'variable', 'availability', 'global_range_test',
                                         'min[global, data]', 'max[global, data]', 'fill_test', 'fill_value', 'not_nan', 'gaps'])
        df.to_csv(os.path.join(save_dir, '{}-{}-{}_RIC_{}.csv'.format(ds.subsite, ds.node, ds.sensor, dt.datetime.now().strftime('%Y-%m-%dT%H%M00'))), index=False)

if __name__ == '__main__':
    url = 'https://opendap.oceanobservatories.org/thredds/catalog/ooi/friedrich-knuth-gmail/20161104T213423-RS01SUM2-MJ01B-12-ADCPSK101-streamed-adcp_velocity_beam/catalog.xml'
    save_dir = '/Users/michaesm/Documents/'
    main(url, save_dir)