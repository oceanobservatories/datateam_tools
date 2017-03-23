#!/usr/bin/env python
"""
@file extract_gaps.py
@author Lori Garzio
@email lgarzio@marine.rutgers.edu
@brief Read the .json output from analyze_nc_data.py and extract the data gaps
@purpose Provide a human-readable file for data gaps
@usage
dataset Path to .json output from analyze_nc_data.py
save_dir Location to save output
"""

import simplejson as json
import pandas as pd
import os

dataset = '/Users/lgarzio/Documents/OOI/DataReviews/CE06ISSM-RID16-07-NUTNRB000_recovered_inst-nutnr_b_instrument_recovered-processed_on_2017-03-22T172633.json'
save_dir = '/Users/lgarzio/Documents/OOI/DataReviews/'

def extract_gaps(dataset,save_dir):
    file = open(dataset, 'r')
    data = json.load(file)
    ref_des = data.get('ref_des')
    for d in data['deployments']:
        deployment = d
        deploy_begin = data['deployments'][d]['begin']
        deploy_end = data['deployments'][d]['end']
        for s in data['deployments'][d]['streams']:
            stream = s
            for x in data['deployments'][d]['streams'][s]['files']:
                fName = x
                gaps = data['deployments'][d]['streams'][s]['files'][x]['time_gaps']
                data_begin = data['deployments'][d]['streams'][s]['files'][x]['data_start']
                data_end = data['deployments'][d]['streams'][s]['files'][x]['data_end']

                try:
                    df
                    df.append((ref_des,fName,stream,deployment,deploy_begin,deploy_end,data_begin,data_end,gaps))
                except NameError:
                    df = []
                    df.append((ref_des,fName,stream,deployment,deploy_begin,deploy_end,data_begin,data_end,gaps))

    df = pd.DataFrame(df, columns=['ref_des', 'filename', 'stream', 'deployment', 'deploy_begin', 'deploy_end',
                                   'data_begin (file)', 'data_end (file)', 'gaps'])
    df.to_csv(os.path.join(save_dir, ref_des + '-data_gaps.csv'), index=False)

extract_gaps(dataset,save_dir)