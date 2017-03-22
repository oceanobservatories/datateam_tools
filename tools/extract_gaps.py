#!/usr/bin/env python
"""
@file extract_gaps.py
@author Lori Garzio
@email lgarzio@marine.rutgers.edu
@brief Read the .json output from analyze_nc_data.py and extract the data gaps for each file in a directory
@purpose Provide a human-readable file for data gaps
@usage
rootdir Directory that contains .json files, and where the output will be saved.
"""

import simplejson as json
import pandas as pd
import os

rootdir = '/Users/lgarzio/Documents/OOI/DataReviews'

def extract_gaps(rootdir):
    for root, dirs, files in os.walk(rootdir):
        for filename in files:
            if filename.endswith('.json'):
                print filename
                f = os.path.join(root,filename)
                file = open(f, 'r')
                data = json.load(file)

                ref_des = data.get('ref_des')
                for item in data['deployments']:
                    deployment = item['name']
                    deploy_begin = item['begin']
                    deploy_end = item['end']
                    for s in item['streams']:
                        for x in s['files']:
                            fname = x['name']
                            gaps = x['time_gaps']
                            data_begin = x['data_start']
                            data_end = x['data_end']

                            try:
                                print df
                                df.append((ref_des,fname,deployment,deploy_begin,deploy_end,data_begin,data_end,gaps))
                            except NameError:
                                df = []
                                df.append((ref_des,fname,deployment,deploy_begin,deploy_end,data_begin,data_end,gaps))

    df = pd.DataFrame(df, columns=['ref_des', 'filename', 'deployment', 'deploy_begin', 'deploy_end', 'data_begin (file)', 'data_end (file)', 'gaps'])
    df.to_csv(os.path.join(rootdir, ref_des + '-data_gaps.csv'), index=False)

extract_gaps(rootdir)