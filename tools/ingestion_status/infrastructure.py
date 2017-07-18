#! /usr/local/bin/python

"""
Created on Wen Feb 01 2017

#@author: leila
"""

import pandas as pd
import os

platform_name = 'CP05MOAS'
glider_name = 'GL335'
filed = '/Users/leila/Documents/OOI_GitHub_repo/repos/Sage_seagrinch/data-team-python/infrastructure/' + 'data_streams.csv'


rfd = pd.read_csv(filed)
rfd['platform'] = rfd['reference_designator'].str.split('_').str[0].str[0:8]
#rfd['GLxxx'] = rfd['reference_designator'].str.split('_').str[1].str[0:5]

ind_r = rfd.loc[(rfd['platform'] == platform_name)]
refdes = list(pd.unique(ind_r['reference_designator'].ravel()))



refdes_list = []
method_list = []
type_list = []

for rf in refdes:
    pl = rf.split('-')[0][4:8]
    gl = rf.split('-')[1][0:5]
    if pl == 'MOAS':
        if gl == glider_name:
            print rf
            ind_s = rfd.loc[(rfd['reference_designator'] == rf)]
            method = list(pd.unique(ind_s['method'].ravel()))
            type = list(pd.unique(ind_s['instrument_type'].ravel()))
            for md in method:
                refdes_list.append(rf)
                method_list.append(md)
                type_list.append(type[0])
    else:
        ind_s = rfd.loc[(rfd['reference_designator'] == rf)]
        method = list(pd.unique(ind_s['method'].ravel()))
        type = list(pd.unique(ind_s['instrument_type'].ravel()))
        for md in method:
            refdes_list.append(rf)
            method_list.append(md)
            type_list.append(type[0])

print len(refdes_list), refdes_list
print len(method_list), method_list
print len(type_list), type_list


df = pd.DataFrame(refdes_list,columns=['refdes_list'])
df['method_list'] = method_list
df['type_list'] = type_list

column = ['refdes_list','method_list','type_list']
outputfile = '/Users/leila/Documents/OOI_GitHub_repo/work/ingest-status/000_ingestpy_run_results/' + platform_name + '_infrastructure.csv'
df.to_csv(outputfile, index=False, columns=column, na_rep='NaN', encoding='utf-8')