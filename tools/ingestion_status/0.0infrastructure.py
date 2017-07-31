#! /usr/local/bin/python

"""
Created on Wen Feb 01 2017

#@author: leila
"""

import pandas as pd
import numpy as np
import os

platform_name = 'CP05MOAS'
glider_name = 'GL388'
filed = '/Users/leila/Documents/OOI_GitHub_repo/repos/Sage_seagrinch/data-team-python/infrastructure/' + 'data_streams.csv'


rfd = pd.read_csv(filed)
rfd.fillna('', inplace=True)
rfd['platform'] = rfd['reference_designator'].str.split('_').str[0].str[0:8]
#rfd['GLxxx'] = rfd['reference_designator'].str.split('_').str[1].str[0:5]

ind_r = rfd.loc[(rfd['platform'] == platform_name)]
refdes = list(pd.unique(ind_r['reference_designator'].ravel()))



refdes_list = []
method_list = []
type_list = []
parserDriver_list = []


for rf in refdes:
    pl = rf.split('-')[0][4:8]
    gl = rf.split('-')[1][0:5]
    if pl == 'MOAS':
        if gl == glider_name:
            filename = platform_name + '-' + glider_name
            print rf
            ind_s = rfd.loc[(rfd['reference_designator'] == rf)]
            method = list(pd.unique(ind_s['method'].ravel()))


            for md in method:
                ind_d = ind_s.loc[(ind_s['method'] == md)]
                driver = list(pd.unique(ind_d['driver'].ravel()))
                # driver.replace(np.nan, '', regex=True)
                parser = list(pd.unique(ind_d['parser'].ravel()))
                # parser.replace(np.nan, '', regex=True)
                type = list(pd.unique(ind_d['instrument_type'].ravel()))

                print 'report:'
                if len(type) > 1:
                    print len(type), 'instrument should be either science or engineering not both'
                    print type

                if len(driver) > 1:
                    print len(driver), 'more than one driver'
                    print driver

                if len(parser) > 1:
                    print len(parser), 'more than one parser'
                    print parser

                refdes_list.append(rf)
                method_list.append(md)
                type_list.append(type[0])


                parserDriver_list.append('mi.dataset.driver.' + parser[0]+ '.' + driver[0])

    else:
        filename = platform_name
        ind_s = rfd.loc[(rfd['reference_designator'] == rf)]
        method = list(pd.unique(ind_s['method'].ravel()))
        type = list(pd.unique(ind_s['instrument_type'].ravel()))
        for md in method:
            ind_d = ind_s.loc[(ind_s['method'] == md)]
            driver = list(pd.unique(ind_d['driver'].ravel()))
            # driver.replace(np.nan, '', regex=True)
            parser = list(pd.unique(ind_d['parser'].ravel()))
            # parser.replace(np.nan, '', regex=True)
            type = list(pd.unique(ind_d['instrument_type'].ravel()))

            print 'report:'
            if len(type) > 1:
                print len(type), 'instrument should be either science or engineering not both'
                print type

            if len(driver) > 1:
                print len(driver), 'more than one driver'
                print driver

            if len(parser) > 1:
                print len(parser), 'more than one parser'
                print parser

            refdes_list.append(rf)
            method_list.append(md)
            type_list.append(type[0])

            parserDriver_list.append('mi.dataset.driver.' + parser[0] + '.' + driver[0])

print len(refdes_list), refdes_list
print len(method_list), method_list
print len(type_list), type_list
print len(parserDriver_list), parserDriver_list


data_list = [refdes_list] #, method_list, type_list, parserDriver_list
# result = np.asarray(data_list).T.tolist()


df = pd.DataFrame(refdes_list,columns=['refdes_list'])
df['method_list'] = method_list
df['type_list'] = type_list
df['parserDriver_list'] = parserDriver_list

column_list = ['refdes_list', 'method_list', 'type_list', 'parserDriver_list']

outputfile = '/Users/leila/Documents/OOI_GitHub_repo/work/ingest-status/000_ingestpy_run_results/' + filename + '/data/' + filename + '_infrastructure.csv'
df.to_csv(outputfile, index=False, columns=column_list, na_rep='NaN', encoding='utf-8')