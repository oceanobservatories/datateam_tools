#! /usr/local/bin/python

"""
Created on Mon July 25 2017
@author: leilabbb
"""
from __future__ import division

import pandas as pd
import os
import time


start_time = time.time()

'''
This script recreate the ingestion and deployment sheets
'''
# select the platform
platform = 'CP03ISSM'
# path to baseline file
maindir = '/Users/leila/Documents/OOI_GitHub_repo/work/ingest-status/000_ingestpy_run_results/'
rootdir = maindir + platform + '/data/'
# select the ingestion file example _D00003_ingest.csv or leave it as generic _ingest.csv
key_file = '_P.csv'
# headers' name for ingestion files
col_header= ['reference_designator', 'data_source', 'deployment#', 'Available', 'percenta','Missing','percentm','note']
col_header_info = ['reference_designator', 'data_source', 'deployment#','Automated_status','status']

for item in os.listdir(rootdir):
    if item.startswith(platform):
        if item.endswith(key_file):
            if os.path.isfile(os.path.join(rootdir, item)):
                print item
                with open(os.path.join(rootdir, item), 'r') as csv_file:
                    filereader = pd.read_csv(csv_file)
                    status_list = list(pd.unique(filereader['Automated_status'].ravel()))
                    deploy_list = list(pd.unique(filereader['deployment#'].ravel()))
                    method_list = list(pd.unique(filereader['data_source'].ravel()))
                    refdes_list = list(pd.unique(filereader['reference_designator'].ravel()))

                    for refdesx in refdes_list:
                        df = pd.DataFrame(columns=col_header)
                        df_info = pd.DataFrame(columns=col_header)
                        ind0 = filereader.loc[(filereader['reference_designator'] == refdesx)]
                        df_info = df_info.append(ind0)
                        outfile1 = maindir + platform + '/statistics/instrument/' + refdesx + '_availability_info.csv'
                        df_info.to_csv(outfile1, index=False, columns=col_header_info, na_rep='', encoding='utf-8')
                        for methodx in method_list:
                            #print methodx
                            ind1 = ind0.loc[(ind0['data_source'] == methodx)]

                            for deployx in deploy_list:
                                #print deployx
                                ind2 = ind1.loc[(ind1['deployment#'] == deployx)]

                                total = len(ind2)

                                counta = 0
                                countm = 0

                                for statusx in status_list:
                                    #print statusx

                                    ind3 = ind2.loc[(ind2['Automated_status'] == statusx)]
                                    count = len(ind3)

                                    if statusx == 'Not Deployed':
                                        # print 'Not Deployed found ', len(ind2)
                                        counta += len(ind3)
                                    if statusx == 'Not Expected':
                                        # print 'Not Expected found ', len(ind2)
                                        counta += len(ind3)
                                    if statusx == 'Pending':
                                        # print 'Pending found ', len(ind2)
                                        counta += len(ind3)
                                    if statusx == 'Available':
                                        # print 'Available found ', len(ind2)
                                        counta += len(ind3)

                                    if statusx == 'Missing':
                                        # print 'Missing found ', len(ind2)
                                        countm = len(ind3)

                                    if total != 0:
                                        percent = round((count/total) * 100)
                                        note = ''
                                    else:
                                        if deployx == deploy_list[len(deploy_list)-1]:
                                            note = 'Active deployment'
                                            percent = total
                                        else:
                                            note = 'why?'
                                            percent = total


                                if total != 0:
                                    percenta = round((counta/total) * 100)
                                    percentm = round((countm/total) * 100)
                                    note = ''
                                else:
                                    if deployx == deploy_list[len(deploy_list) - 1]:
                                        note = 'Active deployment'
                                        percenta = total
                                        percentm = total
                                    else:
                                        note = 'why?'
                                        percenta = total
                                        percentm = total



                                # print methodx, '...', deployx, 'Available', counta, '...', percenta, '%', '....', note
                                # print methodx, '...', deployx, 'Missing', countm, '...', percentm, '%', '....', note
                                df1 = pd.DataFrame([[refdesx, methodx, deployx, counta, percenta, countm, percentm, note]], columns=col_header)
                                df = df.append(df1)
                                print 'df', df.values


                        outfile0 = maindir + platform + '/statistics/instrument/' + refdesx + '_availability_state.csv'
                        # print outputfileind3['status'].values
                        df.to_csv(outfile0, index=False, columns=col_header, na_rep='', encoding='utf-8')

