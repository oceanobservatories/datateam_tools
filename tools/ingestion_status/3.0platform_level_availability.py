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
col_header= ['data_source', 'deployment#', 'Available', 'percenta','Missing','percentm','note']

df = pd.DataFrame(columns=col_header)

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

                    for methodx in method_list:
                        #print methodx
                        ind0 = filereader.loc[(filereader['data_source'] == methodx)]
                        for deployx in deploy_list:
                            #print deployx
                            ind1 = ind0.loc[(ind0['deployment#'] == deployx)]
                            total = len(ind1)
                            counta = 0
                            countm = 0
                            for statusx in status_list:
                                #print statusx

                                ind2 = ind1.loc[(ind1['Automated_status'] == statusx)]
                                count = len(ind2)

                                if statusx == 'Not Deployed':
                                    # print 'Not Deployed found ', len(ind2)
                                    counta += len(ind2)
                                if statusx == 'Not Expected':
                                    # print 'Not Expected found ', len(ind2)
                                    counta += len(ind2)
                                if statusx == 'Pending':
                                    # print 'Pending found ', len(ind2)
                                    counta += len(ind2)
                                if statusx == 'Available':
                                    # print 'Available found ', len(ind2)
                                    counta += len(ind2)

                                if statusx == 'Missing':
                                    # print 'Missing found ', len(ind2)
                                    countm = len(ind2)

                                if total != 0:
                                    percent = round((count/total) * 100)
                                    note = ''
                                else:
                                    note = 'Active deployment'
                                    percent = total
                                # print methodx, '...', deployx, '...', statusx, '...', count, '...', percent, '%', '....', note


                            if total != 0:
                                percenta = round((counta/total) * 100)
                                percentm = round((countm/total) * 100)
                                note = ''
                            else:
                                note = 'Active deployment'
                                percenta = total
                                percentm = total

                            # print methodx, '...', deployx, 'Available', counta, '...', percenta, '%', '....', note
                            # print methodx, '...', deployx, 'Missing', countm, '...', percentm, '%', '....', note
                            df1 = pd.DataFrame([[methodx, deployx, counta, percenta, countm, percentm, note]], columns=col_header)
                            print 'df1', df1.values
                            df = df.append(df1)
                            print 'df', df.values
                            # print df


                outputfile = maindir + platform + '/statistics/platform/'+ platform + '_availability_state.csv'
                # print outputfile
                df.to_csv(outputfile, index=False, columns=col_header, na_rep='', encoding='utf-8')