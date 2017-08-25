#! /usr/local/bin/python

"""
Created on Mon July 25 2017
@author: leilabbb
"""
from __future__ import division
import datetime
import pandas as pd
import os
import time


start_time = time.time()

'''
This script recreate the ingestion and deployment sheets
'''
# select the platform
#platform = 'CP04OSSM'
# path to baseline file
maindir = '/Users/leila/Documents/OOI_GitHub_repo/work/ingest-status/000_ingestpy_run_results/'
#rootdir = maindir + platform + '/data/'

# select the ingestion file example _D00003_ingest.csv or leave it as generic _ingest.csv
key_file = '_P.csv'
# headers' name for ingestion files
col_header = ['data_source', 'deployment#', 'Available', 'Missing', 'Pending', 'Active Deployment', 'Start', 'End']
#df = pd.DataFrame(columns=col_header)


for platform in os.listdir(maindir):
    if os.path.isdir(os.path.join(maindir, platform)):
        print platform
        if not platform.startswith('CP05MOAS-A'):
            for item in os.listdir(os.path.join(maindir, platform, 'data')):
                if item.endswith(key_file):
                    with open(os.path.join(maindir, platform, 'data', item), 'r') as csv_file:
                        print item
                        df1 = pd.DataFrame()
                        filereader = pd.read_csv(csv_file)
                        filereader.fillna('', inplace=True)
                        status_list = list(pd.unique(filereader['Automated_status'].ravel()))
                        deploy_list = list(pd.unique(filereader['deployment#'].ravel()))
                        method_list = list(pd.unique(filereader['data_source'].ravel()))

                        for methodx in method_list:
                            #print methodx
                            ind0 = filereader.loc[(filereader['data_source'] == methodx)]
                            dfa = pd.DataFrame(index=[methodx])
                            dfp = pd.DataFrame(index=[methodx])
                            dfm = pd.DataFrame(index=[methodx])

                            for deployx in deploy_list:
                                #print deployx
                                ind1 = ind0.loc[(ind0['deployment#'] == deployx)]
                                total = len(ind1)

                                if total != 0:
                                    start = ind1['startDateTime'].values[0]
                                    end = ind1['stopDateTime'].values[0]
                                    if deployx == deploy_list[len(deploy_list) - 1]:
                                        try:
                                            if end is '':
                                                note = 'TRUE'  # active deployment
                                        except IndexError:
                                            note = ''
                                    else:
                                        note = 'x'
                                else:
                                    if deployx == deploy_list[len(deploy_list) - 1]:
                                        note = 'TRUE'
                                        indy = filereader.loc[(filereader['deployment#'] == deploy_list[len(deploy_list) - 1])]
                                        start = indy['startDateTime'].values[0]
                                        end = indy['stopDateTime'].values[0]
                                    else:
                                        note = 'Missing'
                                        start = ''
                                        end = ''

                                counta = 0
                                countm = 0
                                countp = 0
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
                                    if statusx == 'Available':
                                        # print 'Available found ', len(ind2)
                                        counta += len(ind2)

                                    if statusx == 'Missing':
                                        # print 'Missing found ', len(ind2)
                                        countm = len(ind2)

                                    if statusx == 'Pending':
                                        # print 'Pending found ', len(ind2)
                                        countp = len(ind2)

                                    if total != 0:
                                        percent = round((count/total) * 100)
                                       # note = ''
                                    else:
                                       # note = 'Active deployment'
                                        percent = total
                                    # print methodx, '...', deployx, '...', statusx, '...', count, '...', percent, '%', '....', note


                                if total != 0:
                                    percenta = round((counta/total) * 100)
                                    percentm = round((countm/total) * 100)
                                    percentp = round((countp/total) * 100)
                                else:
                                    percenta = total
                                    percentm = total
                                    percentp = total

                                # print methodx, '...', deployx, 'Available', counta, '...', percenta, '%', '....', note
                                # print methodx, '...', deployx, 'Missing', countm, '...', percentm, '%', '....', note
                                if end is not '':
                                    start = datetime.datetime.strptime(start, "%Y-%m-%dT%H:%M:%S")
                                    start_x = 'D0'+ str(deployx) + '_' + str(start.year) + '-' + str(start.month)
                                else:
                                    start_x = 'D0' + str(deployx) + '_'

                                if end is not '':
                                    end = datetime.datetime.strptime(end, "%Y-%m-%dT%H:%M:%S")
                                    end_x = 'D0'+ str(deployx)+ '_' + str(end.year) + '-' + str(end.month)
                                else:
                                    end_x = 'D0'+ str(deployx)+ '_'

                                dfa['status'] = pd.Series(['Available'], index=[methodx])
                                #dfa[deployx] =  pd.Series([percenta], index=[methodx])
                                dfa[start_x] = pd.Series([percenta], index=[methodx])
                                dfa[end_x] = pd.Series([percenta], index=[methodx])

                                dfp['status'] = pd.Series(['Pending'], index=[methodx])
                                #dfp[deployx] = pd.Series([percentp], index=[methodx])
                                dfp[start_x] = pd.Series([percentp], index=[methodx])
                                dfp[end_x] = pd.Series([percentp], index=[methodx])

                                dfm['status'] = pd.Series(['Missing'], index=[methodx])
                                #dfm[deployx] = pd.Series([percentm], index=[methodx])
                                dfm[start_x] = pd.Series([percentm], index=[methodx])
                                dfm[end_x] = pd.Series([percentm], index=[methodx])


                                #df1 = pd.DataFrame([[methodx, deployx, percenta, percentm, percentp, note, start_x, end_x]],
                                #                  columns=col_header)

                            df1 = df1.append(dfa)
                            df1 = df1.append(dfp)
                            df1 = df1.append(dfm)

                    #print 'df1', df1.values

                        outputfile = maindir + platform + '/statistics/platform/'+ platform + '_availability_state.csv'
                    # print outputfile
                        df1.to_csv(outputfile, index=True, na_rep='', encoding='utf-8')
