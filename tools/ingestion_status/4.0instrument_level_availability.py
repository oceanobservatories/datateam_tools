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
platform = 'CP01CNSM'
# path to baseline file
maindir = '/Users/leila/Documents/OOI_GitHub_repo/work/ingest-status/000_ingestpy_run_results/'
rootdir = maindir + platform + '/data/'
# select the ingestion file example _D00003_ingest.csv or leave it as generic _ingest.csv
key_file = '_P.csv'
infrastructure = rootdir + platform + '_infrastructure.csv'
# headers' name for ingestion files
#col_header= ['reference_designator', 'data_source', 'deployment#', 'Available', 'percenta','Missing','percentm','Active Deployment','Start','End']
col_header_info = ['reference_designator', 'data_source', 'deployment#','Automated_status','status']

df = pd.DataFrame()
df1 = pd.DataFrame()

with open(os.path.join(infrastructure), 'r') as base_file:
    filebase = pd.read_csv(base_file)
    refdes_list = list(pd.unique(filebase['refdes_list'].ravel()))

for item in os.listdir(rootdir):
    if item.startswith(platform):
        if item.endswith(key_file):
            if os.path.isfile(os.path.join(rootdir, item)):
                with open(os.path.join(rootdir, item), 'r') as csv_file:
                    filereader = pd.read_csv(csv_file)
                    filereader.fillna('', inplace=True)
                    status_list = list(pd.unique(filereader['Automated_status'].ravel()))
                    deploy_list = list(pd.unique(filereader['deployment#'].ravel()))

                    for refdesx in refdes_list:
                        print refdesx
                        indx = filebase.loc[(filebase['refdes_list'] == refdesx)]
                        method_list = list(pd.unique(indx['method_list'].ravel()))
                        #df = pd.DataFrame(columns=col_header)
                        df_info = pd.DataFrame(columns=col_header_info)
                        ind0 = filereader.loc[(filereader['reference_designator'] == refdesx)]
                        df_info = df_info.append(ind0)
                        outfile1 = maindir + platform + '/statistics/instrument/' + refdesx + '_availability_info.csv'
                        df_info.to_csv(outfile1, index=False, columns=col_header_info, na_rep='', encoding='utf-8')

                        for methodx in method_list:
                            dfa = pd.DataFrame(index=[methodx])
                            dfp = pd.DataFrame(index=[methodx])
                            dfm = pd.DataFrame(index=[methodx])
                            ind1 = ind0.loc[(ind0['data_source'] == methodx)]

                            for deployx in deploy_list:
                                ind2 = ind1.loc[(ind1['deployment#'] == deployx)]
                                total = len(ind2)

                                if total != 0:
                                    start = ind2['startDateTime'].values[0]
                                    end = ind2['stopDateTime'].values[0]

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

                                    ind3 = ind2.loc[(ind2['Automated_status'] == statusx)]
                                    count = len(ind3)

                                    if statusx == 'Not Deployed':
                                        # print 'Not Deployed found ', len(ind2)
                                        counta += len(ind3)
                                    if statusx == 'Not Expected':
                                        # print 'Not Expected found ', len(ind2)
                                        counta += len(ind3)

                                    if statusx == 'Available':
                                        # print 'Available found ', len(ind2)
                                        counta += len(ind3)

                                    if statusx == 'Missing':
                                        # print 'Missing found ', len(ind2)
                                        countm = len(ind3)

                                    if statusx == 'Pending':
                                        # print 'Pending found ', len(ind2)
                                        countp = len(ind3)

                                    if total != 0:
                                        percent = round((count/total) * 100)
                                    else:
                                         percent = total

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
                                #df1 = pd.DataFrame([[refdesx, methodx, deployx, counta, percenta, countm, percentm, note, start, end]], columns=col_header)
                                #df = df.append(df1)
                                #print 'df', df.values
                                print deployx, start, end
                                if start is not '':
                                    start = datetime.datetime.strptime(start, "%Y-%m-%dT%H:%M:%S")
                                    start_x = 'D0' + str(deployx) + '_' + str(start.year) + '-' + str(start.month)
                                else:
                                    start_x = 'D0' + str(deployx) + '_'
                                if end is not '':
                                    end = datetime.datetime.strptime(end, "%Y-%m-%dT%H:%M:%S")
                                    end_x = 'D0' + str(deployx) + '_' + str(end.year) + '-' + str(end.month)
                                else:
                                    end_x = 'D0' + str(deployx) + '_'

                                dfa['status'] = pd.Series(['Available'], index=[methodx])
                                dfa[start_x] = pd.Series([percenta], index=[methodx])
                                dfa[end_x] = pd.Series([percenta], index=[methodx])

                                dfp['status'] = pd.Series(['Pending'], index=[methodx])
                                dfp[start_x] = pd.Series([percentp], index=[methodx])
                                dfp[end_x] = pd.Series([percentp], index=[methodx])

                                dfm['status'] = pd.Series(['Missing'], index=[methodx])
                                dfm[start_x] = pd.Series([percentm], index=[methodx])
                                dfm[end_x] = pd.Series([percentm], index=[methodx])

                            df1 = df1.append(dfa)
                            df1 = df1.append(dfp)
                            df1 = df1.append(dfm)

                        #print df1.values

                        outfile0 = maindir + platform + '/statistics/instrument/' + refdesx + '_availability_state.csv'
                        # print outputfileind3['status'].values
                        df1.to_csv(outfile0, index=True, na_rep='', encoding='utf-8')

