#! /usr/local/bin/python

"""
Created on Mon July 25 2017
@author: leilabbb
"""
from __future__ import division
import datetime
import pandas as pd
import numpy as np
import os
import time


start_time = time.time()

'''
This script recreate the ingestion and deployment sheets
'''
site = 'Endurance'
# path to working files
maindir = '/Users/leila/Documents/OOI_GitHub_repo/work/ingest-status/' + site + '/'
asset_dir = '/Users/leila/Documents/OOI_GitHub_repo/repos/ooi-integration/asset-management/deployment/'
# path to output files
out_a = maindir + site + '_instrument_available.csv'
out_m = maindir + site + '_instrument_missing.csv'
out_p = maindir + site + '_instrument_pending.csv'

# select the ingestion file example _D00003_ingest.csv or leave it as generic _ingest.csv
if site is not 'Endurance':  #Pioneer
    key_file = '_LG.csv'
else:
    key_file = '_E.csv'  # _P


col_header_info = ['reference_designator', 'data_source', 'deployment#','Automated_status','status']

deploy_columns = [str(x) for x in range(1,11)]
deploy_columns.extend(['deploy','status','Instrument','notes'])

dfaa = pd.DataFrame()
dfmm = pd.DataFrame()
dfpp = pd.DataFrame()

count_ini = 0
for platform in os.listdir(maindir):
    if os.path.isdir(os.path.join(maindir, platform)):
        if not platform.startswith('CP05MOAS-A'):
            count_ini += 1
            if site is not 'Pioneer':
                filedir = maindir + platform + '/'
            else:
                filedir = maindir + platform + '/data/'

            infrastructure = filedir + platform + '_infrastructure.csv'
            with open(os.path.join(infrastructure), 'r') as base_file:
                filebase = pd.read_csv(base_file)
                refdes_list = list(pd.unique(filebase['refdes_list'].ravel()))

            deploy_file = asset_dir + platform + '_Deploy.csv'
            read_deploy = pd.read_csv(deploy_file)
            read_deploy.fillna('', inplace=True)
            deploy_list = list(pd.unique(read_deploy['deploymentNumber'].ravel()))
            deploy_last = deploy_list[len(deploy_list) - 1]
            # deploy_columns = [str(x) for x in range(1, len(deploy_list) + 1)]
            # deploy_columns.extend(['deploy', 'status', 'Instrument', 'notes'])
            print(platform)
            for item in os.listdir(os.path.join(filedir)):
                if item.endswith(key_file):
                    with open(os.path.join(filedir, item), 'r') as csv_file:
                        filereader = pd.read_csv(csv_file)
                        filereader.fillna('', inplace=True)
                        status_list = list(pd.unique(filereader['Automated_status'].ravel()))
                        # deploy_list = list(pd.unique(filereader['deployment#'].ravel()))
                        # deploy_last = deploy_list[len(deploy_list) - 1]
                        # deploy_columns = [str(x) for x in range(1,len(deploy_list)+1)]
                        # deploy_columns.extend(['deploy','status','Instrument','notes'])

                        for refdesx in refdes_list:
                            print(refdesx)
                            indx = filebase.loc[(filebase['refdes_list'] == refdesx)]
                            method_list = list(pd.unique(indx['method_list'].ravel()))
                            method_index = [x for x in method_list]

                            if count_ini == 1:
                                dfaa = pd.DataFrame(columns=deploy_columns, index=method_index)
                                dfpp = pd.DataFrame(columns=deploy_columns, index=method_index)
                                dfmm = pd.DataFrame(columns=deploy_columns, index=method_index)

                            df_info = pd.DataFrame(columns=col_header_info)
                            ind0 = filereader.loc[(filereader['reference_designator'] == refdesx)]
                            df_info = df_info.append(ind0)
                            outfile1 = maindir + platform + '/statistics/instrument/' + refdesx + '_availability_info.csv'
                            df_info.to_csv(outfile1, index=False, columns=col_header_info, na_rep='', encoding='utf-8')

                            for methodx in method_list:

                                dfa = pd.DataFrame(columns=deploy_columns, index=[methodx])
                                dfp = pd.DataFrame(columns=deploy_columns, index=[methodx])
                                dfm = pd.DataFrame(columns=deploy_columns, index=[methodx])

                                ind1 = ind0.loc[(ind0['data_source'] == methodx)]

                                annotation_p = []
                                annotation_m = []
                                for deployx in deploy_list:
                                    ind2 = ind1.loc[(ind1['deployment#'] == deployx)]
                                    total = len(ind2)
                                    if total != 0:
                                        start = ind1['startDateTime'].values[0]
                                        end = ind1['stopDateTime'].values[0]
                                        if deployx == deploy_last:
                                            status_nd = ind1['Automated_status'].values[0]
                                            try:
                                                if end is '':
                                                    note = 'LIVE'  # active deployment
                                                else:
                                                    note = 'LAST'
                                            except IndexError:
                                                note = ''
                                        else:
                                            note = ''
                                            status_nd = ''
                                    else:
                                        if deployx == deploy_last:
                                            indy = read_deploy.loc[(read_deploy['deploymentNumber'] == deploy_last)]
                                            start = indy['startDateTime'].values[0]
                                            end = indy['stopDateTime'].values[0]
                                            indz = filereader.loc[(filereader['deployment#'] == deploy_last)]
                                            try:
                                                status_nd = indz['Automated_status'].values[0]
                                            except IndexError:
                                                status_nd = 'Not_Evaluated'
                                            try:
                                                if end is '':
                                                    note = 'LIVE'  # active deployment
                                                else:
                                                    note = 'LAST'
                                            except IndexError:
                                                note = 'Error'
                                        else:
                                            note = 'Missing from status file'
                                            status_nd = ''
                                            indy = read_deploy.loc[(read_deploy['deploymentNumber'] == deployx)]
                                            start = indy['startDateTime'].values[0]
                                            end = indy['stopDateTime'].values[0]
                                            try:
                                                if end is '':
                                                    note = 'PAST'  # active deployment
                                                else:
                                                    note = 'LAST'
                                            except IndexError:
                                                note = 'Error'

                                    counte = 0
                                    counta = 0
                                    countm = 0
                                    countp = 0
                                    countnd = 0
                                    countne = 0

                                    for statusx in status_list:
                                        #print statusx

                                        ind3 = ind2.loc[(ind2['Automated_status'] == statusx)]
                                        count = len(ind3)

                                        if statusx == 'Not Deployed':
                                            countnd = len(ind3)

                                        if statusx == 'Not Expected':
                                            countne = len(ind3)

                                        if statusx == 'Expected':
                                            counte = len(ind3)

                                        if statusx == 'Available':
                                            counta = len(ind3)

                                        if statusx == 'Missing':
                                            countm = len(ind3)
                                            try:
                                                textm = ind3['status'].values[0]
                                            except IndexError:
                                                textm = 'none'
                                            annotation_m.append(textm)

                                        if statusx == 'Pending':
                                            countp = len(ind3)
                                            try:
                                                textp = ind3['status'].values[0]
                                            except IndexError:
                                                textp = ''
                                            annotation_p.append(textp)

                                    total_a = total - (countnd + countne + counte)

                                    if total_a != 0:
                                        percenta = round((counta / total_a) * 100)
                                        percentm = round((countm / total_a) * 100)
                                        percentp = round((countp / total_a) * 100)
                                    else:
                                        percenta = total_a
                                        percentm = total_a
                                        percentp = total_a

                                    if deployx == deploy_last:
                                        if end is '':
                                            if methodx.find('recovered') != -1:
                                                percenta = -100

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

                                    if status_nd == 'Not Deployed':
                                        if deployx == deploy_last:
                                            deploy_text = str(deploy_last - 1) + '-' + 'LAST'
                                        else:
                                            deploy_text = str(deploy_last) + '-' + note
                                    else:
                                        deploy_text = str(deploy_last) + '-' + note


                                    dfa['Instrument'] = pd.Series([refdesx], index=[methodx])
                                    dfa['status'] = pd.Series(['Available'], index=[methodx])
                                    dfa[str(deployx)] = pd.Series([percenta], index=[methodx])
                                    dfa['deploy'] = pd.Series([deploy_text], index=[methodx])
                                    # dfa[start_x] = pd.Series([percenta], index=[methodx])
                                    # dfa[end_x] = pd.Series([percenta], index=[methodx])

                                    dfp['Instrument'] = pd.Series([refdesx], index=[methodx])
                                    dfp['status'] = pd.Series(['Pending'], index=[methodx])
                                    dfp['notes'] = pd.Series([annotation_p], index=[methodx])
                                    dfp['deploy'] = pd.Series([deploy_text], index=[methodx])
                                    dfp[str(deployx)] = pd.Series([percentp], index=[methodx])
                                    # dfp[start_x] = pd.Series([percentp], index=[methodx])
                                    # dfp[end_x] = pd.Series([percentp], index=[methodx])


                                    dfm['Instrument'] = pd.Series([refdesx], index=[methodx])
                                    dfm['status'] = pd.Series(['Missing'], index=[methodx])
                                    dfm['notes'] = pd.Series([annotation_m], index=[methodx])
                                    dfm['deploy'] = pd.Series([deploy_text], index=[methodx])
                                    dfm[str(deployx)] = pd.Series([percentm], index=[methodx])
                                    # dfm[start_x] = pd.Series([percentm], index=[methodx])
                                    # dfm[end_x] = pd.Series([percentm], index=[methodx])

                                dfaa = dfaa.append(dfa)
                                dfmm = dfmm.append(dfm)
                                dfpp = dfpp.append(dfp)

                            #print df1.values
                dfaa = dfaa.replace(0, np.NaN)
                rows_aa, columns_aa = dfaa.shape
                print(dfaa.columns[0:10])
                dfaa = dfaa.dropna(subset=dfaa.columns[0:10], how='all')

                dfmm = dfmm.replace(0, np.NaN)
                rows_mm, columns_mm = dfmm.shape
                print(dfmm.columns[0:10])
                dfmm = dfmm.dropna(subset=dfmm.columns[0:10], how='all')
                dfmm.dropna(axis=1, how='all')

                dfpp = dfpp.replace(0, np.NaN)
                rows_pp, columns_pp = dfpp.shape
                print(dfpp.columns[0:10])
                dfpp = dfpp.dropna(subset=dfpp.columns[0:10], how='all')

                # print outputfileind3['status'].values
                dfaa.to_csv(out_a, index=True, na_rep='', encoding='utf-8')
                dfmm.to_csv(out_m, index=True, na_rep='', encoding='utf-8')
                dfpp.to_csv(out_p, index=True, na_rep='', encoding='utf-8')