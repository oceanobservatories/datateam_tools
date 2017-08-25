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
This script creates three files listing the site platforms with the percent available, pending or missing ingestion path
'''
site = 'Papa' # ' Irminger SouthernOcean 'Papa' '' #''SouthernOcean
maindir = '/Users/leila/Documents/OOI_GitHub_repo/work/ingest-status/' + site + '/'
asset_dir = '/Users/leila/Documents/OOI_GitHub_repo/repos/ooi-integration/asset-management/deployment/'

out_a = maindir + site + '_platform_available.csv'
out_m = maindir + site + '_platform_missing.csv'
out_p = maindir + site + '_platform_pending.csv'

if site is not 'Pioneer':
    key_file = '_LG.csv'
else:
    key_file = '_P.csv'

deploy_columns = [str(x) for x in range(1,11)]
deploy_columns.extend(['deploy','status','platform'])

dfaa = pd.DataFrame()
dfmm = pd.DataFrame()
dfpp = pd.DataFrame()

count_ini = 0
for platform in os.listdir(maindir):
    if os.path.isdir(os.path.join(maindir, platform)):
        if site is not 'Pioneer':
            filedir = maindir + platform
        else:
            filedir = maindir + platform + '/data/'
        if not platform.startswith('CP05MOAS-A'):
            print(platform)
            count_ini += 1

            base_file = filedir + '/' + platform + '_infrastructure.csv'
            read_base = pd.read_csv(base_file)
            method_list = list(pd.unique(read_base['method_list'].ravel()))
            method_index = [x for x in method_list]

            if count_ini == 1:
                dfaa = pd.DataFrame(columns=deploy_columns, index=method_index)
                dfpp = pd.DataFrame(columns=deploy_columns, index=method_index)
                dfmm = pd.DataFrame(columns=deploy_columns, index=method_index)

            deploy_file = asset_dir + platform + '_Deploy.csv'
            read_deploy = pd.read_csv(deploy_file)
            read_deploy.fillna('', inplace=True)
            deploy_list = list(pd.unique(read_deploy['deploymentNumber'].ravel()))
            deploy_last = deploy_list[len(deploy_list) - 1]
            #deploy_columns = [str(x) for x in range(1, len(deploy_list) + 1)]
            #deploy_columns.extend(['deploy', 'status', 'platform'])

            for item in os.listdir(os.path.join(filedir)):
                if item.endswith(key_file):
                    with open(os.path.join(filedir, item), 'r') as csv_file:
                        print(item)
                        filereader = pd.read_csv(csv_file)
                        filereader.fillna('', inplace=True)
                        status_list = list(pd.unique(filereader['Automated_status'].ravel()))
                        #deploy_list = list(pd.unique(filereader['deployment#'].ravel()))
                        # method_list = list(pd.unique(filereader['data_source'].ravel()))
                        # deploy_last =  deploy_list[len(deploy_list) - 1]
                        # deploy_columns = [str(x) for x in range(1, len(deploy_list) + 1)]
                        # deploy_columns.extend(['deploy', 'status', 'platform', 'notes'])

                        for methodx in method_list:
                            #print methodx

                            ind0 = filereader.loc[(filereader['data_source'] == methodx)]

                            dfa = pd.DataFrame(columns=deploy_columns, index=[methodx])
                            dfm = pd.DataFrame(columns=deploy_columns, index=[methodx])
                            dfp = pd.DataFrame(columns=deploy_columns, index=[methodx])



                            for deployx in deploy_list:
                                #print deployx
                                ind1 = ind0.loc[(ind0['deployment#'] == deployx)]

                                total = len(ind1)

                                if total != 0:
                                    start = ind1['startDateTime'].values[0]
                                    end = ind1['stopDateTime'].values[0]
                                    status_nd = ind1['Automated_status'].values[0]
                                    if deployx == deploy_last:
                                        try:
                                            if end is '':
                                                note = 'LIVE'  # active deployment
                                            else:
                                                note = 'LAST'
                                        except IndexError:
                                            note = 'Error'
                                    else:
                                        note = 'PAST'
                                else:
                                    if deployx == deploy_last:
                                        indy = read_deploy.loc[(read_deploy['deploymentNumber'] == deploy_last)]
                                        start = indy['startDateTime'].values[0]
                                        end = indy['stopDateTime'].values[0]
                                        indz =  filereader.loc[(filereader['deployment#'] == deploy_last)]
                                        try:
                                            status_nd = indz['Automated_status'].values[0]
                                        except IndexError:
                                            status_nd = 'Not_Evaluated'
                                        try:
                                            if end is '':
                                                note = 'LIVE' # active deployment
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


                                if status_nd == 'Not Deployed':
                                    if deployx == deploy_last:
                                        deploy_status = str(deploy_last - 1) + '-' + 'LAST'
                                    else:
                                        deploy_status = str(deploy_last) + '-' + note
                                else:
                                    deploy_status = str(deploy_last) + '-' + note

                                counte = 0
                                counta = 0
                                countm = 0
                                countp = 0
                                countnd = 0
                                countne = 0

                                for statusx in status_list:

                                    ind2 = ind1.loc[(ind1['Automated_status'] == statusx)]

                                    if statusx == 'Not Deployed':
                                        countnd = len(ind2)

                                    if statusx == 'Not Expected':
                                        countne = len(ind2)

                                    if statusx == 'Expected':
                                        counte = len(ind2)

                                    if statusx == 'Available':
                                        counta = len(ind2)

                                    if statusx == 'Missing':
                                        countm = len(ind2)

                                    if statusx == 'Pending':
                                        countp = len(ind2)

                                total_a = total-(countnd+countne+counte)
                                if total_a != 0:
                                    percenta = round((counta/total_a) * 100)
                                    percentm = round((countm/total_a) * 100)
                                    percentp = round((countp/total_a) * 100)
                                else:
                                    if total != 0:
                                        percenta = 100
                                    else:
                                        percenta = total_a
                                    percentm = total_a
                                    percentp = total_a

                                if deployx == deploy_last:
                                    if end is '':
                                        if methodx.find('recovered') != -1:
                                            percenta = -100

                                # if start is not '':
                                #     start = datetime.datetime.strptime(start, "%Y-%m-%dT%H:%M:%S")
                                #     start_x = str(start.year) + '-' + str(start.month)
                                # else:
                                #     start_x = ''

                                # if end is not '':
                                #     end = datetime.datetime.strptime(end, "%Y-%m-%dT%H:%M:%S")
                                #     end_x = str(end.year) + '-' + str(end.month)
                                # else:
                                #     end_x = ''


                                #df[start] = pd.Series([percenta], index=[methodx])
                                #df[end] = pd.Series([percenta], index=[methodx])
                                dfa['platform'] = pd.Series([platform], index=[methodx])
                                dfa['status'] = pd.Series(['Available'], index=[methodx])
                                dfa['deploy'] = pd.Series([deploy_status],index=[methodx])
                                dfa[str(deployx)] = pd.Series([percenta], index=[methodx])

                                dfm['platform'] = pd.Series([platform], index=[methodx])
                                dfm['status'] = pd.Series(['Missing'], index=[methodx])
                                dfm['deploy'] = pd.Series([deploy_status], index=[methodx])
                                dfm[str(deployx)] = pd.Series([percentm], index=[methodx])

                                dfp['platform'] = pd.Series([platform], index=[methodx])
                                dfp['status'] = pd.Series(['Pending'], index=[methodx])
                                dfp['deploy'] = pd.Series([deploy_status], index=[methodx])
                                dfp[str(deployx)] = pd.Series([percentp], index=[methodx])

                            dfaa = dfaa.append(dfa)
                            dfmm = dfmm.append(dfm)
                            dfpp = dfpp.append(dfp)


                dfaa = dfaa.replace(0, np.NaN)
                rows_aa, columns_aa = dfaa.shape
                dfaa = dfaa.dropna(subset=dfaa.columns[0:10], how='all')

                dfmm = dfmm.replace(0, np.NaN)
                rows_mm, columns_mm = dfmm.shape
                dfmm = dfmm.dropna(subset=dfmm.columns[0:10], how='all')

                dfpp = dfpp.replace(0, np.NaN)
                rows_pp, columns_pp = dfpp.shape
                dfpp = dfpp.dropna(subset=dfpp.columns[0:10], how='all')  #len(deploy_list)

                dfaa.to_csv(out_a, index=True, na_rep='', encoding='utf-8')
                dfmm.to_csv(out_m, index=True, na_rep='', encoding='utf-8')
                dfpp.to_csv(out_p, index=True, na_rep='', encoding='utf-8')