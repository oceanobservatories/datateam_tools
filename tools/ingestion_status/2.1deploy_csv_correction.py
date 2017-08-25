#! /usr/local/bin/python

"""
Created on Mon July 25 2017
@author: leilabbb
"""

import pandas as pd
import glob
import os
import time

start_time = time.time()

'''
This script recreate the ingestion and deployment sheets
'''
# select a site
site = 'SouthernOcean'
# select the platform
platform = 'GS05MOAS-PG566'

# path to baseline file
maindir = '/Users/leila/Documents/OOI_GitHub_repo/work/ingest-status/' + site + '/'
rootdir  = maindir + platform + '/'
# select the ingestion file example _D00003_ingest.csv or leave it as generic _ingest.csv
key_file = '_LG.csv'
# headers' name for ingestion files
ingestion_header= ['CUID_Deploy', 'deployedBy', 'CUID_Recover', 'recoveredBy', 'Reference Designator', 'deploymentNumber',
                   'versionNumber', 'startDateTime', 'stopDateTime', 'mooring.uid', 'node.uid',
                   'sensor.uid', 'lat', 'lon', 'orbit', 'deployment_depth', 'water_depth','notes']

# directory = os.path.dirname(rootdir+platform+'/')
# if not os.path.exists(directory):
#         os.makedirs(directory)

df = pd.DataFrame()
for item in os.listdir(rootdir):
    if item.startswith(platform):
        if item.endswith(key_file):
            if os.path.isfile(os.path.join(rootdir, item)):
                print item
                with open(os.path.join(rootdir, item), 'r') as csv_file:
                    filereader = pd.read_csv(csv_file)
                    filereader = filereader.rename(columns={'deployment#': 'deploymentNumber'})
                    filereader = filereader.rename(columns={'reference_designator': 'Reference Designator'})
                    filereader = filereader.rename(columns={'notes_y': 'notes'})
                    deployment_list = list(pd.unique(filereader['deploymentNumber'].ravel()))
                    print deployment_list
                    for deploymentx in deployment_list:
                        print deploymentx
                        ind_r = filereader.loc[(filereader['deploymentNumber'] == deploymentx)]
                        print 'before', len(ind_r)
                        ind_r = ind_r.drop_duplicates(subset='Reference Designator',keep='first')
                        print 'after', len(ind_r)

                        df = df.append(ind_r)


                    outputfile = rootdir + '/deploy/' + platform + '_Deploy.csv'
                    print outputfile

                    df.to_csv(outputfile, index=False, columns=ingestion_header, na_rep='', encoding='utf-8')