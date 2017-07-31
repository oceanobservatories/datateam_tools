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
# select the platform
platform = 'CP01CNSM'
# path to baseline file
rootdir = '/Users/leila/Documents/OOI_GitHub_repo/work/ingest-status/000_ingestpy_run_results/'
# select the ingestion file example _D00003_ingest.csv or leave it as generic _ingest.csv
key_file = '_P.csv'
# headers' name for ingestion files
ingestion_header= ['uframe_route', 'filename_mask', 'reference_designator', 'data_source','status','notes']

# directory = os.path.dirname(rootdir+platform+'/')
# if not os.path.exists(directory):
#         os.makedirs(directory)

# df = pd.DataFrame()
for item in os.listdir(rootdir):
    if item.startswith(platform):
        if item.endswith(key_file):
            if os.path.isfile(os.path.join(rootdir, item)):
                print item
                with open(os.path.join(rootdir, item), 'r') as csv_file:
                    filereader = pd.read_csv(csv_file)
                    filereader = filereader.rename(columns={'status': 'notes'})
                    filereader = filereader.rename(columns={'Automated_status': 'status'})
                    filename_list = list(pd.unique(filereader['ingest_csv_filename'].ravel()))
                    print filename_list
                    for filex in filename_list:
                        print filex
                        ind_r = filereader.loc[(filereader['ingest_csv_filename'] == filex)]

                        filename = ind_r['ingest_csv_filename']
                        print filename.values[0]

                        outputfile = rootdir + platform + '/ingest/' + filename.values[0]
                        print outputfile

                        ind_r.to_csv(outputfile, index=False, columns=ingestion_header, na_rep='', encoding='utf-8')
