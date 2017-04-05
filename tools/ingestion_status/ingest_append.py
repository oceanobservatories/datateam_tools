#! /usr/local/bin/python

"""
Created on Mon Feb 06 2017

@author: leilabbb
"""

import pandas as pd
import glob
import os
import time

start_time = time.time()

'''
This script combines all or selected ingestion sheets found in GitHub repo (https://github.com/ooi-integration/ingestion-csvs)

'''
# path to local copy of ingestion-csvs repo
rootdir = '/Users/leila/Documents/OOI_GitHub_repo/ingestion-csvs/'
# select the ingestion folder
ingest_key = 'CP05MOAS-A6263'

# path to data file on the raw data repo
dav_mount = '/Volumes/dav/'

# path modification variables to match to system data file paths
splitter = '/OMC/'
splitter_C = '/rsn_data/DVT_Data/'
splitter_CC = '/RSN/'



df = pd.DataFrame()
for root, dirs, files in os.walk(rootdir):
    for item in dirs:
        if item.startswith(ingest_key):
            rootCP = root + item + '/'
            for rCP, dirCP, fileCP in os.walk(rootCP):
                for f in fileCP:
                    print f
                    if f.endswith('_ingest.csv'):
                        with open(os.path.join(rCP,f),'r') as csv_file:
                            filereader = pd.read_csv(csv_file)
                            if 'Unnamed: 4' in filereader.columns:
                                filereader = filereader.rename(columns={'Unnamed: 4': 'status'})
                            # remove rows with empty Reference Designators
                            filereader.dropna(subset=['reference_designator'], inplace=True)

                            # remove rows with empty cells
                            filereader.dropna(how="all", inplace=True)

                            # replace NAN by empty string
                            filereader.fillna('', inplace=True)

                            # add the file name as a column
                            filereader['ingest_csv_filename'] = str(f)

                            # add just the platform name and the deployment number from the filename to data frame
                            filereader['platform'] = filereader['ingest_csv_filename'].str.split('_').str[0].str[0:8]
                            filereader['deployment#'] = filereader['ingest_csv_filename'].str.split('_').str[1].str[3:6]
                            filereader['number_files'] = ''

                            # check file path on dav
                            file_num = []
                            iloc = 0
                            for row in filereader.itertuples():

                                if row.filename_mask is not '':
                                    try:
                                        try:
                                            web_dir = os.path.join(dav_mount, row.filename_mask.split(splitter)[1])
                                        except IndexError:
                                            try:
                                                web_dir = os.path.join(dav_mount, row.platform, row.filename_mask.split(splitter_C)[1])
                                            except IndexError:
                                                web_dir = os.path.join(dav_mount, row.filename_mask.split(splitter_CC)[1])

                                        file_match = glob.glob(web_dir)
                                        file_num.append(str(len(file_match)))

                                        print web_dir, '.....', str(len(file_match))

                                    except AttributeError:
                                        web_dir = row.filename_mask
                                        file_num.append('file_mask w attribute error')
                                        print web_dir, '.....',  'file_mask w attribute error'
                                else:
                                    web_dir = row.filename_mask
                                    file_num.append('file_mask is empty')
                                    print web_dir, '.....', 'file_mask is empty'


                                filereader['number_files'][iloc] = file_num[iloc]
                                iloc += 1
                                print filereader['number_files'],

                            # append all deployment sheets in one file
                            df = df.append(filereader)
                            df.fillna('', inplace=True)


                mooring_header = ['ingest_csv_filename', 'platform', 'deployment#', 'uframe_route', 'filename_mask', 'number_files',
                                                  'reference_designator', 'data_source', 'status', 'source', 'note']
                outputfile = '/Users/leila/Documents/OOI_GitHub_repo/ingest-status/' + item + '_ingest_file.csv'
                df.to_csv(outputfile, index=False, columns=mooring_header, na_rep='NaN', encoding='utf-8')

print "time elapsed: {:.2f}s".format(time.time() - start_time)