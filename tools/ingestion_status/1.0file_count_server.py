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
This script combines all/or selected ingestion sheets found in GitHub repo:
https://github.com/ooi-integration/ingestion-csvs
# used for: (1) output file created on
#           (2) check raw data files created today
'''


# path to local copy of ingestion-csvs repo
rootdir = '/Users/leila/Documents/OOI_GitHub_repo/repos/ooi-integration/ingestion-csvs/'
# select the ingestion file example _D00003_ingest.csv or leave it as generic _ingest.csv
ingestion_file = '_ingest.csv'
# path to data file on the raw data repo
dav_mount = '/Volumes/dav/'
# path modification variables to match to system data file paths
splitter = '/OMC/'
splitter_C = '/rsn_data/DVT_Data/'
splitter_CC = '/RSN/'

# select a site
site_name = 'Endurance'
# select a platform
ingest_key = 'CE02SHSM'#'CE02SHBP' #CE01ISSP
# locate data file
main = '/Users/leila/Documents/OOI_GitHub_repo/work/ingest-status/' + site_name + '/'

# create output file
outputfile = main + ingest_key + '/'

# timestamp the output files
datein = pd.to_datetime('today')


# start script
df = pd.DataFrame()
for root, dirs, files in os.walk(rootdir):
    for item in dirs:
        if item.startswith(ingest_key):
            rootCP = root + item + '/'
            for rCP, dirCP, fileCP in os.walk(rootCP):
                for f in fileCP:
                    print f
                    if f.endswith(ingestion_file):
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

                            # add to data frame --> platform name || deployment number || results
                            filereader['platform'] = filereader['ingest_csv_filename'].str.split('_').str[0].str[0:8]
                            filereader['deployment#'] = filereader['ingest_csv_filename'].str.split('_').str[1].str[3:6]
                            filereader['number_files'] = ''
                            filereader['file <= 1k'] = ''
                            filereader['file > 1K'] = ''
                            filereader['file of today'] = ''
                            filereader['Automated_status'] = ''

                            # check file path on dav
                            file_num = []
                            size1kless = []
                            size1kplus = []
                            todayfile = []
                            statusfile = []

                            index_i = 0
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

                                        compare_2_date = datein.strftime("%d/%m/%Y")
                                        file_1K_size = 0
                                        file_1Kplus_size = 0
                                        file_of_today = 0
                                        kk = 0
                                        for file in file_match:
                                            if os.path.isfile(file):
                                                file_modified = pd.to_datetime(time.ctime(os.path.getmtime(file)))
                                                file_modified = file_modified.strftime("%d/%m/%Y")
                                                get_size = os.path.getsize(file)

                                                if get_size <= 1024:  # 513410:
                                                    file_1K_size += 1

                                                if get_size > 1024:
                                                    file_1Kplus_size += 1

                                                if file_modified == compare_2_date:
                                                    file_of_today += 1
                                            else:
                                                print file

                                            kk += 1

                                        size1kless.append(str(file_1K_size))
                                        size1kplus.append(str(file_1Kplus_size))
                                        todayfile.append(str(file_of_today))

                                        num_files = len(file_match)
                                        if num_files == 0:
                                            statusfile.append('Missing')
                                        else:
                                            statusfile.append('Available')

                                        num_files_text = str(num_files)
                                        file_num.append(num_files_text)

                                    except AttributeError:
                                        kk = 0
                                        file_1K_size = 0
                                        file_1Kplus_size = 0
                                        file_of_today = 0
                                        web_dir = row.filename_mask
                                        num_files_text = 'file_mask w attribute error'
                                        size1kless.append('file_mask w attribute error')
                                        size1kplus.append('file_mask w attribute error')
                                        todayfile.append('file_mask w attribute error')
                                        file_num.append('file_mask w attribute error')
                                        statusfile.append('')
                                        #print web_dir, '.....',  'file_mask w attribute error'
                                else:
                                    kk = 0
                                    file_1K_size = 0
                                    file_1Kplus_size = 0
                                    file_of_today = 0
                                    web_dir = row.filename_mask
                                    num_files_text = 'file_mask is empty'
                                    size1kless.append('file_mask is empty')
                                    size1kplus.append('file_mask is empty')
                                    todayfile.append('file_mask is empty')
                                    file_num.append('file_mask is empty')
                                    statusfile.append('')
                                    #print web_dir, '.....', 'file_mask is empty'



                                filereader['number_files'][row.Index] = file_num[index_i]
                                filereader['file <= 1k'][row.Index] = size1kless[index_i]
                                filereader['file > 1K'][row.Index] = size1kplus[index_i]
                                filereader['file of today'][row.Index] = todayfile[index_i]
                                filereader['Automated_status'][row.Index] = statusfile[index_i]



                                print row.Index, index_i, "--->", web_dir, ' : '
                                print '           number of files =', file_num[index_i], kk
                                print '           file <= 1k =', size1kless[index_i] #file_1K_size
                                print '           file > 1K =', size1kplus[index_i]#file_1Kplus_size
                                print '           file of today =', todayfile[index_i]#file_of_today

                                index_i += 1


                            # append all sheets in one file
                            df = df.append(filereader)
                            df.fillna('', inplace=True)


                mooring_header = ['ingest_csv_filename', 'platform', 'deployment#', 'uframe_route', 'filename_mask',
                                  'number_files', 'file of today','file <= 1k', 'file > 1K',
                                  'reference_designator', 'data_source','Automated_status','status', 'notes']
                created_on = datein.strftime("%d-%m-%Y")
                outputfile = main + item + '/' + item + '_' + created_on + '_rawfiles_query' + ingestion_file.split('_ingest.csv')[0] +'.csv'
                df.to_csv(outputfile, index=False, columns=mooring_header, na_rep='NaN', encoding='utf-8')

print "time elapsed: {:.2f}s".format(time.time() - start_time)