#!/usr/bin/env python
"""
@file annotate_streams.py
@author Lori Garzio
@email lgarzio@marine.rutgers.edu
@brief
Read the .json output from analyze_nc_data.py, extract data availability and gaps, and write the results to stream-level annotation csvs (https://github.com/ooi-data-review/annotations).
Two other files are created:
(1) a file listing other streams used in the preparation of the main data stream
(2) a file reporting issues: (1) missing deployment end dates and (2) sequential datafiles time difference greater than a min.
@purpose Auto-populate the stream-level annotation csvs with data availability, data gaps, and issues
@usage
dataset Path to .json output from analyze_nc_data.py
annotations_dir Directory that contains the annotation csvs cloned from https://github.com/ooi-data-review/annotations,
in which to save the output
user User that completed the review
"""

import simplejson as json
import csv
import os
from datetime import datetime as dt
import re
import pandas as pd


def make_dir(save_dir):
    try:  # Check if the save_dir exists already... if not, make it
        os.mkdir(save_dir)
    except OSError:
        pass

# function to sort a list
def atoi(text):
    return int(text) if text.isdigit() else text

def natural_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    '''
    return [ atoi(c) for c in re.split('(\d+)', text) ]

# main function
def extract_gaps(data, stream_csv, stream_csv_other, stream_csv_issues, stream_name, user):
    # read in deployment information from asset management and start and end dates of the data requested by deployment
    deployment_list = data['deployments']
    deployment_list_sorted = deployment_list.keys()
    deployment_list_sorted.sort(key = natural_keys)  # sorts the deployments

    for d in deployment_list_sorted:
        deployment = d
        deploy_begin = data['deployments'][d]['start'] # asset management start date
        deploy_end = data['deployments'][d]['end'] # asset management end date
        data_begin = data['deployments'][d]['data_times']['start'] # first data file start date
        data_begin = pd.to_datetime(data_begin).strftime('%Y-%m-%dT%H:%M:%SZ')
#        data_end = data['deployments'][d]['data_times']['end']  # last data file end date

        # read in stream information
        for s in data['deployments'][d]['streams']:
            stream = s
            if stream == stream_name:  # if stream matches the stream from the file name, write to main .csv
                file = stream_csv
            else:
                file = stream_csv_other  # if the stream does not match the stream from the file name, write to the collocated instrument .csv

            # test if deployment begin from data equals deployment begin from asset management.
            if deploy_begin is data_begin:
                pass
            else:
                newline = (stream,deployment,deploy_begin,data_begin,'','NOT_AVAILABLE','','check: data begin does not equal deployment begin date',user)
                file.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)

            file_list = data['deployments'][d]['streams'][s]['files'] # list of files for the deployment
            file_list_sorted = file_list.keys()
            file_list_sorted.sort(key = natural_keys)  # sorts the data files

            cnf = 0  # to count files
            # read data file information
            for x in range(len(file_list_sorted)):
                data_start_file = data['deployments'][d]['streams'][s]['files'][file_list_sorted[x]]['data_start']  # data start date of file 1
                data_start_file = pd.to_datetime(data_start_file).strftime('%Y-%m-%dT%H:%M:%SZ')
                data_end_file = data['deployments'][d]['streams'][s]['files'][file_list_sorted[x]]['data_end'] # data end date of file 1
                data_end_file = pd.to_datetime(data_end_file).strftime('%Y-%m-%dT%H:%M:%SZ')

                if cnf is not 0: # any file other than the first file in the list
                    timedelta = pd.to_datetime(data_start_file) - pd.to_datetime(file_end) # compare the end date of the previous file to the start date of the next file
                    if timedelta < pd.Timedelta(days=0.016666666666667): # 1min/day = 1/60 = 0.016666666666667
                        pass
                    else:
                        newline = (stream, deployment, file_end, data_start_file, str(timedelta), '', '','check: time difference between .nc files', user)
                        stream_csv_issues.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline) # write output to the issues file

                gaps = data['deployments'][d]['streams'][s]['files'][file_list_sorted[x]]['time_gaps']  # list of gaps

                # if there are no gaps in the file, identifies available data
                if not gaps:
                    if cnf is 0:
                        var_gap_end = data_begin
                        if cnf is len(
                                file_list) - 1:  # last file, check against deployment end date from asset management
                            newline = (stream, deployment, var_gap_end, data_end_file, '', 'NOT_EVALUATED', '',
                                       'check: evaluate parameters', user)
                            file.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)
                            if deploy_end is data_end_file:
                                pass
                            else:
                                newline = (stream, deployment, data_end_file, deploy_end, '', 'NOT_AVAILABLE', '',
                                           'check: data end does not equal deployment end date', user)
                                file.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)
                    else: # combine annotations if the gap between files is < 1 day
                        if timedelta < pd.Timedelta(days=1):
                            print cnf, var_gap_end
                            if cnf is len(file_list) - 1:  # last file, check against deployment end date from asset management
                                newline = (stream, deployment, var_gap_end, data_end_file, '', 'NOT_EVALUATED', '',
                                           'check: evaluate parameters', user)
                                file.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)
                                if deploy_end is data_end_file:
                                    pass
                                else:
                                    newline = (stream, deployment, data_end_file, deploy_end, '', 'NOT_AVAILABLE', '',
                                               'check: data end does not equal deployment end date', user)
                                    file.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)


                # if there are gaps in the file, list the gap date ranges and available data date ranges
                else:
                    cnt = 0 # count of gaps
                    for g in gaps:
                        gap_start = g[0]  # gap start date
                        gap_end = g[-1]   # gap end date

                        if cnf is 0:  # first file
                            if cnt is 0:  # first gap
                                newline = (stream,deployment,data_start_file,gap_start,'','NOT_EVALUATED','','check: evaluate parameters',user)
                                file.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)
                        else:
                            newline = (stream,deployment,var_gap_end,gap_start,'','NOT_EVALUATED','','check: evaluate parameters',user)
                            file.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)

                        newline = (stream, deployment, gap_start, gap_end, '', 'NOT_AVAILABLE', '', 'check: data gap', user)
                        file.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)

                        if cnf is len(file_list)-1: # last file, check against deployment end date from asset management
                            newline = (stream, deployment, gap_end, data_end_file, '', 'NOT_EVALUATED', '', 'check: evaluate parameters', user)
                            file.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)
                            if deploy_end == 'None':
                                newline = (stream,deployment,deploy_begin,deploy_end,'','','','check: no deployment end date in asset management',user)
                                stream_csv_issues.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)
                            else:
                                if deploy_end is data_end_file:
                                    pass
                                else:
                                    newline = (stream,deployment,data_end_file,deploy_end,'','NOT_AVAILABLE','','check: data end does not equal deployment end date',user)
                                    file.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)

                        var_gap_end = gap_end
                        cnt = cnt + 1

                cnf = cnf + 1
                file_end = data_end_file


def main(dataset, save_dir, user):
    file = open(dataset, 'r')
    data = json.load(file)
    ref_des = data.get('ref_des')
    site = ref_des.split('-')[0]

    make_dir(save_dir)
    site_dir = os.path.join(save_dir, site)
    make_dir(site_dir) # make the site directory

    refdes_dir = os.path.join(site_dir, ref_des)
    make_dir(refdes_dir) # make the ref des directory

    drafts_dir = os.path.join(refdes_dir, 'drafts')
    make_dir(drafts_dir)

    dm_stream = dataset.split('/')[-1].split('__')[1]
    stream_name = dm_stream.split('-')[-1]
    stream_file = os.path.join(drafts_dir,dm_stream + '_processed_on_' + dt.now().strftime('%Y-%m-%dT%H%M%S') + '.csv')
    stream_file_other = os.path.join(drafts_dir,'collocated_inst_streams_processed_on_' + dt.now().strftime('%Y-%m-%dT%H%M%S') + '.csv')
    stream_file_issues = os.path.join(drafts_dir,dm_stream + '_issues_processed_on_' + dt.now().strftime('%Y-%m-%dT%H%M%S') + '.csv')

    with open(stream_file,'a') as stream_csv: # stream-level annotation .csv
        writer = csv.writer(stream_csv)
        writer.writerow(['Level', 'Deployment', 'StartTime', 'EndTime', 'Annotation', 'Status', 'Redmine#', 'Todo', 'reviewed_by'])
        with open(stream_file_other,'a') as stream_csv_other: # stream-level annotation .csv
            writer = csv.writer(stream_csv_other)
            writer.writerow(['Level', 'Deployment', 'StartTime', 'EndTime', 'Annotation', 'Status', 'Redmine#', 'Todo', 'reviewed_by'])
            with open(stream_file_issues,'a') as stream_csv_issues: # stream-level issues annotation .csv
                writer = csv.writer(stream_csv_issues)
                writer.writerow(['Level', 'Deployment', 'StartTime', 'EndTime', 'Note', 'Status', 'Redmine#', 'Todo', 'reviewed_by'])
                extract_gaps(data, stream_csv, stream_csv_other, stream_csv_issues, stream_name, user)

    # delete the collocated_inst_streams file if file is empty
    if os.stat(stream_file_other).st_size < 90:
        os.remove(stream_file_other)


if __name__ == '__main__':
    dataset = '/Users/leila/Documents/OOI_GitHub_repo/output_ric/CE04OSPS-SF01B-2A-CTDPFA107-streamed/test/CE04OSPS-SF01B-2A-CTDPFA107__streamed-ctdpf_sbe43_sample__requested-20170322T221944.json'
    annotations_dir = '/Users/leila/Documents/OOI_GitHub_repo/output_ric/CE04OSPS-SF01B-2A-CTDPFA107-streamed/test'
    user = 'leila'
    main(dataset, annotations_dir, user)