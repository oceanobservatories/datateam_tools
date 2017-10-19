#!/usr/bin/env python
"""
@file annotate_streams.py
@author Lori Garzio
@email lgarzio@marine.rutgers.edu
@brief Read the .json output from analyze_nc_data.py, extract data availability and gaps, and write the results to stream-level annotation csvs (https://github.com/ooi-data-review/annotations)
@purpose Auto-populate the stream-level annotation csvs with data availability and data gaps
@usage
dataset Path to .json output from analyze_nc_data.py
annotations_dir Directory that contains the annotation csvs cloned from https://github.com/ooi-data-review/annotations,
in which to save the output
user User that completed the review
"""

try: import simplejson as json
except ImportError: import json
import csv
import os
from datetime import datetime as dt
import re
import itertools
import pandas as pd
import shutil


def make_dir(save_dir):
    try:  # Check if the save_dir exists already... if not, make it
        os.mkdir(save_dir)
    except OSError:
        pass


def atoi(text):
    return int(text) if text.isdigit() else text


def natural_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    '''
    return [ atoi(c) for c in re.split('(\d+)', text) ]


def check_deploy_end(s, d, deploy_begin, deploy_end, data_end, stream_csv_issues, outfile, user, review_date):
    '''
    checks for an end date from asset management (param: deploy_end). if there is a deployment end date in asset management,
    checks if the end date from the data file (param: data_end) matches the deployment end date
    '''
    format = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n'
    if deploy_end == 'None':  # if there is no deployment end date in asset management
        newline = (s, d, deploy_begin, deploy_end, '', '', '', 'check: no deployment end date in asset management', user, review_date)
        stream_csv_issues.write(format % newline)
    else:
        timedelta_deployend = pd.to_datetime(deploy_end) - pd.to_datetime(data_end) # compare the asset management deployment end date with data file end date
        if pd.Timedelta(minutes=1) < timedelta_deployend < pd.Timedelta(days=1): # if the difference is less than 1 day, print to issues file
            newline = (s, d, data_end, deploy_end, str(timedelta_deployend), '', '', 'check: difference between deploy end date and data file end date', user, review_date)
            stream_csv_issues.write(format % newline)
        else:  # if the difference is > 1 day, print to stream files
            if timedelta_deployend > pd.Timedelta(days=1):
                newline = (s, d, data_end, deploy_end, '', 'NOT_AVAILABLE', '', 'check: difference between data end and deployment end date is: ' + str(timedelta_deployend), user, review_date)
                outfile.write(format % newline)


def annotate_one_gap(s, d, data_begin, gap_start, gap_end, timedelta_gap, data_end, outfile, user, review_date):
    '''
    stream annotations for only one gap in a deployment
    '''
    format = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n'
    newline = (s, d, data_begin, gap_start, '', 'NOT_EVALUATED', '', 'check: evaluate parameters', user, review_date)
    outfile.write(format % newline)

    newline = (s, d, gap_start, gap_end, '', 'NOT_AVAILABLE', '', 'check: data gap: ' + str(timedelta_gap), user, review_date)
    outfile.write(format % newline)

    newline = (s, d, gap_end, data_end, '', 'NOT_EVALUATED', '', 'check: evaluate parameters', user, review_date)
    outfile.write(format % newline)


def gaps_between_files(data, file_list_sorted, d, s, stream_csv_issues, user, review_date):
    '''
    check for gaps between deployment files of >1 minute
    '''
    format = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n'
    cnf = 0
    for x in file_list_sorted:
        data_start_file = data['deployments'][d]['streams'][s]['files'][x]['data_start']  # data start date of file 1
        data_start_file = pd.to_datetime(data_start_file).strftime('%Y-%m-%dT%H:%M:%SZ')
        data_end_file = data['deployments'][d]['streams'][s]['files'][x]['data_end']  # data end date of file 1
        data_end_file = pd.to_datetime(data_end_file).strftime('%Y-%m-%dT%H:%M:%SZ')

        if cnf is not 0: # any file other than the first file in the list
            timedelta = pd.to_datetime(data_start_file) - pd.to_datetime(file_end) # compare the end date of the previous file to the start date of the next file
            if timedelta < pd.Timedelta(minutes=1):
                pass
            else:
                newline = (s, d, file_end, data_start_file, str(timedelta), '', '','check: time difference between .nc files', user,review_date)
                stream_csv_issues.write(format % newline) # write output to the issues file

        file_end = data_end_file
        cnf = cnf + 1


def extract_gaps(data, stream_csv, stream_csv_other, stream_csv_issues, stream_name, user, review_date):
    format = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n'
    # read in deployment information from asset management and start and end dates of the data requested by deployment
    deployment_list = data['deployments']
    deployment_list_sorted = deployment_list.keys()
    deployment_list_sorted.sort(key = natural_keys)  # sorts the deployments

    for d in deployment_list_sorted:
        temp = data['deployments'][d]
        deploy_begin = temp['start'] # asset management start date
        deploy_end = temp['end'] # asset management end date
        data_begin = temp['data_times']['start'] # first data file start date
        data_begin = pd.to_datetime(data_begin).strftime('%Y-%m-%dT%H:%M:%SZ')
        data_end = temp['data_times']['end']  # last data file end date
        data_end = pd.to_datetime(data_end).strftime('%Y-%m-%dT%H:%M:%SZ')

        # read in stream information
        for s in temp['streams']:
            if s == stream_name:  # if stream matches the stream from the file name, write to main .csv
                outfile = stream_csv
            else:
                outfile = stream_csv_other  # if the stream does not match the stream from the file name, write to the collocated instrument .csv

            # test if deployment begin from data equals deployment begin from asset management.
            timedelta_deploystart = pd.to_datetime(data_begin) - pd.to_datetime(deploy_begin) # compare the asset management deployment start date with data file start date
            if pd.Timedelta(minutes=1) < timedelta_deploystart < pd.Timedelta(days=1):  # if the difference is less than 1 day, print to issues file
                newline = (s, d, deploy_begin, data_begin, str(timedelta_deploystart), '', '', 'check: difference between deploy start date and data file start date', user, review_date)
                stream_csv_issues.write(format % newline)
            else:  # if the difference is > 1 day, print to stream files
                if timedelta_deploystart > pd.Timedelta(days=1):
                    newline = (s, d, deploy_begin, data_begin, '', 'NOT_AVAILABLE', '', 'check: difference between data begin and deployment begin date is: ' + str(timedelta_deploystart), user, review_date)
                    outfile.write(format % newline)

            stream_data = temp['streams'][s]
            file_list = stream_data['files'] # list of files for the deployment
            file_list_sorted = file_list.keys()
            file_list_sorted.sort(key=natural_keys)  # sorts the data files
            gap_dict = dict(time_gaps=[])

            # check for gaps between deployment files of >1 minute
            gaps_between_files(data, file_list_sorted, d, s, stream_csv_issues, user, review_date)

            # read data file information
            for x in file_list_sorted:
                gaps = stream_data['files'][x]['time_gaps']
                gap_dict['time_gaps'].append(gaps)  # grab the gaps from each file and append to the dictionary

            gap_dict = list(itertools.chain.from_iterable(gap_dict['time_gaps']))  # flattens the dictionary

            if not gap_dict: # if there are no gaps
                newline = (s, d, data_begin, data_end, '', 'NOT_EVALUATED', '', 'check: evaluate parameters', user, review_date)
                outfile.write(format % newline)

                check_deploy_end(s, d, deploy_begin, deploy_end, data_end, stream_csv_issues, outfile, user, review_date)

            cnt = 0
            for g in gap_dict: # if there are gaps
                gap_start = g[0]
                gap_end = g[1]
                timedelta_gap = pd.to_datetime(gap_end) - pd.to_datetime(gap_start)

                if len(gap_dict) == 1: # stream annotations if there is only 1 gap
                    annotate_one_gap(s, d, data_begin, gap_start, gap_end, timedelta_gap, data_end, outfile, user, review_date)
                    check_deploy_end(s, d, deploy_begin, deploy_end, data_end, stream_csv_issues, outfile, user, review_date)

                else:  # stream annotations if there are multiple gaps
                    if cnt == 0: # first gap
                        newline = (s, d, data_begin, gap_start, '', 'NOT_EVALUATED', '', 'check: evaluate parameters', user, review_date)
                        outfile.write(format % newline)

                        newline = (s, d , gap_start, gap_end, '', 'NOT_AVAILABLE', '', 'check: data gap: '+ str(timedelta_gap), user, review_date)
                        outfile.write(format % newline)
                    else:
                        newline = (s, d, g_end_prev, gap_start, '', 'NOT_EVALUATED', '', 'check: evaluate parameters', user, review_date)
                        outfile.write(format % newline)

                        newline = (s, d, gap_start, gap_end, '', 'NOT_AVAILABLE', '', 'check: data gap: '+ str(timedelta_gap), user, review_date)
                        outfile.write(format % newline)

                        if cnt is len(gap_dict)-1: # last gap
                            newline = (s, d, gap_end, data_end, '', 'NOT_EVALUATED', '', 'check: evaluate parameters', user, review_date)
                            outfile.write(format % newline)

                            check_deploy_end(s, d, deploy_begin, deploy_end, data_end, stream_csv_issues, outfile, user, review_date)

                g_end_prev = gap_end
                cnt = cnt + 1


def main(dataset, save_dir, user):
    t_now = dt.now().strftime('%Y-%m-%dT%H%M%S')
    review_date = dataset.split('_')[-1].split('.')[0][0:8]
    review_date = dt.strptime(review_date, '%Y%m%d').strftime('%Y-%m-%dT%H:%M:%SZ')

    file = open(dataset, 'r')
    data = json.load(file)
    ref_des = data.get('ref_des')

    drafts_dir = os.path.join(save_dir, 'file_analysis')
    make_dir(drafts_dir)

    stream_name = dataset.split('/')[-1].split('-')[-1].split('__requested')[0]
    delivered_method = dataset.split('/')[-1].split('__')[1].split('-')[0]

    stream_file_draft = os.path.join(drafts_dir, '{}-processed_on-{}.csv'.format(ref_des + '-' + delivered_method + '-' + stream_name,t_now))
    # stream_file = os.path.join(save_dir, '{}.csv'.format(delivered_method + '-' + stream_name))
    stream_file_other = os.path.join(drafts_dir, 'collocated_inst_streams_processed_on-{}.csv'.format(t_now))
    stream_file_issues = os.path.join(drafts_dir, '{}-issues_processed_on-{}.csv'.format(ref_des + '-' + delivered_method + '-' + stream_name, t_now))

    #print 'Processing {} issues into internal draft stream csv files '.format(dataset,)
    with open(stream_file_draft,'a') as stream_csv: # stream-level annotation .csv
        writer = csv.writer(stream_csv)
        writer.writerow(['Level', 'Deployment', 'StartTime', 'EndTime', 'Annotation', 'Status', 'Redmine#', 'Todo', 'ReviewedBy', 'ReviewedDate'])
        with open(stream_file_other,'a') as stream_csv_other: # stream-level annotation .csv
            writer = csv.writer(stream_csv_other)
            writer.writerow(['Level', 'Deployment', 'StartTime', 'EndTime', 'Annotation', 'Status', 'Redmine#', 'Todo', 'ReviewedBy', 'ReviewedDate'])
            with open(stream_file_issues,'a') as stream_csv_issues: # stream-level issues annotation .csv
                writer = csv.writer(stream_csv_issues)
                writer.writerow(['Level', 'Deployment', 'StartTime', 'EndTime', 'Notes', 'Status', 'Redmine#', 'Todo', 'ReviewedBy', 'ReviewedDate'])
                extract_gaps(data, stream_csv, stream_csv_other, stream_csv_issues, stream_name, user, review_date)

    # delete the collocated_inst_streams file if empty
    if os.stat(stream_file_other).st_size < 100:
        os.remove(stream_file_other)

    # shutil.copyfile(stream_file_draft, stream_file)

if __name__ == '__main__':
    dataset = '/Users/leila/Documents/OOI_GitHub_repo/output/rest_in_class/CP02PMUI-WFP01-01-VEL3DK000__recovered_wfp-vel3d_k_wfp_instrument__requested_20170517T223334.json'
    annotations_dir = '/Users/leila/Documents/OOI_GitHub_repo/output/annotations'

    user = 'Leila'
    main(dataset, annotations_dir, user)