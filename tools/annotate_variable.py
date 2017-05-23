#!/usr/bin/env python
"""
@file annotate_variable.py
@author Leila Belabbassi
@email leila@marine.rutgers.edu
@brief Read the .json output from analyze_nc_data.py and extract variables' tests results
@purpose Provide a human-readable file of variables' tests results
@usage
dataset Path to .json output from analyze_nc_data.py
save_dir Location to save output
"""

try: import simplejson as json
except ImportError: import json
import csv
import os
from datetime import datetime as dt
import re
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


def annotate_variable(data, parameter_csv, parameter_issues_csv, stream_name, review_date, user='root'):

    format = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n'
    deployment_list = data['deployments']
    deployment_list_sorted = deployment_list.keys()
    deployment_list_sorted.sort(key = natural_keys)  # sorts the deployments

    deploy_cnt = 0
    for d in deployment_list_sorted:
        deployment = d
        s = stream_name

        deployment_data_begin = data['deployments'][d]['data_times']['start'] # first data file start date
        deployment_data_begin = pd.to_datetime(deployment_data_begin).strftime('%Y-%m-%dT%H:%M:%SZ')
        deployment_data_end = data['deployments'][d]['data_times']['end']  # last data file end date
        deployment_data_end = pd.to_datetime(deployment_data_end).strftime('%Y-%m-%dT%H:%M:%SZ')

        file_list = data['deployments'][d]['streams'][s]['files']
        file_list_sorted = file_list.keys()
        file_list_sorted.sort(key = natural_keys)  # sorts the files


        cnt = 0
        for x in file_list_sorted:
            data_begin = data['deployments'][d]['streams'][s]['files'][x]['data_start'] # start date of file
            data_begin = pd.to_datetime(data_begin).strftime('%Y-%m-%dT%H:%M:%SZ')
            data_end = data['deployments'][d]['streams'][s]['files'][x]['data_end'] # end date of file
            data_end = pd.to_datetime(data_end).strftime('%Y-%m-%dT%H:%M:%SZ')
            vars_not_in_db = data['deployments'][d]['streams'][s]['files'][x]['vars_not_in_db']
            vars_not_in_file = data['deployments'][d]['streams'][s]['files'][x]['vars_not_in_file']

            if not vars_not_in_db:
                pass
            else:
                for i in vars_not_in_db:
                    newline = (i, deployment, data_begin, data_end, '', 'vars_not_in_db', '',
                               'check: variable listed in file but not in database', user, review_date)
                    parameter_issues_csv.write(format % newline)

            if not vars_not_in_file:
                pass
            else:
                for ii in vars_not_in_file:
                    if ii == 'time':  # time will never be listed as a variable in the files
                        pass
                    else:
                        newline = (ii, deployment, data_begin, data_end, '', 'NOT_AVAILABLE', '',
                                   'check: variable listed in database but not in file', user, review_date)
                        parameter_csv.write(format % newline)

            vars = data['deployments'][d]['streams'][s]['files'][x]['variables']
            misc = ['time','volt']
            reg_ex = re.compile('|'.join(misc))
            sci_vars = [nn for nn in vars if not reg_ex.search(nn)]

            for v in sci_vars:
                # print v
                parameter = v
                if deploy_cnt is 0 and cnt is 0:  # print all variables in the file to be used in the timeline plot
                    newline = (parameter, '', '', '', '', '', '', '', user, review_date)
                    parameter_csv.write(format % newline)

                t1 = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['available']
                # if intern(t1) is intern('False'):
                if t1 == 'False':
                    pass
                else:
                    try:
                        t2 = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['all_nans']
                        # if intern(t2) is intern('False'):
                        if t2 == 'False':
                            pass
                        else:
                            # print t2
                            flag = 'FAILED'
                            newline = (parameter, deployment, data_begin, data_end, 'applies to one file', flag, '',
                                       'tested all_nans: ' + t2, user, review_date)
                            parameter_csv.write(format % newline)
                    except KeyError:
                        pass

                    try:
                        t3 = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['fill_test']
                        # if intern(t3) is intern('False'):
                        if t3 == 'False':
                            pass
                        else:
                            # print t3
                            flag = 'FAILED'
                            newline = (parameter, deployment, data_begin, data_end, 'applies to one file', flag, '',
                                       'tested fill_test: ' + t3, user, review_date)
                            parameter_csv.write(format % newline)
                    except KeyError:
                        pass


                    try:
                        t4 = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['fill_value']
                        if cnt is 0:
                            if t4 == -9999999.0:
                                pass

                            else:
                                flag = 'Fill Value'
                                newline = (parameter, deployment, deployment_data_begin, deployment_data_end,
                                           'extracted from 1st file of '+  str(len(file_list_sorted))+ ' files',
                                           flag, '', 'tested: ' + str(-9999999.0) + ' found: ' + str(t4),
                                           user, review_date)
                                parameter_issues_csv.write(format % newline)

                    except KeyError:
                        pass

                    try:
                        global_max = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['global_max']
                        global_min = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['global_min']
                        if cnt is 0:
                            newline = (parameter, deployment, deployment_data_begin, deployment_data_end,
                                       'extracted from 1st file of '+  str(len(file_list_sorted))+ ' files',
                                       'Global Range Values', '',
                                       'global min = ' + str(global_min) + ' global_max = ' + str(global_max),
                                       user, review_date)
                            parameter_issues_csv.write(format % newline)

                    except KeyError:
                        pass

                    try:
                        t5 = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['global_range_test']
                        if not t5:
                            pass
                        else:
                            newline = (parameter, deployment, data_begin, data_end, 'applies to one file',
                                       'Global Range QC Test', '', 'check: test triggered', user, review_date)
                            parameter_issues_csv.write(format % newline)
                    except KeyError:
                        pass

                    try:
                        t6 = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['dataqc_spiketest']
                        if not t6:
                            pass
                        else:
                            newline = (parameter, deployment, data_begin, data_end, 'applies to one file',
                                       'Spike QC Test', '', 'check: test triggered', user, review_date)
                            parameter_issues_csv.write(format % newline)
                    except KeyError:
                        pass

                    try:
                        t7 = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['dataqc_stuckvaluetest']
                        if not t7:
                            pass
                        else:
                            newline = (parameter, deployment, data_begin, data_end, 'applies to one file',
                                       'Stuck Value QC Test', '', 'check: test triggered', user, review_date)
                            parameter_issues_csv.write(format % newline)
                    except KeyError:
                        pass

            cnt = cnt + 1

        deploy_cnt = deploy_cnt + 1


def main(dataset, save_dir, user):
    t_now = dt.now().strftime('%Y-%m-%dT%H%M%S')
    review_date = dataset.split('-')[-1].split('.')[0][0:8]
    review_date = dt.strptime(review_date, '%Y%m%d').strftime('%Y-%m-%dT%H:%M:%SZ')

    file = open(dataset, 'r')
    data = json.load(file)
    ref_des = data.get('ref_des')
    site = ref_des.split('-')[0]

    make_dir(save_dir)
    site_dir = os.path.join(save_dir, site)
    make_dir(site_dir)  # make the site directory

    refdes_dir = os.path.join(site_dir, ref_des)
    make_dir(refdes_dir)  # make the ref des directory

    drafts_dir = os.path.join(refdes_dir, 'internal_drafts')
    make_dir(drafts_dir)

    dm_stream = dataset.split('/')[-1].split('__')[1]
    stream_name = dm_stream.split('-')[-1]
    parameter_file = os.path.join(refdes_dir, '{}-parameters.csv'.format(dm_stream))
    parameter_file_draft = os.path.join(drafts_dir, '{}-parameters_processed_on-{}.csv'.format(dm_stream, t_now))
    parameter_issues_draft = os.path.join(drafts_dir, '{}-parameter_issues_processed_on-{}.csv'.format(dm_stream, t_now))

    print 'Processing {} issues into internal draft parameter csv files '.format(dataset)
    with open(parameter_file_draft, 'a') as parameter_csv:  # stream-level annotation .csv
        writer = csv.writer(parameter_csv)
        writer.writerow(['Level', 'Deployment', 'StartTime', 'EndTime', 'Annotation', 'Status', 'Redmine#', 'Todo', 'ReviewedBy', 'ReviewedDate'])
        with open(parameter_issues_draft, 'a') as parameter_issues_csv:  # stream-level annotation .csv
            writer = csv.writer(parameter_issues_csv)
            writer.writerow(['Level', 'Deployment', 'StartTime', 'EndTime', 'Notes', 'Test', 'Redmine#', 'Todo', 'ReviewedBy', 'ReviewedDate'])

            annotate_variable(data, parameter_csv, parameter_issues_csv, stream_name, review_date, user)

    shutil.copyfile(parameter_file_draft, parameter_file)

if __name__ == '__main__':
    dataset = '/Users/leila/Documents/OOI_GitHub_repo/output/rest_in_class/CP02PMUI-WFP01-03-CTDPFK000__recovered_wfp-ctdpf_ckl_wfp_instrument_recovered__requested-20170517T223350.json'
    annotations_dir = '/Users/leila/Documents/OOI_GitHub_repo/output/'
    user = 'Leila'
    main(dataset, annotations_dir, user)
