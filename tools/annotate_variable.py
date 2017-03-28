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

import simplejson as json
import csv
import os
from datetime import datetime as dt
import re


def make_dir(save_dir):
    try:  # Check if the save_dir exists already... if not, make it
        os.mkdir(save_dir)
    except OSError:
        pass

def annotate_variable(data, parameter_csv, parameter_issues_csv, stream_name, user):
    for d in data['deployments']:
        deployment = d
        s = stream_name
        for x in data['deployments'][d]['streams'][s]['files']:
            data_begin = data['deployments'][d]['streams'][s]['files'][x]['data_start']
            data_end = data['deployments'][d]['streams'][s]['files'][x]['data_end']
            vars_not_in_db = data['deployments'][d]['streams'][s]['files'][x]['vars_not_in_db']
            vars_not_in_file = data['deployments'][d]['streams'][s]['files'][x]['vars_not_in_file']

            if not vars_not_in_db:
                pass
            else:
                for i in vars_not_in_db:
                    newline = (i, deployment, data_begin, data_end, '', 'vars_not_in_db', '', 'check: variable listed in file but not in database', user)
                    parameter_issues_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)

            if not vars_not_in_file:
                pass
            else:
                for ii in vars_not_in_file:
                    if ii == 'time':  # time will never be listed as a variable in the files
                        pass
                    else:
                        newline = (ii, deployment, data_begin, data_end, '', 'NOT_AVAILABLE', '', 'check: variable listed in database but not in file', user)
                        parameter_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)

            vars = data['deployments'][d]['streams'][s]['files'][x]['variables']
            misc = ['time']
            reg_ex = re.compile('|'.join(misc))
            sci_vars = [nn for nn in vars if not reg_ex.search(nn)]

            for v in sci_vars:
                print v
                parameter = v

                t1 = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['available']
                if intern(t1) is intern('True'):
                    pass
                else:
                    print t1
                    flag = 'NOT_AVAILABLE'
                    newline = (parameter, deployment, data_begin, data_end, '', flag, '', 'tested availability: ' + t1, user)
                    parameter_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)

                try:
                    t2 = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['all_nans']
                    if intern(t2) is intern('False'):
                        pass
                    else:
                        print t2
                        flag = 'FAILED'
                        newline = (parameter, deployment, data_begin, data_end, '', flag, '', 'tested all_nans: ' + t2, user)
                        parameter_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)
                except KeyError:
                    pass

                try:
                    t3 = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['fill_test']
                    if intern(t3) is intern('False'):
                        pass
                    else:
                        print t3
                        flag = 'FAILED'
                        newline = (parameter, deployment, data_begin, data_end, '', flag, '', 'tested fill_test: ' + t3, user)
                        parameter_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)
                except KeyError:
                    pass

                try:
                    t4 = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['fill_value']
                    if t4 == -9999999.0:
                        pass
                    else:
                        print t4
                        flag = 'Fill Value'
                        newline = (parameter, deployment, data_begin, data_end, '', flag, '', 'tested: ' + str(-9999999.0) + ' found: ' + str(t4) , user)
                        parameter_issues_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)
                except KeyError:
                    pass

                try:
                    global_max = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['global_max']
                    global_min = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['global_min']
                    newline = (parameter, deployment, data_begin, data_end, '', 'Global Range Values', '', 'global min = ' + str(global_min) + ' global_max = ' + str(global_max), user)
                    parameter_issues_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)
                except KeyError:
                    pass

                try:
                    t5 = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['global_range_test']
                    if not t5:
                        pass
                    else:
                        newline = (parameter, deployment, data_begin, data_end, '', 'Global Range QC Test', '', 'check: test triggered', user)
                        parameter_issues_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)
                except KeyError:
                    pass

                try:
                    t6 = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['dataqc_spiketest']
                    if not t6:
                        pass
                    else:
                        newline = (parameter, deployment, data_begin, data_end, '', 'Spike QC Test', '', 'check: test triggered', user)
                        parameter_issues_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)
                except KeyError:
                    pass

                try:
                    t7 = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['dataqc_stuckvaluetest']
                    if not t7:
                        pass
                    else:
                        newline = (parameter, deployment, data_begin, data_end, '', 'Stuck Value QC Test', '', 'check: test triggered', user)
                        parameter_issues_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)
                except KeyError:
                    pass


def main(dataset, save_dir, user):
    file = open(dataset, 'r')
    data = json.load(file)
    ref_des = data.get('ref_des')
    site = ref_des.split('-')[0]

    make_dir(save_dir)
    site_dir = os.path.join(save_dir, site)
    make_dir(site_dir)  # make the site directory

    refdes_dir = os.path.join(site_dir, ref_des)
    make_dir(refdes_dir)  # make the ref des directory

    drafts_dir = os.path.join(refdes_dir, 'drafts')
    make_dir(drafts_dir)

    dm_stream = dataset.split('/')[-1].split('__')[1]
    stream_name = dm_stream.split('-')[-1]
    parameter_file = os.path.join(drafts_dir,
                               dm_stream + '_parameters_processed_on_' + dt.now().strftime('%Y-%m-%dT%H%M%S') + '.csv')
    parameter_issues = os.path.join(drafts_dir,
                               dm_stream + '_parameter_issues_processed_on_' + dt.now().strftime('%Y-%m-%dT%H%M%S') + '.csv')

    with open(parameter_file, 'a') as parameter_csv:  # stream-level annotation .csv
        writer = csv.writer(parameter_csv)
        writer.writerow(['Level', 'Deployment', 'StartTime', 'EndTime', 'Annotation', 'Status', 'Redmine#', 'Todo', 'reviewed_by'])
        with open(parameter_issues, 'a') as parameter_issues_csv:  # stream-level annotation .csv
            writer = csv.writer(parameter_issues_csv)
            writer.writerow(['Level', 'Deployment', 'StartTime', 'EndTime', 'Annotation', 'Test', 'Redmine#', 'Todo', 'reviewed_by'])

            annotate_variable(data, parameter_csv, parameter_issues_csv, stream_name, user)


if __name__ == '__main__':
#    dataset = '/Users/leila/Documents/OOI_GitHub_repo/output_ric/CE04OSPS-SF01B-2A-CTDPFA107-streamed/test/GP03FLMA-RIM01-02-CTDMOG040__telemetered-ctdmo_ghqr_sio_mule_instrument__requested-20170315T142924.json'
#    annotations_dir = '/Users/leila/Documents/OOI_GitHub_repo/output_ric/CE04OSPS-SF01B-2A-CTDPFA107-streamed/test'
    dataset = '/Users/lgarzio/Documents/OOI/DataReviews/2017/RIC/test/GI01SUMO-SBD11-06-METBKA000__recovered_host-metbk_a_dcl_instrument_recovered__requested-20170227T202508.json'
    annotations_dir = '/Users/lgarzio/Documents/OOI/DataReviews/2017/RIC/test'
    user = 'leila'
    main(dataset, annotations_dir, user)