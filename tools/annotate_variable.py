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

def annotate_variable(data, parameter_csv, user):
    for d in data['deployments']:
        deployment = d
        for s in data['deployments'][d]['streams']:
            for x in data['deployments'][d]['streams'][s]['files']:
                data_begin = data['deployments'][d]['streams'][s]['files'][x]['data_start']
                data_end = data['deployments'][d]['streams'][s]['files'][x]['data_end']
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
                        newline = (parameter, deployment, data_begin, data_end, '', flag, '', 'tested -' + t1, user)
                        parameter_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)

                    t2 = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['all_nans']
                    if intern(t2) is intern('False'):
                        pass
                    else:
                        print t2
                        flag = 'FAILED'
                        newline = (parameter, deployment, data_begin, data_end, '', flag, '', 'tested -' + t2, user)
                        parameter_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)

                    t3 = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['fill_test']
                    if intern(t3) is intern('False'):
                        pass
                    else:
                        print t3
                        flag = 'FAILED'
                        newline = (parameter, deployment, data_begin, data_end, '', flag, '', 'tested -' + t3, user)
                        parameter_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)

                    t4 = data['deployments'][d]['streams'][s]['files'][x]['variables'][v]['fill_value']
                    if t4 == -9999999.0:
                        pass
                    else:
                        print t4
                        flag = 'FAILED'
                        newline = (parameter, deployment, data_begin, data_end, '', flag, '', 'tested -' + str(-9999999.0) + 'found:' + str(t4) , user)
                        parameter_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)


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
    parameter_file = os.path.join(drafts_dir,
                               dm_stream + '_parameters_processed_on_' + dt.now().strftime('%Y-%m-%dT%H%M%S') + '.csv')

    with open(parameter_file, 'a') as parameter_csv:  # stream-level annotation .csv
        if os.stat(parameter_file).st_size == 0:
            writer = csv.writer(parameter_csv)
            writer.writerow(['Level', 'Deployment', 'StartTime', 'EndTime', 'Annotation', 'Status',
                             'Redmine#', 'Todo', 'reviewed_by'])
            annotate_variable(data, parameter_csv, user)
        else:
            print 'File already exists'
            pass

if __name__ == '__main__':
    dataset = '/Users/leila/Documents/OOI_GitHub_repo/output_ric/CE04OSPS-SF01B-2A-CTDPFA107-streamed/test/GP03FLMA-RIM01-02-CTDMOG040__telemetered-ctdmo_ghqr_sio_mule_instrument__requested-20170315T142924.json'
    annotations_dir = '/Users/leila/Documents/OOI_GitHub_repo/output_ric/CE04OSPS-SF01B-2A-CTDPFA107-streamed/test'
    user = 'leila'
    main(dataset, annotations_dir, user)