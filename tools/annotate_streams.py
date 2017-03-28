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

import simplejson as json
import csv
import os
from datetime import datetime as dt


def make_dir(save_dir):
    try:  # Check if the save_dir exists already... if not, make it
        os.mkdir(save_dir)
    except OSError:
        pass


def extract_gaps(data, stream_csv, user):
    for d in data['deployments']:
        deployment = d
        deploy_begin = data['deployments'][d]['begin']
        deploy_end = data['deployments'][d]['end']
        for s in data['deployments'][d]['streams']:
            stream = s
            for x in data['deployments'][d]['streams'][s]['files']:
                data_begin = data['deployments'][d]['streams'][s]['files'][x]['data_start']
                data_end = data['deployments'][d]['streams'][s]['files'][x]['data_end']
                gaps = data['deployments'][d]['streams'][s]['files'][x]['time_gaps']

                if deploy_begin is data_begin:
                    pass
                else:
                    newline = (stream,deployment,deploy_begin,data_begin,'','NOT_AVAILABLE','','check - data begin does not equal deployment begin date',user)
                    stream_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)


                if not gaps:
                    if deploy_end is data_end:
                        pass
                    else:
                        newline = (stream,deployment,data_end,deploy_end,'','NOT_AVAILABLE','','check - data end does not equal deployment end date',user)
                        stream_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)
                else:
                    cnt = 0
                    for g in gaps:
                        gap_start = g[0]
                        gap_end = g[-1]
                        if cnt is 0:
                            newline = (stream,deployment,data_begin,gap_start,'','NOT_EVALUATED','','check - evaluate parameters',user)
                            stream_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)
                        else:
                            newline = (stream,deployment,g1,gap_start,'','NOT_EVALUATED','','check - evaluate parameters',user)
                            stream_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)

                        newline = (stream,deployment,gap_start,gap_end,'','NOT_AVAILABLE','','check - data gap',user)
                        stream_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)

                        if cnt is len(gaps)-1:
                            newline = (stream,deployment,gap_end,data_end,'','NOT_EVALUATED','','check - evaluate parameters',user)
                            stream_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)

                            if deploy_end is data_end:
                                pass
                            else:
                                newline = (stream,deployment,data_end,deploy_end,'','NOT_EVALUATED','','check - data end does not equal deployment end date',user)
                                stream_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)

                        g1 = gap_end
                        cnt = cnt + 1


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
    stream_file = os.path.join(drafts_dir,dm_stream + '_processed_on_' + dt.now().strftime('%Y-%m-%dT%H%M%S') + '.csv')

    with open(stream_file,'a') as stream_csv: # stream-level annotation .csv
        if os.stat(stream_file).st_size==0:
            writer = csv.writer(stream_csv)
            writer.writerow(['Level', 'Deployment', 'StartTime', 'EndTime', 'Annotation', 'Status',
                                       'Redmine#', 'Todo', 'reviewed_by'])
            extract_gaps(data,stream_csv,user)
        else:
            print 'File already exists'
            pass


if __name__ == '__main__':
    dataset = '/Users/lgarzio/Documents/OOI/DataReviews/2017/RIC/CTDs/cabled/CE04OSBP-LJ01C-06-CTDBPO108/CE04OSBP-LJ01C-06-CTDBPO108__streamed-ctdbp_no_sample__requested.json'
    annotations_dir = '/Users/lgarzio/Documents/repo/OOI/ooi-data-review/annotations/annotations'
    user = 'lori'
    main(dataset, annotations_dir, user)