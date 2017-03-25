#!/usr/bin/env python
"""
@file stream_gaps_annotations.py
@author Lori Garzio
@email lgarzio@marine.rutgers.edu
@brief Read the .json output from analyze_nc_data.py, extract data gaps and write the results to stream-level annotation csvs (https://github.com/ooi-data-review/annotations)
@purpose Auto-populate the stream-level annotation csvs with data gaps
@usage
dataset Path to .json output from analyze_nc_data.py
annotations_dir Directory that contains the annotation csvs cloned from https://github.com/ooi-data-review/annotations,
in which to save the output
user User that completed the review
"""

import simplejson as json
import csv
import os


def make_dir(save_dir):
    try:  # Check if the save_dir exists already... if not, make it
        os.mkdir(save_dir)
    except OSError:
        pass


def extract_gaps(data, stream_csv, user):
    for d in data['deployments']:
        deployment = d
        for s in data['deployments'][d]['streams']:
            stream = s
            for x in data['deployments'][d]['streams'][s]['files']:
                gaps = data['deployments'][d]['streams'][s]['files'][x]['time_gaps']
                if not gaps:
                    pass
                else:
                    for g in gaps:
                        gap_start = g[0]
                        gap_end = g[-1]
                        newline = (stream,deployment,gap_start,gap_end,'Data gap','NOT_AVAILABLE','','check',user)
                        stream_csv.write('%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % newline)


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

    dm_stream = dataset.split('/')[-1].split('__')[1]
    stream_file = os.path.join(refdes_dir,dm_stream + '.csv')

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