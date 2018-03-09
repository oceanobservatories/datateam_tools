#!/usr/bin/env python
"""
@author Mike Smith
@email michaesm@marine.rutgers.edu
@brief This is a wrapper script that imports the tools: check_data, annotate_streams and annotate_variable as python methods.
@usage
datasets List of thredds catalog xmls, path to a csv file containing multiple urls (carriage return between each), or a string containing a single catalog url
save_dir Location to save all output files
user User that completed the review
username Username to access the OOI API
token Password to access the OOI API
"""
import csv
import os

from tools import check_data, annotate_streams, annotate_variable, m2m_get_annotations_subsite

datasets = 'https://opendap.oceanobservatories.org/thredds/catalog/ooi/michaesm-marine-rutgers/20170421T141015-CE09OSPM-WFP01-03-CTDPFK000-recovered_wfp-ctdpf_ckl_wfp_instrument_recovered/catalog.html'
cwd = os.getcwd()
save_dir = '{}/output/rest_in_class'.format(cwd)
user = 'michaesm'
username = 'API_username'
token = 'API_password'

if type(datasets) == str:
    if datasets.endswith('csv'):
        with open(datasets, 'rb') as f:
            reader = csv.reader(f)
            datasets = list(reader)
            datasets = [x[0] for x in datasets]
    else:
        datasets = [datasets]

for url in datasets:
    splitter = url.split('/')[-2].split('-')
    subsite = splitter[1]
    refdes = '-'.join([splitter[1],splitter[2],splitter[3],splitter[4]])

    subsite_dir = os.path.join(save_dir, subsite)
    check_data.make_dir(subsite_dir) # make the site directory

    refdes_dir = os.path.join(subsite_dir, refdes)
    check_data.make_dir(refdes_dir) # make the ref des directory

    json_file = check_data.main(url, refdes_dir)
    annotate_streams.main(json_file, refdes_dir, user)
    annotate_variable.main(json_file, refdes_dir, user)
    #m2m_get_annotations_refdes.main(username, token, refdes_dir, refdes)
    m2m_get_annotations.main(username, token, subsite, subsite_dir)