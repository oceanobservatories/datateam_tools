#!/usr/bin/env python
"""
@author Mike Smith
@email michaesm@marine.rutgers.edu
@brief This is a wrapper script that imports the tools: check_data, annotate_streams and annotate_variable as python methods.
@usage
datasets List of thredds catalog xmls, path to a csv file containing multiple urls (carriage return between each), or a string containing a single catalog url
save_dir Location to save .json output files containing analysis information
annotations_dir Directory that contains the annotation csvs cloned from https://github.com/ooi-data-review/annotations
user User that completed the review
"""
import csv
import os

from tools import check_data, annotate_streams, annotate_variable

datasets = 'https://opendap.oceanobservatories.org/thredds/catalog/ooi/michaesm-marine-rutgers/20170421T141015-CE09OSPM-WFP01-03-CTDPFK000-recovered_wfp-ctdpf_ckl_wfp_instrument_recovered/catalog.html'
cwd = os.getcwd()
save_dir = '{}/output/rest_in_class'.format(cwd)
annotations_dir = '{}/output/annotations'.format(cwd)
user = 'michaesm'

if type(datasets) == str:
    if datasets.endswith('csv'):
        with open(datasets, 'rb') as f:
            reader = csv.reader(f)
            datasets = list(reader)
            datasets = [x[0] for x in datasets]
    else:
        datasets = [datasets]

for url in datasets:
    json_file = check_data.main(url, save_dir)
    annotate_streams.main(json_file, annotations_dir, user)
    annotate_variable.main(json_file, annotations_dir, user)
