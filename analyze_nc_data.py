#!/usr/bin/env python
"""
@author Mike Smith
@email michaesm@marine.rutgers.edu
@brief This is a wrapper script that imports the tool, check_data, as a python method.
@usage
datasets List of thredds catalog xmls, path to a csv file containing multiple xml urls (carriage return between each), or a string containing a single catalog xml
save_dir Location to save .json output files containing analysis information
annotations_dir Directory that contains the annotation csvs cloned from https://github.com/ooi-data-review/annotations
user User that completed the review
"""
import csv
from tools import check_data, stream_gaps_annotations

datasets = 'https://opendap.oceanobservatories.org/thredds/catalog/ooi/lgarzio-marine-rutgers/20170322T160522-CE04OSBP-LJ01C-06-CTDBPO108-streamed-ctdbp_no_sample/catalog.xml'
# datasets = '/Users/mikesmith/Downloads/deployment0001_RS01SUM1-LJ01B-09-PRESTB102-streamed-prest_real_time_20140918T133030-20160723T012756.763249.nc'
# datasets = '/Users/lgarzio/Documents/OOI/DataReviews/2017/RIC/CTDs/cabled/CE04OSBP-LJ01C-06-CTDBPO108/deploy3/data/files.csv'
save_dir = '/Users/lgarzio/Documents/OOI/DataReviews/2017/RIC/CTDs/cabled/CE04OSBP-LJ01C-06-CTDBPO108/'
annotations_dir = '/Users/lgarzio/Documents/repo/OOI/ooi-data-review/annotations/annotations'
user = 'lori'


if type(datasets) == str:
    if datasets.endswith('xml'):
        datasets = [datasets]
    elif datasets.endswith('csv'):
        with open(datasets, 'rb') as f:
            reader = csv.reader(f)
            datasets = list(reader)
            datasets = [x[0] for x in datasets]
    elif datasets.endswith('nc'):
        datasets = [datasets]

for url in datasets:
    json_file = check_data.main(url, save_dir)
    stream_gaps_annotations.main(json_file, annotations_dir, user)
