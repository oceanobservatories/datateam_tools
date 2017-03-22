#!/usr/bin/env python
"""
@author Mike Smith
@email michaesm@marine.rutgers.edu
@brief This is a wrapper script that imports the tool, check_data, as a python method.
@usage
tds_catalogs List of thredds catalog xmls, path to a csv file containing multiple xml urls (carriage return between each), or a string containing a single catalog xml
save_dir Location to save csv files containing analysis information
"""
import csv
from tools import check_data

# datasets = ['https://opendap.oceanobservatories.org/thredds/catalog/ooi/m-smith3887-gmail/20170221T202829-CE01ISSP-SP001-09-CTDPFJ000-telemetered-ctdpf_j_cspp_instrument/catalog.xml']
#datasets = '/Users/lgarzio/Documents/OOI/DataReviews/2017/RIC/CTDs/CE04OSPS-SF01B-2A-CTDPFA107/deployment1/data/deployment0001_CE04OSPS-SF01B-2A-CTDPFA107-streamed-ctdpf_sbe43_sample_20141105T213049.640058-20150801T141332.013419.nc'
# datasets = 'https://opendap.oceanobservatories.org/thredds/catalog/ooi/datareview_2017_spring/20170202T222124-RS03AXPS-PC03A-06-VADCPA301-streamed-vadcp_velocity_beam/catalog.xml'
datasets = '/Users/lgarzio/Documents/OOI/DataReviews/2017/RIC/CTDs/cabled/CE04OSBP-LJ01C-06-CTDBPO108/deploy1/data/files.csv'
save_dir = '/Users/lgarzio/Documents/OOI/DataReviews/2017/RIC/CTDs/cabled/CE04OSBP-LJ01C-06-CTDBPO108/deploy1/'


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
    check_data.main(url, save_dir)