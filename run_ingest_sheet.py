#!/usr/bin/env python
"""
@file parse_from_ingest.py
@author Mike Smith
@brief This tool reads in ingestion CSVs and parses the appropriate raw data
The purpose is to provide a quick way to parse raw data files that SHOULD have
already been routed correctly to their appropriate drivers in the ingestion csvs
"""
import os
import glob
import pandas as pd
from utils.parse_file import find_driver, ParticleHandler, log_timing, monkey_patch_particles, StopWatch
import urllib2
import pickle

csv_file = '/Users/michaesm/Documents/dev/repos/ooi-integration/ingestion-csvs/CE07SHSM/CE07SHSM_D00004_ingest.csv'
out_dir = '/Users/michaesm/Documents/dev/repos/ooi-integration/ingestion-csvs/CE07SHSM/'
file_format = 'csv'
base_path = '/Users/michaesm/Documents/dev/repos/ooi-data-review/check_ooi_nc' #base path of this toolbox
dav_mount = '/Volumes/dav/'
splitter = '/OMC/'
# dav_mount = '/Volumes/dav/RS01SBPD/PD01A/'
# splitter = '/PD01A/'


def uframe_routes():
    """
    This function loads a pickle file containing all uframe_routes to their proper drivers
    :return: dictionary containing the uframe_routes to driver
    :rtype: dictionary
    """
    fopen = urllib2.urlopen('https://raw.githubusercontent.com/ooi-data-review/parse_spring_files/master/uframe_routes.pkl')
    ingest_dict = pickle.load(fopen)
    return ingest_dict


def make_dir(dir):
    try:  # Check if the save_dir exists already... if not, make it
        os.mkdir(out_dir)
    except OSError:
        pass


def run(base_path, driver, files, fmt, out):
    """
    This script runs the drivers on given raw data files
    :param base_path: Base path of this scripts location
    :param driver: name of the driver
    :param files: a list of files
    :type files: list
    :param fmt: 'csv', 'json', 'pd-pickle', 'xr-pickle'
    :param out: save directory
    """
    monkey_patch_particles()
    module = find_driver(driver)
    particle_handler = ParticleHandler(output_path=out, formatter=fmt)
    for file_path in files:
        with StopWatch('Parsing file: %s took' % file_path):
            module.parse(base_path, file_path, particle_handler)

    particle_handler.write()

make_dir(out_dir)
ingest_dict = uframe_routes()

data = []
fname = os.path.basename(csv_file).split('.csv')[0]
make_dir(out_dir)
new_dir = os.path.join(out_dir, fname)
make_dir(new_dir)

df = pd.read_csv(csv_file)
for row in df.itertuples():
    route = row.uframe_route
    try:
        web_dir = os.path.join(dav_mount, row.filename_mask.split(splitter)[1])
    except AttributeError:
        web_dir = 'None'

    refdeg = row.reference_designator
    data_source = row.data_source
    print 'Ingestion row'
    if '#' in route:
        if len(route.strip('#')) is 0:
            # route = 'N/A'
            driver = 'N/A'
        else:
            try:
                driver = ingest_dict[route]
            except KeyError:
                print 'No spring file/driver exists for the given route.'
                driver = 'Does not exist for given route.'

        print 'Reference Designator: %s, Data Source: %s' % (refdeg, data_source)
        print 'Route: %s, Driver: %s, file_mask: %s' % (route, 'N/A', web_dir)
        print 'Skipping above due to commented file'
        print ' '
        data.append((refdeg, data_source, route, driver, web_dir, '#'))
        continue
    elif not route:
        continue
    else:
        try:
            driver = ingest_dict[route]
        except KeyError:
            print 'No spring file/driver exists for the given route.'
            driver = 'Does not exist for given route.'
            print ' '
            data.append((refdeg, data_source, route, driver, web_dir, 0))
            continue

        print 'Reference Designator: %s, Data Source: %s' % (refdeg, data_source)
        print 'Route: %s, Driver: %s, file_mask: %s' % (route, driver, web_dir)
        print 'Parsing data'

    matches = glob.glob(web_dir)
    # if len(matches) > 10:
    #     matches = matches[:5]

    out_rd = os.path.join(new_dir, refdeg); make_dir(out_rd)

    out_ds = os.path.join(out_rd, data_source); make_dir(out_ds)

    run(base_path, driver, matches, file_format, out_ds)
    path, dirs, files = os.walk(out_ds).next()
    file_count_new = len(files)
    print '%i stream files created.' % file_count_new
    data.append((refdeg, data_source, route, driver, web_dir, file_count_new))
    print ' '

df = pd.DataFrame(data, columns=['refdeg', 'data_source', 'uframe_route', 'driver', 'web_dir', 'file_count'])
df.to_csv(os.path.join(out_dir, fname + '-ingest_results.csv'), index=False)