#!/usr/bin/env python
"""
@file parse_from_ingest.py
@author Mike Smith
@email michaesm@marine.rutgers.edu
@brief This tool reads in OOI ingestion CSVs and parses the appropriate raw data
@purpose Provide a quick way to parse raw data files that SHOULD have already been routed correctly to their
appropriate drivers in the ingestion csvs
"""
import os
import glob
import pandas as pd
from utils.parse_file import find_driver, ParticleHandler, log_timing, monkey_patch_particles, StopWatch
import urllib2
import pickle


def uframe_routes():
    """
    This function loads a pickle file containing all uframe_routes to their proper drivers
    :return: dictionary containing the uframe_routes to driver
    :rtype: dictionary
    """
    fopen = urllib2.urlopen('https://raw.githubusercontent.com/ooi-data-review/parse_spring_files/master/uframe_routes.pkl')
    ingest_dict = pickle.load(fopen)
    return ingest_dict


def make_dir(save_dir):
    try:  # Check if the save_dir exists already... if not, make it
        os.mkdir(save_dir)
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


def main(ingest_file, save_dir, dav_mount, file_format='csv', splitter='/OMC/'):
    """
    Main method when sript is imported
    :param ingest_file: The full path and filename of the ingestion file that you want to chek
    :param save_dir: directory where you want to save your ingestion analysis data (raw data and status csv)
    :param dav_mount: Directory on local computer to OOI Raw Data dav server
    :param file_format: Format for parsed data. Optional. 'csv' is default. Options: 'csv', 'json', 'pd-pickle', 'xr-pickle'
    :param splitter: expression to split the omc server location on. This is used to transform the directory of the omc server to something we can actually can read from, the webdav server
    :return:
    """
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) # base path of this toolbox
    make_dir(save_dir)
    ingest_dict = uframe_routes()

    data = []
    fname = os.path.basename(ingest_file).split('.csv')[0]
    make_dir(save_dir)
    new_dir = os.path.join(save_dir, fname)
    make_dir(new_dir)

    df = pd.read_csv(ingest_file)
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
        if len(matches) > 10:
            matches = matches[:5]

        out_rd = os.path.join(new_dir, refdeg); make_dir(out_rd)

        out_ds = os.path.join(out_rd, data_source); make_dir(out_ds)

        run(base_path, driver, matches, file_format, out_ds)
        path, dirs, files = os.walk(out_ds).next()
        file_count_new = len(files)
        print '%i stream files created.' % file_count_new
        data.append((refdeg, data_source, route, driver, web_dir, file_count_new))
        print ' '

    df = pd.DataFrame(data, columns=['refdeg', 'data_source', 'uframe_route', 'driver', 'web_dir', 'file_count'])
    df.to_csv(os.path.join(save_dir, fname + '-ingest_results.csv'), index=False)

if __name__ == '__main__':
    # change pandas display width to view longer dataframes
    ingest_file = '/Users/mikesmith/Documents/git/ooi-integration/ingestion-csvs/CE05MOAS-GL311/CE05MOAS-GL311_D00003_ingest.csv'
    save_dir = '/Users/mikesmith/Documents/git/ooi-integration/ingestion-csvs/CE05MOAS-GL311/'
    file_format = 'csv'
    dav_mount = '/Volumes/dav/'
    splitter = '/OMC/'
    main(ingest_file, save_dir, dav_mount, file_format, splitter)