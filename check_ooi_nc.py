#! /usr/bin/env python
import os
import xarray as xr
import pandas as pd
import re
import fnmatch
# import click as click

from thredds_crawler.crawl import Crawl
fmt = '%Y.%m.%dT%H.%M.00'


def mk_str(attrs, str_type='t'):
    """
    make a string of either 't' for title. 's' for file save name.
    """
    site = attrs['subsite']
    node = attrs['node']
    sensor = attrs['sensor']
    stream = attrs['stream']

    if str_type is 's':
        string = site + '-' + node + '-' + sensor + '-' + stream + '-'
    else:
        string = site + '-' + node + ' Stream: ' + stream + '\n' + 'Variable: '
    return string



# @click.command()
# @click.argument('files', nargs=1, type=click.Path())
# @click.argument('out', nargs=1, type=click.Path(exists=False))
def main(url='http://opendap-devel.ooi.rutgers.edu:8090/thredds/catalog/first-in-class/catalog.xml', stmt='.*ncml'):
    C = Crawl(url, select=[stmt])
    tds = 'http://opendap-devel.ooi.rutgers.edu:8090/thredds/dodsC/'
    reg_ex = re.compile('|'.join(['config', 'meta', 'engine', 'diag']))

    data = []
    for dataset in C.datasets:
        if reg_ex.search(dataset.id) is not None:
            continue
        file = tds + dataset.id
        with xr.open_dataset(file) as ds:
            ds_disk = ds.swap_dims({'obs': 'time'}) # change dimensions from 'obs' to 'time'
            # ds_variables = ds.data_vars.keys()  # List of dataset variables
            refdes = ds.subsite + '-' + ds.node + '-' + ds.sensor
            stream = ds_disk.stream  # List stream name associated with the data
            delivery = ds.collection_method
            start = ds.time_coverage_start
            end = ds.time_coverage_end
            # data.append((refdes, stream, delivery, 'Yes', 'Yes', start + ' to ' + end))
    pd.DataFrame(data, columns=['RefDes','Stream','Delivery Method','Data Downloaded','Data range - MIO','Time Range'])
    pd.DataFrame.to_csv('/Users/michaesm/Documents/Summary.csv')



if __name__ == '__main__':
    main()