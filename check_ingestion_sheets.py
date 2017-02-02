#!/usr/bin/env python
"""
@author Mike Smith
@email michaesm@marine.rutgers.edu
@brief This is a wrapper script that imports the tool, run_ingest, as a python method.
@usage
ingest_files List of ingestion csvs
save_dir Location to save csv files containing analysis information and raw data
"""
import os
from tools import run_ingest

ingest_files = ['/Users/mikesmith/Documents/git/ooi-integration/ingestion-csvs/CE05MOAS-GL311/CE05MOAS-GL311_D00003_ingest.csv']
save_dir = '/Users/mikesmith/Documents/git/ooi-integration/ingestion-csvs/'
dav_mount = '/Volumes/dav/'
file_format = 'csv'

for i in ingest_files:
    new_dir = os.path.join(save_dir, os.path.dirname(i).split('/')[-1])
    run_ingest.main(i, new_dir, dav_mount, file_format)