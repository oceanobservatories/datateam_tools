#! /usr/local/bin/python

"""
Created on Wen Feb 01 2017

#@author: leila
"""

import pandas as pd
import os

'''
This script merge multiple files
'''
platform = 'CP05MOAS-A6264'
file5 = '/Users/leila/Documents/OOI_GitHub_repo/asset-management/deployment/' + platform + '_Deploy.csv'
file6 = '/Users/leila/Documents/OOI_GitHub_repo/ingest-status/' + platform + '_ingest_file.csv'
ofile = '/Users/leila/Documents/OOI_GitHub_repo/ingest-status/' + platform + '_status_file.csv'

# merge with sensor_bulk_load

rf5 = pd.read_csv(file5)
sbl = rf5.rename(columns={'Reference Designator': 'reference_designator'})
sbl5 = sbl.rename(columns={'deploymentNumber': 'deployment#'})

rf6 = pd.read_csv(file6)

mf56 = pd.merge(rf6, sbl5, on=['reference_designator','deployment#'], how='left')


header = ['ingest_csv_filename', 'platform', 'deployment#', 'uframe_route', 'filename_mask', 'number_files',
          'reference_designator', 'data_source', 'status', 'source', 'note',
          'CUID_Deploy', 'deployedBy', 'CUID_Recover', 'recoveredBy', 'reference_designator', 'deployment#', 'versionNumber',
          'startDateTime', 'stopDateTime', 'mooring.uid', 'node.uid', 'sensor.uid', 'lat', 'lon', 'orbit', 'deployment_depth',
          'water_depth', 'notes']

mf56.to_csv(ofile, index = False, columns = header)