#! /usr/local/bin/python

"""
Created on Wen Feb 01 2017

#@author: leila
"""

import pandas as pd
import numpy as np
import os

'''
This script merge multiple files
'''
platform = 'CP05MOAS-GL388'
file5 = '/Users/leila/Documents/OOI_GitHub_repo/repos/ooi-integration/asset-management/deployment/' + platform + '_Deploy.csv'
filed = '/Users/leila/Documents/OOI_GitHub_repo/work/ingest-status/000_ingestpy_run_results/' + platform + '_infrastructure.csv'
file6 = '/Users/leila/Documents/OOI_GitHub_repo/work/ingest-status/000_ingestpy_run_results/' + platform + '_19-07-2017_rawfiles_query.csv'
ofile = '/Users/leila/Documents/OOI_GitHub_repo/work/ingest-status/000_ingestpy_run_results/' + platform + '_19-07-2017_rawfiles_status.csv'

# merge with sensor_bulk_load 'refdes_list','method_list'
rfd = pd.read_csv(filed)
sbln = rfd.rename(columns={'method_list': 'data_source'})
sbld = sbln.rename(columns={'refdes_list': 'reference_designator'})

rf5 = pd.read_csv(file5)
sbl = rf5.rename(columns={'Reference Designator': 'reference_designator'})
sbl5 = sbl.rename(columns={'deploymentNumber': 'deployment#'})

rf6 = pd.read_csv(file6)

mf56 = pd.merge(rf6, sbl5, on=['reference_designator','deployment#'], how='left')

mf56d = pd.merge(sbld, mf56, on=['reference_designator','data_source'], how='outer')

# check deployment number: _ingest.csv vs file_mask
mf56d['file_name_consistency'] = mf56d['ingest_csv_filename'].str.split('_').str[1]
mf56d['mask_name_consistency'] = mf56d['filename_mask'].str.split('/').str[5]
mf56d['compare_DH'] = np.where(mf56d['file_name_consistency'] == mf56d['mask_name_consistency'], mf56d['file_name_consistency'], 'False')

header = ['reference_designator', 'data_source', 'type_list',
          'ingest_csv_filename', 'platform', 'deployment#', 'uframe_route_y',
          'filename_mask', 'number_files', 'file of today', 'file <= 1k',
          'file > 1K', 'compare_DH',
          'Automated_status', 'status', 'notes_x', 'CUID_Deploy','compare',
          'deployedBy', 'CUID_Recover', 'recoveredBy', 'versionNumber',
          'startDateTime', 'stopDateTime', 'mooring.uid', 'node.uid',
          'sensor.uid', 'lat', 'lon', 'orbit', 'deployment_depth', 'water_depth', 'notes_y']

mf56d.to_csv(ofile, index = False, columns = header)
