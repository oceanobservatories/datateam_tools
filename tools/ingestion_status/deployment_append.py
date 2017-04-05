#! /usr/local/bin/python

"""
Created on Mon Feb 10 2017

@author: leilabbb
"""

import pandas as pd
import glob
import os

'''
This script combines all deployment sheets in a local repo /Users/leila/Documents/OOI_GitHub_repo/asset-management/deployment

'''

rootdir = '/Users/leila/Documents/OOI_GitHub_repo/asset-management/deployment'


df = pd.DataFrame()
for root, dirs, files in os.walk(rootdir):
    for f in files:
        if f.endswith('_Deploy.csv'):
            with open(os.path.join(root,f),'r') as csv_file:
                filereader = pd.read_csv(csv_file)

                # remove rows with empty cells
                filereader.dropna(how="all", inplace=True)

                # replace NAN by empty string
                filereader.fillna('', inplace=True)

                # add the file name as a column
                filereader['Deploy_csv_filename'] = str(f)


                # append all deployment sheets in one file
                df = df.append(filereader)
                df.fillna('', inplace=True)

file_header = ['CUID_Deploy', 'deployedBy', 'CUID_Recover', 'recoveredBy', 'Reference Designator',
               'deploymentNumber', 'versionNumber', 'startDateTime', 'stopDateTime', 'mooring.uid',
               'node.uid', 'sensor.uid', 'lat', 'lon', 'orbit', 'deployment_depth', 'water_depth', 'notes']

df.to_csv('/Users/leila/Documents/OOI_GitHub_repo/ingest-status/Deploy_file.csv', index = False, columns = file_header)