#!/usr/bin/env python
"""
@file file_list.py
@author Lori Garzio
@email lgarzio@marine.rutgers.edu
@brief Create a csv containing the file paths for all netCDF files in a directory
@usage
rootdir Directory that contains netCDF files, and where the output will be saved.
"""

import os
import pandas as pd

rootdir = '/Users/lgarzio/Documents/OOI/DataReviews/data'

list = []
for root, dirs, files in os.walk(rootdir):
    for f in files:
        if f.endswith('.nc'):
            fpath = root + '/' + f
            list.append(fpath)
    df = pd.DataFrame(list)
    df.to_csv(os.path.join(rootdir,'files.csv'), index=False, header=False)