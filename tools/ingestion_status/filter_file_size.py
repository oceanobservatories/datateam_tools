#! /usr/local/bin/python

"""
Created on Fri Apr 21 2017

@author: leilabbb
"""


import os
import os.path, time
import pandas

#basedir = '/Volumes/dav/CP04OSSM/D00005/cg_data/dcl11/mopak'
# basedir = '/Volumes/dav/CP02PMUI/D00008/imm/mmp/A*.DEC'
# basedir = '/Volumes/dav/GI01SUMO/R00001/cg_data/dcl11/imm/CTDMO_sn37-11481/'
basedir = '/Volumes/dav/CE01ISSM/D00001/dcl17/velpt*/'
compare_2_date = pandas.to_datetime('today')
compare_2_date = compare_2_date.strftime("%d/%m/%Y")
file_1K_size = 0
file_1Kplus_size = 0
file_of_today = 0

#print len([name for name in os.listdir(basedir) if os.path.isfile('*.DAT 2')])
#print len([name for name in os.listdir(basedir) if os.path.isfile(os.path.join(basedir, '*.DAT 2'))])
# path, dirs, files = os.walk(basedir).next()
# file_count = len(files)
# print file_count

k = 0
for f in os.listdir(basedir):
    path = os.path.join(basedir, f)
    if os.path.isfile(path):
        file_modified = pandas.to_datetime(time.ctime(os.path.getmtime(path)))
        file_modified = file_modified.strftime("%d/%m/%Y")
        file_size = os.path.getsize(path)
        if file_size <= 1024: #513410:
            file_1K_size += 1

        if file_size > 1024:
            file_1Kplus_size += 1

        if file_modified == compare_2_date:
            file_of_today += 1
    else:
        print f

    k += 1
print 'number of files in directory', k
print 'file <= 1k = ', file_1K_size
print 'file > 1K = ', file_1Kplus_size
print 'file of today = ', file_of_today

