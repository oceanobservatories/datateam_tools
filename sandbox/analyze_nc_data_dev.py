#!/usr/bin/env python

from tools import check_data
import csv

input_url_list = '/Users/knuth/Desktop/requests/thredds_urls.csv'
save_dir = '/Users/knuth/Desktop/output'
my_list = []
with open(input_url_list, 'rb') as f:
    reader = csv.reader(f)
    my_list= list(reader)

tds_catalogs = []
for i in my_list:
    for j in i:
        tds_catalogs.append(j)
        
for url in tds_catalogs:
	try:
		check_data.main(url, save_dir)
	except UnboundLocalError:
		print "This url points to an empty catalog"
		continue
