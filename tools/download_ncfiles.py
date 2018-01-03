#!/usr/bin/env python
"""
Created on 5/10/16

@author: Sage Lichtenwalner
@modified: lgarzio on 12/7/2017
Script to download .nc files from a THREDDS directory

output_dir: local directory to which files are saved
url: THREDDS directory containing .nc files
"""

from xml.dom import minidom
import urllib2
import urllib
import os


def get_elements(url, tag_name, attribute_name):
    """Get elements from an XML file"""
    usock = urllib2.urlopen(url)
    xmldoc = minidom.parse(usock)
    usock.close()
    tags = xmldoc.getElementsByTagName(tag_name)
    attributes=[]
    for tag in tags:
        attribute = tag.getAttribute(attribute_name)
        attributes.append(attribute)
    return attributes


def main(output_dir, url):
    server_url = 'https://opendap.oceanobservatories.org'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    #catalog_url = server_url + '/thredds/catalog/' + request_url + '/catalog.xml'
    if url.endswith('.html'):
        catalog_url = url.replace('.html', '.xml')
    files=[]
    datasets = get_elements(catalog_url, 'dataset', 'urlPath')
    for d in datasets:
        if (d[-3:]=='.nc'):
            files.append(d)
    count = 0
    print files
    for f in files:
        count +=1
        file_url = server_url + '/thredds/fileServer/' + f
        file_name = output_dir + '/' + file_url.split('/')[-1]
        #     file_prefix = file_url.split('/')[-1][:-3]
        #     file_name = file_prefix + '_' + str(count) + '.nc'
        print 'Downloading ' + str(count) + ' of ' + str(len(files)) + ' ' + f
        urllib.urlretrieve(file_url,file_name)


if __name__ == '__main__':
    output_dir = '/Users/lgarzio/Documents/OOI/GI01SUMO'
    url = 'https://opendap.oceanobservatories.org/thredds/catalog/ooi/lgarzio-marine-rutgers/20171207T142520-GI01SUMO-SBD12-06-METBKA000-recovered_host-metbk_hourly/catalog.html'
    main(output_dir, url)