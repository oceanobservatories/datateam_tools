#!/usr/bin/env python
# Script to download .nc files from a THREDDS catalog directory
# Written by Sage 4/5/16
# Modified by Mike Smith 2/24/17 for data download
from xml.dom import minidom
import os
import urllib2
import urllib


def create_dir(new_dir):
    # Check if dir exists.. if it doesn't... create it.
    if not os.path.isdir(new_dir):
        try:
            os.makedirs(new_dir)
        except OSError:
            if os.path.exists(new_dir):
                pass
            else:
                raise


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


def main(server_url, request_url, output_dir):
    catalog_url = server_url + '/thredds/catalog/' + request_url + '/catalog.xml'
    files=[]
    datasets = get_elements(catalog_url, 'dataset', 'urlPath')
    for d in datasets:
        if (d[-3:]=='.nc'):
            files.append(d)
    count = 0
    print files
    split_file = files[0].split('/')
    build_dir = split_file[2].split('-')
    new_dir = os.path.join(output_dir, build_dir[1], build_dir[2], '{}-{}'.format(build_dir[3], build_dir[4]), 'data')
    create_dir(new_dir)

    for f in files:
        count +=1
        file_url = server_url + '/thredds/fileServer/' + f
        file_name = new_dir + '/' + file_url.split('/')[-1]
        print 'Downloading ' + str(count) + ' of ' + str(len(files)) + ' ' + f
        urllib.urlretrieve(file_url,file_name)


# Run main function when in comand line mode        
if __name__ == '__main__':
    server_url = 'https://opendap.oceanobservatories.org'
    request_url = 'ooi/friedrich-knuth-gmail/20170222T210321-CE02SHBP-LJ01D-06-CTDBPN106-streamed-ctdbp_no_sample'

    # Output directory
    output_dir = '/home/ooi/array/'
    main(server_url, request_url, output_dir)