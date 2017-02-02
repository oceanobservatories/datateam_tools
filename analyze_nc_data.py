#!/usr/bin/env python
"""
@author Mike Smith
@email michaesm@marine.rutgers.edu
@brief This is a wrapper script that imports the tool, check_data, as a python method.
@usage
tds_catalogs List of thredds catalog xmls
save_dir Location to save csv files containing analysis information
"""

from tools import check_data

tds_catalogs = ['https://opendap.oceanobservatories.org/thredds/catalog/ooi/friedrich-knuth-gmail/20170123T165201-RS03AXBS-MJ03A-06-PRESTA301-streamed-prest_real_time/catalog.xml',
                'https://opendap.oceanobservatories.org/thredds/catalog/ooi/m-smith3887-gmail/20170130T215345-CE04OSSM-RID26-07-NUTNRB000-recovered_inst-nutnr_b_instrument_recovered/catalog.xml']
save_dir = '/Users/mikesmith/Documents/'

for url in tds_catalogs:
    check_data.main(url, save_dir)
