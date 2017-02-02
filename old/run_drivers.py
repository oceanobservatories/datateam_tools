#!/usr/bin/env python

from utils.parse_file import find_driver, ParticleHandler, log_timing, monkey_patch_particles, StopWatch
import fnmatch as fn
import os

out_dir = '/Users/michaesm/Documents/parsed/'
file_format = 'csv'
base_path = '/Users/michaesm/Documents/dev/repos/ooi-data-review/check_ooi_nc' #base path of this toolbox

# raw_dir = '/Volumes/dav/CP02PMUI/R00001/dcl00/ADCP_sn19336/'
# regex = '*.000'
# driver = 'mi.dataset.driver.adcps_jln.stc.adcps_jln_stc_recovered_driver'

raw_dir = '/Volumes/dav/CE09OSSM/R00001/cg_data/dcl11/metbk/'
regex = '*.metbk.log'
driver = 'mi.dataset.driver.pco2w_abc.pco2w_abc_recovered_driver'

raw_dir = '/Volumes/dav/CE01ISSM/R00001/instrmt/dcl35/PRESF_*/*_presf_*.hex'


def run(base_path, driver, files, fmt, out):
    monkey_patch_particles()
    module = find_driver(driver)
    particle_handler = ParticleHandler(output_path=out, formatter=fmt)
    for file_path in files:
        with StopWatch('Parsing file: %s took' % file_path):
            module.parse(base_path, file_path, particle_handler)

    particle_handler.write()

matches = []
for root, dirnames, filenames in os.walk(raw_dir):
    for filename in fn.filter(filenames, regex):
        matches.append(os.path.join(root, filename))
import glob

run(base_path, driver, matches, file_format, out_dir)