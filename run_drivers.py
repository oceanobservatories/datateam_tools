#!/usr/bin/env python

from utils.parse_file import find_driver, ParticleHandler, log_timing, monkey_patch_particles, StopWatch
import fnmatch as fn
import os

out_dir = '/Users/michaesm/Documents/parsed/'
file_format = 'csv'
base_path = '/Users/michaesm/Documents/dev/repos/ooi-data-review/check_ooi_nc' #base path of this toolbox

# raw_dir = '/Volumes/dav/CE09OSSM/R00001/instrmts/dcl35/'
# regex= '*pco2w*.txt'
# driver = 'mi.dataset.driver.pco2w_abc.pco2w_abc_recovered_driver'

raw_dir = '/Volumes/dav/CE06ISSM/R00002/cg_data/dcl16/velpt2/'
regex='*.log'
driver = 'mi.dataset.driver.velpt_ab.velpt_ab_recovered_driver'

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

run(base_path, driver, matches, file_format, out_dir)