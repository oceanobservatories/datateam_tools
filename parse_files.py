#!/usr/bin/env python
import sys, os
sys.path.append('/Users/michaesm/Documents/dev/repos/oceanobservatories/mi-dataset')
from utils.parse_file import find_driver, ParticleHandler, log_timing, monkey_patch_particles, StopWatch
import datetime as dt

def run(base_path, driver, files, fmt, out):
    monkey_patch_particles()
    # log.error('Importing driver: %s', driver)
    module = find_driver(driver)
    particle_handler = ParticleHandler(output_path=out, formatter=fmt)
    for file_path in files:
        # log.info('Begin parsing: %s', file_path)
        with StopWatch('Parsing file: %s took' % file_path):
            module.parse(base_path, file_path, particle_handler)

    particle_handler.write()

# res/config/mi-logging.yml
driver = 'mi.dataset.driver.dosta_abcdjm/cspp/dosta_abcdjm_cspp_recovered_driver'
files = ['/Users/michaesm/Downloads/cspp/ucspp_32260420_PPB_OPT.txt']
file_format = 'csv'
out_dir = './output'
base_path = '/Users/michaesm/Documents/dev/repos/ooi-data-review/check_ooi_nc'
sys.stderr = open(driver.split('/')[-1] + '_parsed_' + dt.datetime.strftime(dt.datetime.utcnow(), '%Y-%m-%d_%H%M%S') + '.log', 'w')
run(base_path, driver, files, file_format, out_dir)