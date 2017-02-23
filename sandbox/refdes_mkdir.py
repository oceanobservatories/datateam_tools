import os
import csv

out = '/Users/knuth/Documents/ooi/data/cruisedata'
list_of_refdes = '/Users/knuth/Desktop/stream_list_20170130.csv'


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



with open(list_of_refdes, 'rb') as targets:
	targets_dict = csv.reader(targets, lineterminator='\n')
	for row in targets_dict:
		line = row[0]
		array = str(line[0:2])
		site = str(line[0:8])
		node = str(line[9:14])
		sensor = str(line[15:])
		env_qc_data = 'env_qc_data'
		new_dir = array + '/' + site + '/' + node + '/' + sensor + '/' + env_qc_data
		save_dir = os.path.join(out,new_dir)
		create_dir(save_dir)