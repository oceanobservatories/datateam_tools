import os
import csv

out = '/output/directory'
list_of_refdes = 'reference_designators.csv'


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
		new_dir = array + '/' + site + '/' + node + '/' + sensor
		save_dir = os.path.join(out,new_dir)
		create_dir(save_dir)