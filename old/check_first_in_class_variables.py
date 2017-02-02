#!/usr/bin/env python
import pandas as pd
from thredds_crawler.crawl import Crawl
import fnmatch
import xarray as xr
import sys, os
from utils.parse_file import find_driver, ParticleHandler, log_timing, monkey_patch_particles, StopWatch
import datetime as dt

test_cases = '/Users/michaesm/Downloads/EnduranceFirstInClass_testsSuite-EnduranceFirstInClass_testsSuite.csv'
database = '/Users/michaesm/Downloads/EnduranceFirstInClass_testsSuite-Endurance_Baseline_02_05_2016.c.csv'
url='http://opendap.oceanobservatories.org:8090/thredds/catalog/first-in-class/Coastal_Endurance/CE09OSSM/catalog.xml'
tds='http://opendap.oceanobservatories.org:8090/thredds/dodsC/'
dav_dir='/Volumes/dav' # Mounted directory of the dav raw data server
file_format = 'pd-pickle' # Format to save parsed data in
out_dir = './output' # Where should we save the parsed data to?
base_path = '/Users/michaesm/Documents/dev/repos/ooi-data-review/check_ooi_nc' #base path of this toolbox

if not os.path.exists(out_dir):
    os.makedirs(out_dir)

save_file = os.path.join(base_path, os.path.basename(os.path.splitext(test_cases)[0]) + '-scripted.csv')
C = Crawl(url, select=['.*CE09.*nc'])


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


def perform_test(test):
    if 'This instrument reference designator is available for download.' in test:
        return 1
    elif 'This stream is available for download.' in test:
        return 2
    elif 'This stream file has all parameters listed in the database.' in test:
        return 3
    elif  'This stream in the database has all parameters listed in the stream file.' in test:
        return 4
    elif  'One stream NetCDF file has no provenance errors.' in test:
        return 5
    elif 'One raw data file passes the parser/playback test.' in test:
        return 6
    elif 'This stream passes the algorithm input/output check.' in test:
        return 7
    elif 'This science parameter is available in file.' in test:
        return 8
    elif 'This science parameter is plotting with reasonable values.' in test:
        return 9


def get_file(refdes, stream, links):
    wildcard = '*' + refdes + '*' + stream + '*.nc'
    new_list = [s for s in links if fnmatch.fnmatch(s, wildcard)]

    if not new_list:
        link = ''
    else:
        link = new_list[0]
    return link


def check_ref_des(ref_des, links):
    wildcard = '*' + ref_des + '*'
    new_list = [s for s in links if fnmatch.fnmatch(s, wildcard)]
    if not new_list:
        return False
    else:
        return True


def compare_lists(list1, list2):
    match = []
    unmatch = []
    for i in list1:
        if i in list2:
            match.append(i)
        else:
            unmatch.append(i)
    return match, unmatch

links = [d.id for d in C.datasets]
del C
# appended_data = []

df = pd.read_csv(test_cases)
df2 = pd.read_csv(database)

ref_degs = list(pd.unique(df.ReferenceDesignator.ravel()))

for ref in ref_degs: # iterate through reference designators
    temp = df.loc[(df.ReferenceDesignator == ref)] # create temporary dataframe of this reference designator

    # Check if there is NOT any data with this reference designator
    if not check_ref_des(ref, links):
        print ref + ' does not exist in TDS. Failing test.'
        for row in temp.itertuples(): # Loop through every row in the csv containing this reference designator
            if perform_test(row.Test) is 1: # When it's test number 1, set the Status equal to fail
                df.loc[row.Index, 'Status'] = 'Fail'
            else: # for each subsequent row, set it to 'Blocked - Instrument Unavailable'
                df.loc[row.Index, 'Status'] = 'Blocked - Instrument Unavailable'
    else:
        print ref + ' exists. Proceeding.'
        streams = list(pd.unique(temp.StreamID.ravel())) # Get unique streams for an available reference designator
        for stream in streams: # Iterate through the streams
            if pd.isnull(stream): # If the stream is null
                temp_stream = temp.loc[pd.isnull(temp.StreamID)]
                for row in temp_stream.itertuples():
                    if perform_test(row.Test) is 1:
                        df.loc[row.Index, 'Status'] = 'Pass'
                    break
            else:
                temp_stream = temp.loc[(temp.StreamID == stream)]
                fname = get_file(temp_stream.iloc[0].ReferenceDesignator, temp_stream.iloc[0].StreamID, links)

                if not fname:
                    for row in temp_stream.itertuples():
                        if perform_test(row.Test) is 2:
                            df.loc[row.Index, 'Status'] = 'Fail'
                        else:
                            df.loc[row.Index, 'Status'] = 'Blocked - Stream Unavailable'
                else:
                    dataset_url = tds + fname
                    print dataset_url
                    # try:
                    with xr.open_dataset(dataset_url, engine='pydap', mask_and_scale=False) as ds:
                        # pdb.pm()
                        temp_df2 = df2.loc[(df2.ReferenceDesignator == ref) & (df2.StreamID == stream)]
                        ds_vars = ds.data_vars.keys()  # Stream file
                        # ds_vars.sort()
                        # ds_vars = [x.encode('UTF8') for x in ds_vars]
                        database_vars = pd.unique(temp_df2.ParameterID.ravel()).tolist()  # Database file
                        del temp_df2
                        # database_vars.sort()
                        for row in temp_stream.itertuples():
                            df.loc[row.Index, 'File'] = dataset_url
                            test_num = perform_test(row.Test)
                            if test_num is 2: # 'This stream is available for download.'
                                df.loc[row.Index, 'Status'] = 'Pass'
                                continue
                            elif test_num is 3: # 'This stream file has all parameters listed in the database.'
                                match, unmatch = compare_lists(ds_vars, database_vars)
                                if not unmatch:
                                    df.loc[row.Index, 'Status'] = "Pass"
                                else:
                                    df.loc[row.Index, 'Status'] = "Fail"
                                df.loc[row.Index, 'Comment'] = "Unmatch: " + str(unmatch)
                            elif test_num is 4: # 'This stream in the database has all parameters listed in the stream
                                match, unmatch = compare_lists(database_vars, ds_vars)
                                if not unmatch:
                                    df.loc[row.Index, 'Status'] = "Pass"
                                else:
                                    df.loc[row.Index, 'Status'] = "Fail"
                                df.loc[row.Index, 'Comment'] = "Unmatch: " + str(unmatch)
                                del ds_vars
                            elif test_num is 5: # 'One stream NetCDF file has no provenance errors.'
                                # continue
                                cp_dict = json.loads(ds['computed_provenance'].data[0])
                                errors = cp_dict['errors']

                                if not errors:
                                    df.loc[row.Index, 'Status'] = "Pass"
                                    df.loc[row.Index, 'Comment'] = "Errors: " + str(errors)
                                else:
                                    df.loc[row.Index, 'Status'] = "Fail"
                                    df.loc[row.Index, 'Comment'] = "Errors: " + str(errors)
                            elif test_num is 6: # 'One raw data file passes the parser/playback test.'
                                prov_split = str(ds['l0_provenance_information'].data[0]).split(' ')
                                driver = prov_split[1]
                                slog = driver.split('/')[-1] + '_parsed_' + dt.datetime.strftime(dt.datetime.utcnow(), '%Y-%m-%d_%H%M%S')
                                raw_file = [os.path.join(dav_dir, prov_split[0].split('/omc_data/whoi/OMC/')[1])]
                                log_name = out_dir + '/' + slog + '.log'
                                sys.stderr = open(log_name, 'w')
                                sys.stderr.write('Stream: ' + stream + '\n')
                                sys.stderr.write('Driver: ' + driver + '\n')
                                sys.stderr.write('Raw File: ' + str(raw_file) + '\n')
                                sys.stderr.write('Save Dir:' + out_dir + '\n')
                                run(base_path, driver, raw_file, file_format, out_dir)
                                # sys.stderr.close()

                                df.loc[row.Index, 'Status'] = ""
                                df.loc[row.Index, 'Comment'] = driver + ' -> ' + raw_file[0] + ' = ' + log_name
                            elif test_num is 7: # 'This stream passes the algorithm input/output check.'
                                df.loc[row.Index, 'Status'] = ""
                                df.loc[row.Index, 'Comment'] = "User needs to check manually"
                            elif test_num is 8: # 'This science parameter is available in file.'
                                if row.ParameterID in database_vars:
                                    df.loc[row.Index, 'Status'] = 'Pass'
                                    continue
                                else:
                                    df.loc[row.Index, 'Status'] = 'Fail'
                                    df.loc[row.Index, 'Comment'] = row.ParameterID + ' not in file.'
                                    continue
                            elif test_num is 9: # 'This science parameter is plotting with reasonable values.'
                                if row.ParameterID not in database_vars:
                                    df.loc[row.Index, 'Status'] = 'Blocked - Parameter Unavailable'
                                    continue
                                else:
                                    df.loc[row.Index, 'Status'] = ''
                                    df.loc[row.Index, 'Comment'] = "User needs to check"
                                # del database_vars
                            else:
                                break
                        del temp_stream
df.to_csv(save_file)