import pandas as pd
from thredds_crawler.crawl import Crawl
import os
import fnmatch
import xarray as xr

test_cases = '/Users/michaesm/Downloads/EnduranceFirstInClass_testsSuite-EnduranceFirstInClass_testsSuite.csv'
database = '/Users/michaesm/Downloads/EnduranceFirstInClass_testsSuite-Endurance_Baseline_02_05_2016.c.csv'
url='http://opendap-devel.ooi.rutgers.edu:8090/thredds/catalog/first-in-class/Coastal_Endurance/catalog.xml'
tds = 'http://opendap-devel.ooi.rutgers.edu:8090/thredds/dodsC/'

save_file = os.path.splitext(test_cases)[0] + '-scripted.csv'
C = Crawl(url, select=['.*CE09.*ncml'])


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
    wildcard = '*' + refdes + '*' + stream + '*.ncml'
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
# appended_data = []

df = pd.read_csv(test_cases)
df2 = pd.read_csv(database, low_memory=False)

ref_degs = pd.unique(df.ReferenceDesignator.ravel())
for ref in ref_degs: # iterate through reference designators
    temp = df.loc[(df.ReferenceDesignator == ref)] # create temporary dataframe of this reference designator
    # temp = df.loc[(df.ReferenceDesignator == 'CE09OSSM-RID27-03-CTDBPC000')]
    # temp = df.loc[(df.ReferenceDesignator == 'CE09OSSM-SBD12-05-WAVSSA000')]

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
        streams = pd.unique(temp.StreamID.ravel()) # Get unique streams for an available reference designator
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
                    with xr.open_dataset(dataset_url) as ds:
                        temp_df2 = df2.loc[(df2.ReferenceDesignator == ref) & (df2.StreamID == stream)]
                        ds_vars = ds.data_vars.keys()  # Stream file
                        ds_vars.sort()
                        ds_vars = [x.encode('UTF8') for x in ds_vars]
                        database_vars = pd.unique(temp_df2.ParameterID.ravel()).tolist()  # Database file
                        database_vars.sort()
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
                            elif test_num is 5: # 'One stream NetCDF file has no provenance errors.'
                                df.loc[row.Index, 'Status'] = ""
                                df.loc[row.Index, 'Comment'] = "User needs to check manually"
                            elif test_num is 6: # 'One raw data file passes the parser/playback test.'
                                df.loc[row.Index, 'Status'] = ""
                                df.loc[row.Index, 'Comment'] = "User needs to check manually"
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
                            else:
                                break
df.to_csv(save_file)