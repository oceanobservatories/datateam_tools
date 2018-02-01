#!/usr/bin/env python
"""
Created on Feb 1 2018

@author: lgarzio@marine.rutgers.edu
@brief: This script is used to convert spreadsheets provided by the MIOs to calibration sheets for glider CTDs and
DOSTAs that include sensor calibration dates. Cal sheets to be added to asset management:
https://github.com/ooi-integration/asset-management
"""

import pandas as pd
import os


def convert_osu_file(file, sheet_names):
    for s in sheet_names:
        name = str(s)
        print '\nConverting sheet name: %s' %name
        f = pd.read_excel(file, sheetname=name, skiprows=1)

        for i,j in f.iterrows():
            ctd_sn = int(j['SN'])
            ctd_uid = 'CGINS-CTDGVM-' + '{0:05d}'.format(ctd_sn)
            print ctd_uid
            ctd_inst = 'CTDGVM'
            ctd_caldate = str(j['Cal Date'])[0:10].replace('-','')
            ctd_sdir = os.path.join(dir,ctd_inst)
            create_dir(ctd_sdir)
            write_csv(ctd_uid, ctd_caldate, ctd_sn, ctd_sdir)

            do_sn = int(j['SN.3'])
            do_uid = 'CGINS-DOSTAM-' + '{0:05d}'.format(do_sn)
            print do_uid
            do_inst = 'DOSTAM'
            do_caldate = str(j['Cal Date.3'])[0:10].replace('-','')
            do_sdir = os.path.join(dir,do_inst)
            create_dir(do_sdir)
            write_csv(do_uid, do_caldate, do_sn, do_sdir)


def convert_whoi_file(file, sheet_names):
    for s in sheet_names:
        name = str(s)
        print '\nConverting sheet name: %s' %name
        c = {col: str for col in (1, 3)} # read in the date columns as strings instead of floats
        f = pd.read_excel(file, sheetname=name, converters=c)

        for i,j in f.iterrows():
            uid = j['Sensor_UID']
            print uid
            sn = int(uid.split('-')[-1])
            inst = uid.split('-')[1]
            caldate1 = j['Cal_Date_1']
            sdir = os.path.join(dir,inst)
            create_dir(sdir)
            write_csv(uid, caldate1, sn, sdir)

            caldate2 = (j['Cal_Date_2'])
            if pd.isnull(caldate2):
                continue
            else:
                write_csv(uid, caldate2, sn, sdir)


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


def get_sheetnames(file):
    xls = pd.ExcelFile(file)
    sheet_names = xls.sheet_names
    return sheet_names


def write_csv(uid, caldate, sn, sdir):
    cols = ['serial','name','value','notes']
    fname = uid + '__' + caldate + '.csv'
    df = pd.DataFrame([[sn,'CC_calibration_date',caldate,'sensor calibration date']], columns = cols)
    df.to_csv(os.path.join(sdir,fname),index=False)


def main(dir, input_files):
    for ifile in input_files:
        print '\nConverting file: %s' %ifile
        file = os.path.join(dir,ifile)
        sheet_names = get_sheetnames(file)

        if ifile == 'ScienceConfigs.xlsx':
            convert_osu_file(file, sheet_names)
        else:
            convert_whoi_file(file, sheet_names)


if __name__ == '__main__':
    dir = '/Users/lgarzio/Documents/OOI/glider_calsheets'
    input_files = ['ScienceConfigs.xlsx','Coastal Pioneer Glider Sensor Cals.xlsx','Global Glider Sensor Cals.xlsx']
    main(dir, input_files)