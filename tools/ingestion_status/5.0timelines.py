#! /usr/local/bin/python

"""
Created on Fri Feb 23 2017

@author: leilabbb
"""
import os
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import matplotlib.pyplot as plt
from numpy import ndarray
from ast import literal_eval

# def convert_list_times(x):
#     if x == '[]':
#         continue
#     else:
#desired_width = 600
#pd.set_option('display.width', desired_width)

file_path = '/Users/leila/Documents/OOI_GitHub_repo/work/ingest-status/000_ingestpy_run_results'
file_name = 'CP01CNSM_17-07-2017_rawfiles_status_LB.csv'

cols = ['reference_designator', 'data_source', 'type_list',
          'ingest_csv_filename', 'platform', 'deployment#', 'uframe_route_y',
          'filename_mask', 'number_files', 'file of today', 'file <= 1k',
          'file > 1K', 'Automated_status', 'status', 'notes_x', 'CUID_Deploy',
          'deployedBy', 'CUID_Recover', 'recoveredBy', 'versionNumber',
          'startDateTime', 'stopDateTime', 'mooring.uid', 'node.uid',
          'sensor.uid', 'lat', 'lon', 'orbit', 'deployment_depth', 'water_depth', 'notes_y']

status_text  = ['Not Deployed','Not Available','Available','Not Expected','Pending', 'Missing','Expected']
textr_marker  = ['.','.','.','.','.', '.','.']#['x','o','.','o','+', '-','*']
status_color = ['black','magenta' , 'green', 'cyan', 'red', 'yellow', 'blue']#'#efefff' '#8f8f99' #5f5f66


df = pd.read_csv(os.path.join(file_path, file_name),parse_dates=True,usecols=cols)
df['startDateTime'] = df['startDateTime'].apply(lambda x: pd.to_datetime(x))
df['stopDateTime'] = df['stopDateTime'].apply(lambda x: pd.to_datetime(x))

reference_designators = list(pd.unique(df['reference_designator'].ravel()))
deployments = list(pd.unique(df['deployment#'].ravel()))


#fig = plt.figure()
#fig.patch.set_facecolor('black')

ref_sci = []
ref_num = 0
for rf in reference_designators:
    #print rf
    ind_r = df.loc[(df['reference_designator'] == rf)]
    inst_genre = list(pd.unique(ind_r['type_list'].ravel()))
    #print inst_genre[0]
    if inst_genre[0] == "Science":
        ref_sci.append(rf)
        ref_num += 1
print ref_num
print ref_sci

cnt = 0
ii_index = 1.5
y_list = []
for ref_des in ref_sci:
    print ref_des
    ind_rr = df.loc[(df['reference_designator'] == ref_des)]
    data_sources = list(pd.unique(ind_rr['data_source'].ravel()))
    print data_sources
    plt.grid()
    plt.ylim([0, (ref_num*3.5)]) #len(reference_designators)
    plt.tick_params(axis='y', which='both', labelleft='off', labelright='on')

    for data_source in data_sources:

        ind_tr     = ind_rr.loc[(ind_rr['data_source'] == data_source)]
        num_dep    = ind_tr['deployment#'].ravel()
        start_dep  = ind_tr['startDateTime'].ravel()
        end_dep    = ind_tr['stopDateTime'].ravel()
        data_stats = list(pd.unique(ind_tr['Automated_status'].ravel()))
        print data_stats

        deploy_index = np.full((2,end_dep.size), ii_index)
        deploy_index = np.array(deploy_index)
        deploy_time  = np.array([start_dep[0:end_dep.size],end_dep[0:end_dep.size]])
        str_dep = [str(i) for i in num_dep]
        x0_date = pd.to_datetime(deploy_time[0][0]) - timedelta(days=120)
        plt.text(x0_date, ii_index, data_source, fontsize=8)

        xi_date = pd.to_datetime(deploy_time[0][0])
        if cnt is 0:
            for ii in range(len(status_text)):
                plt.text(xi_date, 120, status_text[ii], color=status_color[ii], fontsize=9,
                         bbox={'facecolor':'#a3a3ff', 'alpha':0.2, 'pad':5})
                xi_date = xi_date + timedelta(days=140)

            d_index = np.full((2, end_dep.size), 0)
            d_index = np.array(d_index)
            plt.plot(deploy_time, d_index, c='k', marker='v')
            for jj in range(len(str_dep)):
                print str_dep[jj]
                plt.text(deploy_time[0][jj], 0.7, str_dep[jj], style='italic',fontsize=9)


        for data_stat in data_stats:

            color_ind  = status_text.index(data_stat)
            color_data = status_color[color_ind]
            color_marker = textr_marker[color_ind]

            ind_stat = ind_tr.loc[(ind_tr['Automated_status'] == data_stat)]

            st_dep = ind_stat['startDateTime'].ravel()
            en_dep = ind_stat['stopDateTime'].ravel()

            dep_index = np.full((2, en_dep.size), ii_index)
            dep_index = np.array(dep_index)
            dep_time  = np.array([st_dep[0:en_dep.size], en_dep[0:en_dep.size]])

            plt.plot(dep_time, dep_index, linewidth=1, color=color_data)#linewidth=1,marker=color_marker

        cnt += 1
        ii_index += 1

    ii_index += 1
    y_list.append(ii_index-3)

y = np.array(y_list)
print y, len(y) , len(ref_sci)

plt.yticks(y, ref_sci,fontsize=9)
plt.xticks(rotation=20)
# plt.patch.set_facecolor('black')
# plt.set_style("dark")
# plt.title(('RIC: {}, Variable: {}\n{} D0000{}').format(generation, row.variable, split[0], deployment))
fig_size = plt.rcParams['figure.figsize']
fig_size[0] = 12
fig_size[1] = 8.5
plt.rcParams['figure.figsize'] = fig_size

plt.show()
save_file = os.path.join(file_path, df['platform'][0] + '.eps')
plt.savefig(str(save_file), dpi=150)  # save figure

plt.close()