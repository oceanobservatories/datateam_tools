#!/usr/bin/env python
"""
This script is used to plot time gaps and qc flags from the check_data.py output. This script requires matplotlib
to be installed.
"""
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from ast import literal_eval

# def convert_list_times(x):
#     if x == '[]':
#         continue
#     else:
desired_width = 320
pd.set_option('display.width', desired_width)

file_path = '/Users/mikesmith/Documents/'
file_name = 'CE01ISSP-SP001-09-CTDPFJ000_RIC_2017-02-21T155000.csv'
cols = ['variable', 'deployment', 'start', 'stop', 'gaps', 'global_range', 'stuck_value', 'spike_test']

df = pd.read_csv(os.path.join(file_path, file_name),
                 parse_dates=True,
                 usecols=cols,
                 converters={'start': literal_eval,
                             'stop': literal_eval,
                             'gaps': literal_eval,
                             'global_range': literal_eval,
                             'stuck_value': literal_eval,
                             'spike_test': literal_eval
                             }
                 )
deployments = list(pd.unique(df['deployment'].ravel()))

df['start']= df['start'].apply(lambda x: pd.to_datetime(x))
df['stop'] = df['stop'].apply(lambda x: pd.to_datetime(x))
df['gaps'] = df['gaps'].apply(lambda list: [pd.to_datetime(x) for x in list])
df['global_range'] = df['global_range'].apply(lambda list: [pd.to_datetime(x) for x in list])
df['stuck_value'] = df['stuck_value'].apply(lambda list: [pd.to_datetime(x) for x in list])
df['spike_test'] = df['spike_test'].apply(lambda list: [pd.to_datetime(x) for x in list])
split = file_name.split('_')
generation = split[2].split('.')[0]

for deployment in deployments:
    tdf = df.loc[(df['deployment'] == deployment)]
    deploy_time = np.array(([tdf['start'][0][0], tdf['stop'][0][0]]))
    data_time = np.array(([tdf['start'][0][1], tdf['stop'][0][1]]))
    gaps = tdf['gaps'][0]
    # variables = list(pd.unique(df['variable'].ravel()))

    for row in tdf.itertuples():
        if not 'time' in row.variable:
            print row.variable
            plt.plot(deploy_time, np.full((deploy_time.shape), 5), linewidth=10, color='blue')
            plt.plot(data_time, np.full((data_time.shape), 4), linewidth=10, color='green')
            plt.plot(data_time, np.full((data_time.shape), 3), linewidth=10, color='green')
            plt.plot(data_time, np.full((data_time.shape), 2), linewidth=10, color='green')
            plt.plot(data_time, np.full((data_time.shape), 1), linewidth=10, color='green')
            plt.grid()
            plt.ylim([0,6])

            if gaps:
                [plt.plot(i, np.full((i.shape), 4), linewidth=10, color='red', markeredgecolor='black') for i in gaps]


            if row.global_range:
                [plt.plot(i, np.full((i.shape), 3), linewidth=10, color='red', markeredgecolor='black') for i in row.global_range]

            if row.stuck_value:
                [plt.plot(i, np.full((i.shape), 2), linewidth=10, color='red', markeredgecolor='black') for i in row.stuck_value]

            if row.spike_test:
                [plt.plot(i, np.full((i.shape), 1), linewidth=10, color='orange', markeredgecolor='black') for i in row.spike_test]

            y = np.array([1, 2, 3, 4, 5])
            yticks = ['Spike', 'Stuck', 'Global', 'Gaps', 'Deploy']
            plt.yticks(y, yticks)
            plt.xticks(rotation=20)
            plt.title(('RIC: {}, Variable: {}\n{} D0000{}').format(generation, row.variable, split[0], deployment))
            fig_size = plt.rcParams['figure.figsize']
            fig_size[0] = 12
            fig_size[1] = 8.5
            plt.rcParams['figure.figsize'] = fig_size
            plt.savefig(os.path.join(file_path ,'{}-D0000{}-{}.png'.format(split[0], deployment, row.variable)))
            plt.close()