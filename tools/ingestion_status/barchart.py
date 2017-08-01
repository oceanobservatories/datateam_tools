#! /usr/local/bin/python

"""
Created on Mon July 31 2017
@author: leilabbb
"""


import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd


# select the platform
platform = 'CP02PMCI'
instrument = '-RII01-02-ADCPTG010'
# path to baseline file
maindir = '/Users/leila/Documents/OOI_GitHub_repo/work/ingest-status/000_ingestpy_run_results/'
rootdir = maindir + platform + '/statistics/instrument/'

# select the ingestion file example _D00003_ingest.csv or leave it as generic _ingest.csv
key_file = instrument + '_availability_state.csv'
plt.close('all')

jj = [[0,0],[0,1],[1,0],[1,1]]
for item in os.listdir(rootdir):
    if item.startswith(platform):
        if item.endswith(key_file):
            if os.path.isfile(os.path.join(rootdir, item)):
                print item
                with open(os.path.join(rootdir, item), 'r') as csv_file:
                    filereader = pd.read_csv(csv_file)
                    method_list = list(pd.unique(filereader['data_source'].ravel()))
                    num_method = len(method_list)
                    f, axarr = plt.subplots(2, 2, sharex='col', sharey='row')

                    for ii in range(num_method):
                        #print methodx
                        ind0 = filereader.loc[(filereader['data_source'] == method_list[ii])]
                        xt = ind0['deployment#'].values[:]
                        width = 0.45
                        ind = np.arange(len(ind0)) + 0.75
                        # plot row and column sharing

                        p0 = axarr[jj[ii][0],jj[ii][1]].bar(ind, ind0['percenta'].values[:], width, color = 'g')
                        p1 = axarr[jj[ii][0],jj[ii][1]].bar(ind, ind0['percentm'].values[:], width, bottom = ind0['percenta'].values[:], color='r')
                        axarr[jj[ii][0], jj[ii][1]].set_xticks(ind + width / 2.)
                        axarr[jj[ii][0], jj[ii][1]].set_xticklabels(xt)
                        axarr[jj[ii][0], jj[ii][1]].set_title(method_list[ii])


                    f.legend((p0[0], p1[0]), ('Available', 'Missing'), loc=1, borderaxespad=0.)
                    # f.tight_layout()

                    plt.show()

#fig.legend( (p0[0], p1[0], p2[0], p3[0]), ('kitchen', 'laundry', 'aircon&heater', 'other') )
#fig.tight_layout()
# fig.show()




# plt.show()