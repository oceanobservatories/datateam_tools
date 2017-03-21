#!/usr/bin/env python

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np



# specify path to annotation csvs
assets = '/Users/knuth/Documents/ooi/repos/github/annotations/RS03AXPS/RS03AXPS.csv'
stream = '/Users/knuth/Documents/ooi/repos/github/annotations/RS03AXPS/RS03AXPS-SF03A-2A-CTDPFA302/streamed-ctdpf_sbe43_sample.csv'
parameters = '/Users/knuth/Documents/ooi/repos/github/annotations/RS03AXPS/RS03AXPS-SF03A-2A-CTDPFA302/streamed-ctdpf_sbe43_sample-parameters.csv'




# specify deployement time ranges
# TODO auto source deployment time ranges
deployments_df = pd.DataFrame([['2014-09-27T18:33:00','2015-07-09T00:00:00'],['2015-07-09T04:16:00','2016-07-14T00:00:00'], ['2016-07-14T21:18:00','2017-03-21T00:00:00']])




# read in csv files
assets_df = pd.read_csv(assets, parse_dates=True)
stream_df = pd.read_csv(stream, parse_dates=True)
parameters_df = pd.read_csv(parameters, parse_dates=True)




# convert time stamps to date time
deployments_df[0] = deployments_df[0].apply(lambda x: pd.to_datetime(unicode(x)))
deployments_df[1] = deployments_df[1].apply(lambda x: pd.to_datetime(unicode(x)))


assets_df['StartTime'] = assets_df['StartTime'].apply(lambda x: pd.to_datetime(unicode(x)))
assets_df['EndTime'] = assets_df['EndTime'].apply(lambda x: pd.to_datetime(unicode(x)))

stream_df['StartTime'] = stream_df['StartTime'].apply(lambda x: pd.to_datetime(unicode(x)))
stream_df['EndTime'] = stream_df['EndTime'].apply(lambda x: pd.to_datetime(unicode(x)))

parameters_df['StartTime'] = parameters_df['StartTime'].apply(lambda x: pd.to_datetime(unicode(x)))
parameters_df['EndTime'] = parameters_df['EndTime'].apply(lambda x: pd.to_datetime(unicode(x)))




# create timeline labels and indices
yticks = ['Deployments']

for index, row in assets_df.iterrows():
	yticks.append(row["Level"])

for index, row in stream_df.iterrows():
	yticks.append(row["Level"])

for index, row in parameters_df.iterrows():
	yticks.append(row["Level"])

yticks = pd.unique(yticks)
yticks = yticks[::-1]
y = np.arange(len(yticks))
counter = -1





# plot deployment timelines
for index, row in deployments_df.iterrows():
	deploy_time = np.array([row[0],row[1]])
	deploy_shape = np.full((deploy_time.shape), y[counter])
	plt.plot(deploy_time, deploy_shape, linewidth=10, color='blue')

counter = counter -1

# plot subsite timelines
for index, row in assets_df.iterrows():
	subsite_time = np.array([row["StartTime"],row["EndTime"]])
	subsite_shape = np.full((subsite_time.shape), y[counter])
	if len(row["Level"]) == 8 and type(row["Status"]) == str:
		plt.plot(subsite_time, subsite_shape, linewidth=10, color='gray')

counter = counter -1

# plot node timelines
for index, row in assets_df.iterrows():
	node_time = np.array([row["StartTime"],row["EndTime"]])
	node_shape = np.full((node_time.shape), y[counter])
	if len(row["Level"]) == 14 and type(row["Status"]) == str:
		plt.plot(node_time, node_shape, linewidth=10, color='gray')

counter = counter -1

# plot instrument timelines
for index, row in assets_df.iterrows():
	instrument_time = np.array([row["StartTime"],row["EndTime"]])
	instrument_shape = np.full((instrument_time.shape), y[counter])
	if len(row["Level"]) == 27 and type(row["Status"]) == str:
		plt_title = row["Level"]
		plt.plot(instrument_time, instrument_shape, linewidth=10, color='gray')

counter = counter -1

# plot stream timelines
for index, row in stream_df.iterrows():
	stream_time = np.array([row["StartTime"],row["EndTime"]])
	stream_shape = np.full((stream_time.shape), y[counter])
	# TODO available timeline shows inaccurate overlap
	if row["Status"] == 'AVAILABLE':
		print row["StartTime"], row["EndTime"]
		plt.plot(stream_time, stream_shape, linewidth=10, color='green')
	elif row["Status"] == 'NOT_AVAILABLE':
		plt.plot(stream_time, stream_shape, linewidth=10, color='gray')

counter = counter -1



# plot parameter timelines
parameters = []
for index, row in parameters_df.iterrows():
	parameters.append(row["Level"])
parameters = np.unique(parameters)

for parameter in parameters:
	for index, row in parameters_df.iterrows():
		if row["Level"] == parameter:
			parameter_time = np.array([row["StartTime"],row["EndTime"]])
			parameter_shape = np.full((parameter_time.shape), y[counter])
			if row["Status"] == 'SUSPECT':
				plt.plot(parameter_time, parameter_shape, linewidth=10, color='orange')
			elif row["Status"] == 'FAIL':
				plt.plot(parameter_time, parameter_shape, linewidth=10, color='red')
			elif row["Status"] == 'PASS':
				plt.plot(parameter_time, parameter_shape, linewidth=10, color='green')
	counter = counter -1



plt.title(plt_title)
plt.yticks(y, yticks)
plt.xticks(rotation=20)
plt.tight_layout()
plt.ylim([-1,len(yticks)])
plt.grid()
plt.show()

