Raw Data Status:

Create the status file to complete the Automated_status column before creating the availability level status files.

(1) run infrastructure.py [read the database and extract the baseline for a platform]
(2) run file_count_server.py [loop through the ingest.csv file mask column and hit the raw data server to return a count of available files]
(3) run status.py [merge deployment and ingest files to create a status file]
(4) open “*_status_file.csv” verify the status and note columns and complete the Automated_status column
	(4.1) investigate and resolve instrument(s) status. Use status_Key.csv for status definition.
	(4.2) Fill in the gaps of the start and end date columns:
	        (a) some instruments were left out of the deployment sheet when not deployed.
	        (b) fill in the end date of the last deployment.
	        Note: for timeline.py to work the date/time columns should have no blanks.
	(4.3) save file with your initial to preserve the original output.
(5) run timelines.py
	(5.1) Deployment Start and End dates are indicated by arrows on the x-axis
	(5.2) see Status_Key.csv for colors and labels combinations
	