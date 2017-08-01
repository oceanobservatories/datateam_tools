Raw Data Status:

Create the status file to complete the Automated_status column before creating the availability level status files.

(1) run 0.0infrastructure.py [read the database and extract the baseline for a platform]
(2) run 1.0file_count_server.py [loop through the ingest.csv file mask column and hit the raw data server to return a count of available files]
(3) run 2.0status.py [merge deployment and ingest files to create a status file]
(4) open “*_status_file.csv”
     (4.0) save file with your initial to preserve the original output
	 (4.1) merge note_(x,y) and status columns under status
	 (4.2) filter the compare column on false to reconcile between file mask and ingest.csv filename. Change False to True for future reference.
	 (4.3) check uncommented lines to have 0 files on the server and vice versa
	 (4.4) filter on reference designator and data_source to check the number of rows per instrument == number of deployments
	       (Note: the count in telemetered is usually ahead by one because recovered ingest.csv files get created on the next deployment)
     (4.4) use status_Key.csv to complete the automated status column
        (a) filter on the number_files
        (b) verify status across method for an instrument (e.g. Not Deployed should be applied to all method for an instrument)
        (c) mark the recovered data of the cruise before last as Missing if the number of files == 0
	 (4.5) filer on deployment# and add not deployed and/or missing instruments to deployment columns:
	        (b) comment out the line using  the CUID_Deploy column if instrument not deployed or missing a UID
	        (b) add notes to indicate "Not Deployed" or "add sensor UID"
	        (a) when applicable fill in the rows with: start and stop date/time, mooring UID, lat, lon, deployment and water depth
(5) recreate the deployment and ingest csv files
(6) calculate a percent of data availability on the plaform and instrument levels
(7) run timelines.py
	(5.1) Deployment Start and End dates are indicated by arrows on the x-axis
	(5.2) see Status_Key.csv for colors and labels combinations
	Note: for timeline.py to work the date/time columns should have no blanks. Fill in the end date of the active deployment.
	