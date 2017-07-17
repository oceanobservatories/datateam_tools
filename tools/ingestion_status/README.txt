Raw Data Status:

Create the status file to complete the status column before creating the timelines.

(1) run infrastructure.py
(2) run ingest_append.py
(3) run status.py
(4) open “*_status_file.csv” and verify and complete the status and note columns 
	(4.1) investigate and resolve instrument(s) status. Use status_Key.csv for status definition.
	(4.2) Fill in the gaps of the start and end date columns:
	        (a) some instruments were left out of the deployment sheet when not deployed.
	        (b) fill in the end date of the last deployment.
	        Note: for timeline.py to work the date/time columns should have no blanks.
	(4.3) save file with your initial to preserve the original output.
(5) run timelines.py
	(5.1) Deployment Start and End dates are indicated by arrows on the x-axis
	(5.2) see Status_Key.csv for colors and labels combinations
	