HERE TRAFFIC DOWNLOAD AND PROCESSING
------------------------------------

The bash script here_traffic.sh downloads every 5 minutes the whole traffic info for Sardinia and saves it
to a file named HERE_TRAFFIC_date-time.json.
The python script HERE_JSON_extractor.py downloads (if necessary) the OSM (driving) network of Sardinia and
saves it to disk. Each json file is read and processed to extract traffic info on each road. Each road is
characterized by a Position Code that we assign to a node of the OSM graph. These operations are cached to
speed up future processing. The final traffic info is encoded into a dictionary that is finally saved to disk. 

Be advised that each time the python script is run, the old traffic info is read from disk and then saved
with the updated snapshots. This file is about 1 GB for one month of recording.
